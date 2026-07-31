"""ClickHouse ↔ PG metadata-bridge infrastructure tests.

Covers the ``_ch_meta_engine`` DDL (credential/vantage resolution), the
best-effort engine-table provisioning in ``Client.create()`` /  teardown in
``Client.delete()``, and the session-cached degrade of the ``concurrent``
read path.

DDL tests are pure (no DB). The live tests follow the suite convention:
skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL`` are not set.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta

import energydb as edb
import polars as pl
import pytest
from energydb import Client
from energydb._ch_meta_engine import (
    CH_ENGINE_TABLE,
    _engine_table_name,
    engine_table_ddl,
    series_meta_view_ddl,
)
from energydb._io import engine_meta_for_manifest, execute_read

DSN = "postgresql://app_user:s3cret@db.example.com:6543/proddb"


# ---------------------------------------------------------------------------
# engine_meta_for_manifest: superset predicate builder (no DB needed)
# ---------------------------------------------------------------------------


def test_engine_meta_edge_triple_manifest():
    """A (from_path, to_path, edge_type) manifest yields a set-valued edge_triples
    predicate with lowercased data_type — the fast-path parity with path routing."""
    manifest = pl.DataFrame(
        {
            "from_path": ["Grid/A", "Grid/A"],
            "to_path": ["Grid/B", "Grid/C"],
            "edge_type": ["Line", "Line"],
            "data_type": ["Actual", "actual"],
            "name": ["flow", "flow"],
        }
    )
    ms = engine_meta_for_manifest(manifest)
    assert ms is not None
    assert ms.table == CH_ENGINE_TABLE
    assert set(ms.edge_triples) == {("Grid/A", "Grid/B", "Line"), ("Grid/A", "Grid/C", "Line")}
    assert ms.edge_uuids is None and ms.paths is None
    assert set(ms.data_type) == {"actual"}  # lowercased + deduped


def test_engine_meta_edge_triple_falls_back_when_incomplete_or_null():
    """Missing a triple column, or a null in one, is inexpressible → None (the
    read then resolves sequentially and surfaces the proper error)."""
    partial = pl.DataFrame({"from_path": ["Grid/A"], "to_path": ["Grid/B"], "data_type": ["actual"], "name": ["flow"]})
    # partial triple is not a valid route at all → None
    assert engine_meta_for_manifest(partial) is None

    with_null = pl.DataFrame(
        {
            "from_path": ["Grid/A"],
            "to_path": [None],
            "edge_type": ["Line"],
            "data_type": ["actual"],
            "name": ["flow"],
        }
    )
    assert engine_meta_for_manifest(with_null) is None


def test_engine_meta_node_uuid_object_column_stays_engine_eligible():
    """A ``node_uuid`` column of ``uuid.UUID`` objects is polars dtype ``Object``.

    Regression for two bugs at once: it used to raise (``unique()`` is not
    supported on ``Object``), and the tempting fix — widening the Utf8 guard to
    every route — would return ``None`` and silently push every uuid-routed read
    onto the slow sequential path. Assert we get a real predicate.
    """
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    manifest = pl.DataFrame({"node_uuid": [u1, u2], "data_type": ["Actual", "actual"], "name": ["power", "power"]})
    assert manifest["node_uuid"].dtype == pl.Object  # the shape that used to crash

    meta = engine_meta_for_manifest(manifest)
    assert meta is not None
    assert meta.node_uuids == (str(u1), str(u2))
    # data_type dedups before lowercasing, so mixed case can repeat a value in the
    # IN list — harmless for a superset filter, and unchanged by this fix.
    assert set(meta.data_type) == {"actual"}
    assert meta.name == ("power",)


def test_engine_meta_edge_uuid_object_column_stays_engine_eligible():
    u1 = uuid.uuid4()
    manifest = pl.DataFrame([{"edge_uuid": u1, "data_type": "actual", "name": "flow"}])
    assert manifest["edge_uuid"].dtype == pl.Object

    meta = engine_meta_for_manifest(manifest)
    assert meta is not None
    assert meta.edge_uuids == (str(u1),)


def test_engine_meta_uuid_route_is_representation_independent():
    """Stringified and UUID-object manifests produce the identical predicate."""
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    cols = {"data_type": ["actual", "actual"], "name": ["power", "power"]}
    as_objects = engine_meta_for_manifest(pl.DataFrame({"node_uuid": [u1, u2], **cols}))
    as_strings = engine_meta_for_manifest(pl.DataFrame({"node_uuid": [str(u1), str(u2)], **cols}))
    assert as_objects == as_strings


def test_engine_meta_uuid_route_dedupes_mixed_representations():
    """The same uuid as an object and as its string is one owner, not two."""
    u1 = uuid.uuid4()
    manifest = pl.DataFrame({"node_uuid": [u1, str(u1)], "data_type": ["actual", "actual"], "name": ["power", "power"]})
    meta = engine_meta_for_manifest(manifest)
    assert meta is not None
    assert meta.node_uuids == (str(u1),)


def test_engine_meta_uuid_route_with_a_null_returns_none():
    """``null_count()`` is unreliable on ``Object``, so the None scan runs on the
    materialized list — a null owner is inexpressible either way."""
    manifest = pl.DataFrame({"node_uuid": [uuid.uuid4(), None], "data_type": ["actual"] * 2, "name": ["power"] * 2})
    assert engine_meta_for_manifest(manifest) is None


def test_engine_meta_path_route_non_utf8_returns_none():
    """The ``path`` route keeps its Utf8-only contract: a non-string path is a
    caller error that resolve_manifest reports properly."""
    manifest = pl.DataFrame([{"path": uuid.uuid4(), "data_type": "actual", "name": "power"}])
    assert engine_meta_for_manifest(manifest) is None


# ---------------------------------------------------------------------------
# The predicate is built lazily — only when the engine is usable (no DB needed)
# ---------------------------------------------------------------------------


class _FakeClient:
    """Just the one attribute ``execute_read`` consults."""

    def __init__(self, *, engine_unavailable: bool) -> None:
        self._engine_unavailable = engine_unavailable


async def _resolves_to_nothing() -> None:
    """An exact resolve that matched no series — short-circuits before any CH call."""
    return None


def test_engine_predicate_is_not_built_when_the_engine_is_unavailable():
    """``ENERGYDB_DISABLE_ENGINE=1`` (and a degraded session) must skip predicate
    construction entirely — it used to be evaluated eagerly at the call site, so
    a predicate that raised broke reads the kill-switch was supposed to protect."""
    calls = []

    def factory():
        calls.append(1)
        raise AssertionError("the engine predicate must not be built when the engine is off")

    _result, n_series = asyncio.run(
        execute_read(
            None,
            None,
            _FakeClient(engine_unavailable=True),
            resolve=_resolves_to_nothing,
            engine_meta=factory,
        )
    )
    assert calls == []
    assert n_series == 0


def test_engine_predicate_is_built_once_when_the_engine_is_available():
    """The converse: an available engine invokes the factory exactly once. A
    factory yielding ``None`` (inexpressible read) falls through to sequential."""
    calls = []

    def factory():
        calls.append(1)
        return None

    _result, n_series = asyncio.run(
        execute_read(
            None,
            None,
            _FakeClient(engine_unavailable=False),
            resolve=_resolves_to_nothing,
            engine_meta=factory,
        )
    )
    assert calls == [1]
    assert n_series == 0


# ---------------------------------------------------------------------------
# DDL: credential / vantage resolution (no DB needed)
# ---------------------------------------------------------------------------


def test_engine_ddl_inlines_dsn_creds(monkeypatch):
    monkeypatch.delenv("ENERGYDB_CH_PG_COLLECTION", raising=False)
    monkeypatch.delenv("ENERGYDB_CH_PG_HOST", raising=False)
    ddl = engine_table_ddl(DSN, "public")
    assert "'db.example.com:6543'" in ddl
    assert "'proddb'" in ddl
    assert "'app_user'" in ddl
    assert "'s3cret'" in ddl
    assert f"CREATE TABLE IF NOT EXISTS {CH_ENGINE_TABLE}" in ddl


def test_engine_ddl_ch_vantage_host_override(monkeypatch):
    """ENERGYDB_CH_PG_HOST replaces only the network path — ClickHouse dials PG
    from its own vantage (e.g. the compose network), not the app's."""
    monkeypatch.delenv("ENERGYDB_CH_PG_COLLECTION", raising=False)
    monkeypatch.setenv("ENERGYDB_CH_PG_HOST", "postgres:5432")
    ddl = engine_table_ddl(DSN, "public")
    assert "'postgres:5432'" in ddl
    assert "db.example.com" not in ddl
    # identity still comes from the DSN
    assert "'proddb'" in ddl and "'app_user'" in ddl and "'s3cret'" in ddl


def test_engine_ddl_named_collection_keeps_password_out(monkeypatch):
    monkeypatch.setenv("ENERGYDB_CH_PG_COLLECTION", "energydb_pg")
    monkeypatch.setenv("ENERGYDB_CH_PG_HOST", "postgres:5432")  # must be irrelevant here
    ddl = engine_table_ddl(DSN, "energydb")
    # schema is passed explicitly so the engine table targets the active schema,
    # not whatever the named collection encodes.
    assert "ENGINE = PostgreSQL(energydb_pg, table = 'series_meta', schema = 'energydb')" in ddl
    assert "s3cret" not in ddl and "postgres:5432" not in ddl


def test_engine_ddl_inlined_carries_schema(monkeypatch):
    monkeypatch.delenv("ENERGYDB_CH_PG_COLLECTION", raising=False)
    monkeypatch.delenv("ENERGYDB_CH_PG_HOST", raising=False)
    # schema is the final positional arg of the inlined PostgreSQL() source.
    assert engine_table_ddl(DSN, "energydb").rstrip().endswith("'energydb')")


def test_engine_table_name_scoped_by_schema():
    # public keeps the bare (historical) name; a named schema gets a suffix, so a
    # table name can only ever map to one schema (no cross-schema mis-target).
    assert _engine_table_name("energydb_series_meta_pg", "public") == "energydb_series_meta_pg"
    assert _engine_table_name("energydb_series_meta_pg", "energydb") == "energydb_series_meta_pg__energydb"


def test_series_meta_view_ddl_qualifier():
    create, drop = series_meta_view_ddl("energydb.")
    assert "CREATE OR REPLACE VIEW energydb.series_meta" in create
    assert "energydb.node" in create and "energydb.series" in create
    assert drop == "DROP VIEW IF EXISTS energydb.series_meta"


# ---------------------------------------------------------------------------
# Live: provisioning lifecycle + the session degrade
# ---------------------------------------------------------------------------

pytestmark_live = pytest.mark.skipif(
    not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")),
    reason="TIMEDB_PG_DSN / TIMEDB_CH_URL not set",
)


@pytest.fixture
def client():
    c = Client()
    c.delete()
    c.create()
    tree = edb.Portfolio(name="P", members=[edb.wind.WindTurbine(name="T1", capacity=3.0)])
    c.register_tree(tree)
    c.get_node("P", "T1").register_series(
        name="power", canonical_unit="MW", data_type="actual", timeseries_type="FLAT", retention="forever"
    )
    base = datetime(2026, 1, 1, tzinfo=UTC)
    c.write(
        pl.DataFrame(
            {
                "path": ["P/T1", "P/T1"],
                "data_type": ["actual"] * 2,
                "name": ["power"] * 2,
                "valid_time": [base, base + timedelta(hours=1)],
                "value": [1.0, 2.0],
            }
        )
    )
    yield c
    c.delete()
    c.close()


def _engine_table_exists(c: Client) -> bool:
    return bool(c.td._ch.command(f"EXISTS TABLE {CH_ENGINE_TABLE}"))


@pytestmark_live
def test_create_provisions_engine_table_and_delete_drops_it(client):
    """create() auto-provisions the CH engine table (best-effort); delete() tears it down."""
    assert _engine_table_exists(client)
    client.delete()
    assert not _engine_table_exists(client)
    client.create()  # leave the fixture invariant intact for teardown
    assert _engine_table_exists(client)


@pytestmark_live
def test_disable_engine_env_kill_switch(monkeypatch):
    """ENERGYDB_DISABLE_ENGINE=1 seeds the session flag: every read runs sequentially."""
    monkeypatch.setenv("ENERGYDB_DISABLE_ENGINE", "1")
    c = Client()
    try:
        assert c._async._engine_unavailable is True
    finally:
        c.close()


@pytestmark_live
def test_engine_failure_degrades_once_per_session(client, monkeypatch):
    """The first engine-read failure flips the session flag: the read still returns the
    correct (sequential) result, and later reads skip the engine entirely.
    setup_ch_meta_engine() re-enables."""
    import energydb._io as _io

    calls = {"n": 0}
    orig = _io._td_call

    def fake_td_call(td, *, relative, kwargs, meta_source=None):
        if meta_source is None:
            return orig(td, relative=relative, kwargs=kwargs)

        def boom(*args, **kw):
            calls["n"] += 1
            raise RuntimeError("engine down")

        return boom

    monkeypatch.setattr(_io, "_td_call", fake_td_call)
    monkeypatch.setattr(_io, "_ENGINE_STRICT", False)

    client._async._engine_unavailable = True  # sequential baseline
    expected = client.get_node("P").read(data_type="actual", name="power")
    client._async._engine_unavailable = False

    out1 = client.get_node("P").read(data_type="actual", name="power")
    assert out1.equals(expected)  # fell back to the sequential path, correct result
    assert calls["n"] == 1
    assert client._async._engine_unavailable is True

    out2 = client.get_node("P").read(data_type="actual", name="power")
    assert out2.equals(expected)
    assert calls["n"] == 1  # engine not re-tried: the session flag short-circuits

    client.setup_ch_meta_engine()  # explicit re-enable resets the flag
    assert client._async._engine_unavailable is False
    out3 = client.get_node("P").read(data_type="actual", name="power")
    assert out3.equals(expected)
    assert calls["n"] == 2  # the engine was attempted again after the reset


# ---------------------------------------------------------------------------
# Live: uuid-routed manifests carrying uuid.UUID objects
# ---------------------------------------------------------------------------


def _uuid_manifest(node_uuid, *, stringify: bool) -> pl.DataFrame:
    owner = str(node_uuid) if stringify else node_uuid
    return pl.DataFrame([{"node_uuid": owner, "data_type": "actual", "name": "power"}])


@pytestmark_live
def test_uuid_object_routed_read_takes_the_engine_path(client, monkeypatch):
    """End-to-end: a manifest holding a ``uuid.UUID`` reads correctly *and* over the
    engine. ``get_raw()["uuid"]`` returns a UUID object, so feeding energydb's own
    output straight back in is the natural thing to do — and is what used to crash."""
    import energydb._io as _io

    node_uuid = client.get_node("P", "T1").get_raw()["uuid"]
    assert isinstance(node_uuid, uuid.UUID)

    used_engine = []
    orig = _io._td_call

    def spy_td_call(td, *, relative, kwargs, meta_source=None):
        used_engine.append(meta_source is not None)
        return orig(td, relative=relative, kwargs=kwargs, meta_source=meta_source)

    monkeypatch.setattr(_io, "_td_call", spy_td_call)
    monkeypatch.setattr(_io, "_ENGINE_STRICT", True)  # an engine failure raises instead of degrading

    try:
        out_obj = client.read(_uuid_manifest(node_uuid, stringify=False))
    except Exception as exc:  # noqa: BLE001 -- re-raised below with a diagnosis
        if "POSTGRESQL_CONNECTION_FAILURE" in str(exc):
            pytest.fail(
                "ClickHouse could not reach PostgreSQL through the meta-engine table, so this "
                "test's strict engine read failed. This is an environment problem, not a code "
                "one: the engine table inlines PG's address from *ClickHouse's* network vantage, "
                "which is not the DSN the app uses. Set ENERGYDB_CH_PG_HOST to PG's host:port as "
                "ClickHouse sees it (e.g. 'postgres:5432' on a compose/CI network) and "
                f"re-provision with setup_ch_meta_engine(). Underlying error: {exc}"
            )
        raise
    assert any(used_engine), "the uuid-routed read did not take the engine path"
    assert out_obj.height == 2
    assert client._async._engine_unavailable is False  # no degrade happened

    out_str = client.read(_uuid_manifest(node_uuid, stringify=True))
    assert out_obj.equals(out_str)  # representation-independent, byte for byte


@pytestmark_live
def test_uuid_object_routed_read_works_with_the_engine_disabled(client, monkeypatch):
    """Regression for the eager evaluation: with ``ENERGYDB_DISABLE_ENGINE=1`` the
    predicate must never be constructed, so even a predicate that raises cannot
    break the read. This is the exact configuration that broke psd."""
    import energydb.client as _client_mod

    node_uuid = client.get_node("P", "T1").get_raw()["uuid"]
    expected = client.read(_uuid_manifest(node_uuid, stringify=True))

    monkeypatch.setenv("ENERGYDB_DISABLE_ENGINE", "1")

    def must_not_be_called(_manifest):
        raise AssertionError("engine_meta_for_manifest was invoked with the engine disabled")

    monkeypatch.setattr(_client_mod, "engine_meta_for_manifest", must_not_be_called)

    disabled = Client()
    try:
        assert disabled._async._engine_unavailable is True
        out = disabled.read(_uuid_manifest(node_uuid, stringify=False))
    finally:
        disabled.close()
    assert out.equals(expected)
