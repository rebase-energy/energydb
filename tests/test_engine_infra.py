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
import contextlib
import logging
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
    engine_pg_host,
    engine_table_ddl,
    inlines_pg_password,
    series_meta_view_ddl,
)
from energydb._io import _is_unknown_table, engine_meta_for_manifest, execute_read
from energydb.client import AsyncClient
from energydb.errors import ConfigurationError

DSN = "postgresql://app_user:s3cret@db.example.com:6543/proddb"


# ---------------------------------------------------------------------------
# engine_meta_for_manifest: superset predicate builder (no DB needed)
# ---------------------------------------------------------------------------


def test_engine_meta_edge_triple_manifest():
    """A (from_path, to_path, edge_type) manifest yields a set-valued edge_triples
    predicate with lowercased data_type, the fast-path parity with path routing."""
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

    Regression for two bugs at once: raising (``unique()`` is not supported on
    ``Object``), and the tempting fix of widening the Utf8 guard to every route,
    which would return ``None`` and silently push every uuid-routed read onto
    the slow sequential path. Assert we get a real predicate.
    """
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    manifest = pl.DataFrame({"node_uuid": [u1, u2], "data_type": ["Actual", "actual"], "name": ["power", "power"]})
    assert manifest["node_uuid"].dtype == pl.Object  # the shape that used to crash

    meta = engine_meta_for_manifest(manifest)
    assert meta is not None
    assert meta.node_uuids == (str(u1), str(u2))
    # data_type dedups before lowercasing, so mixed case can repeat a value in the
    # IN list: harmless for a superset filter, and unchanged by this fix.
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
    materialized list, a null owner is inexpressible either way."""
    manifest = pl.DataFrame({"node_uuid": [uuid.uuid4(), None], "data_type": ["actual"] * 2, "name": ["power"] * 2})
    assert engine_meta_for_manifest(manifest) is None


def test_engine_meta_path_route_non_utf8_returns_none():
    """The ``path`` route keeps its Utf8-only contract: a non-string path is a
    caller error that resolve_manifest reports properly."""
    manifest = pl.DataFrame([{"path": uuid.uuid4(), "data_type": "actual", "name": "power"}])
    assert engine_meta_for_manifest(manifest) is None


# ---------------------------------------------------------------------------
# The predicate is built lazily: only when the engine is usable (no DB needed)
# ---------------------------------------------------------------------------


class _FakeClient:
    """Just the one attribute ``execute_read`` consults."""

    def __init__(self, *, engine_unavailable: bool) -> None:
        self._engine_unavailable = engine_unavailable


async def _resolves_to_nothing() -> None:
    """An exact resolve that matched no series, short-circuits before any CH call."""
    return None


def test_engine_predicate_is_not_built_when_the_engine_is_unavailable():
    """``ENERGYDB_DISABLE_ENGINE=1`` (and a degraded session) must skip predicate
    construction entirely. Evaluated eagerly at the call site, a predicate that
    raised would break the very reads the kill-switch is meant to protect."""
    calls = []

    def factory():
        calls.append(1)
        raise AssertionError("the engine predicate must not be built when the engine is off")

    _result, n_series, missing = asyncio.run(
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
    # Scope-style resolve= reads have no manifest, so nothing to report.
    assert missing.is_empty()


def test_engine_predicate_is_built_once_when_the_engine_is_available():
    """The converse: an available engine invokes the factory exactly once. A
    factory yielding ``None`` (inexpressible read) falls through to sequential."""
    calls = []

    def factory():
        calls.append(1)
        return None

    _result, n_series, missing = asyncio.run(
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
    assert missing.is_empty()


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
    """ENERGYDB_CH_PG_HOST replaces only the network path, ClickHouse dials PG
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
# engine_pg_host: TCP-vs-socket DSN resolution (no DB needed)
# ---------------------------------------------------------------------------

_SOCKET_DSN = "postgresql:///homelab?host=/run/postgresql&user=homelab"
_QUERY_TCP_HOST_DSN = "postgresql:///db?host=tcphost"


def test_socket_dsn_has_no_engine_pg_host(monkeypatch):
    monkeypatch.delenv("ENERGYDB_CH_PG_HOST", raising=False)
    assert engine_pg_host(_SOCKET_DSN) is None


def test_socket_dsn_ddl_construction_refuses(monkeypatch):
    monkeypatch.delenv("ENERGYDB_CH_PG_COLLECTION", raising=False)
    monkeypatch.delenv("ENERGYDB_CH_PG_HOST", raising=False)
    with pytest.raises(ConfigurationError, match="TCP host"):
        engine_table_ddl(_SOCKET_DSN, "public")


def test_query_string_tcp_host_resolves(monkeypatch):
    """``postgresql:///db?host=tcphost`` puts the TCP host in the query string,
    not the netloc; conninfo_to_dict must still pick it up."""
    monkeypatch.delenv("ENERGYDB_CH_PG_HOST", raising=False)
    assert engine_pg_host(_QUERY_TCP_HOST_DSN) == "tcphost:5432"


def test_query_string_tcp_host_ddl(monkeypatch):
    monkeypatch.delenv("ENERGYDB_CH_PG_COLLECTION", raising=False)
    monkeypatch.delenv("ENERGYDB_CH_PG_HOST", raising=False)
    ddl = engine_table_ddl(_QUERY_TCP_HOST_DSN, "public")
    assert "'tcphost:5432'" in ddl


def test_ch_pg_host_override_wins_for_socket_dsn(monkeypatch):
    """The override is an escape hatch for exactly this case: a socket-only DSN
    with a TCP host ClickHouse can still reach."""
    monkeypatch.setenv("ENERGYDB_CH_PG_HOST", "postgres:5432")
    assert engine_pg_host(_SOCKET_DSN) == "postgres:5432"
    ddl = engine_table_ddl(_SOCKET_DSN, "public")
    assert "'postgres:5432'" in ddl


def test_ch_pg_host_override_wins_for_tcp_dsn(monkeypatch):
    monkeypatch.setenv("ENERGYDB_CH_PG_HOST", "postgres:5432")
    assert engine_pg_host(DSN) == "postgres:5432"


# ---------------------------------------------------------------------------
# setup_ch_meta_engine / provisioning skip for a socket-only DSN (no DB needed)
# ---------------------------------------------------------------------------


def _bare_client(dsn: str) -> AsyncClient:
    """An ``AsyncClient`` with only ``_dsn`` set, no pool or CH client.

    The no-TCP-host branch of ``_provision_engine_table_blocking`` returns
    before touching ``self.td`` or ``self._pool``, so this is enough to test it
    without a live PostgreSQL or ClickHouse.
    """
    obj = object.__new__(AsyncClient)
    obj._dsn = dsn
    return obj


def test_provisioning_skips_for_socket_dsn(monkeypatch, caplog):
    monkeypatch.delenv("ENERGYDB_CH_PG_COLLECTION", raising=False)
    monkeypatch.delenv("ENERGYDB_CH_PG_HOST", raising=False)
    client = _bare_client(_SOCKET_DSN)

    with caplog.at_level(logging.INFO, logger="energydb.client"):
        client._provision_engine_table_blocking()

    records = [r for r in caplog.records if r.name == "energydb.client"]
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    assert "TCP host" in records[0].getMessage()


def test_setup_ch_meta_engine_raises_for_socket_dsn(monkeypatch):
    """The explicit, raising path: ``setup_ch_meta_engine`` must not silently
    degrade the way ``create()``'s best-effort provisioning does."""
    monkeypatch.delenv("ENERGYDB_CH_PG_COLLECTION", raising=False)
    monkeypatch.delenv("ENERGYDB_CH_PG_HOST", raising=False)
    client = _bare_client(_SOCKET_DSN)

    with pytest.raises(ConfigurationError, match="TCP host"):
        client._provision_engine_table_blocking(strict=True)


def test_ch_pg_host_override_lets_provisioning_proceed_for_socket_dsn(monkeypatch, caplog):
    """The override unblocks provisioning entirely, not just ``engine_pg_host``:
    with it set, the socket DSN no longer skips or raises past the host check.

    The bare client has no ``self.td``, so past the host check it hits
    ``AttributeError`` reaching for it, that failure is the proof it got past
    the no-TCP-host branch instead of skipping or raising there.
    """
    monkeypatch.delenv("ENERGYDB_CH_PG_COLLECTION", raising=False)
    monkeypatch.setenv("ENERGYDB_CH_PG_HOST", "postgres:5432")
    client = _bare_client(_SOCKET_DSN)

    with caplog.at_level(logging.INFO, logger="energydb.client"), pytest.raises(AttributeError):
        client._provision_engine_table_blocking(strict=True)

    records = [r for r in caplog.records if r.name == "energydb.client"]
    assert not records  # did not take the no-TCP-host skip path


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
    output straight back in is the natural thing to do."""
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
    except Exception as exc:  # noqa: BLE001  (re-raised below with a diagnosis)
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


# ---------------------------------------------------------------------------
# Engine-probe log level: the not-provisioned case is a state, not an anomaly
# ---------------------------------------------------------------------------


class _FakeDatabaseError(Exception):
    """Stands in for clickhouse-connect's DatabaseError, which carries the
    structured ClickHouse error code as a ``.code`` attribute alongside the
    server-text message."""

    def __init__(self, message: str = "", code: int | None = None) -> None:
        super().__init__(message)
        self.code = code


_UNKNOWN_TABLE_TEXT = (
    "Code: 60. DB::Exception: Table default.energydb_series_meta_pg doesn't exist. (UNKNOWN_TABLE) (version 26.1.1.1)"
)


def test_the_error_code_alone_is_enough():
    """clickhouse-connect exposes the ClickHouse error code as a structured
    ``.code`` attribute; the classifier must recognise it even without any
    matching text markers, since the driver's message format is not a stable
    contract."""
    assert _is_unknown_table(_FakeDatabaseError("some generic failure", code=60)) is True


def test_message_text_alone_is_not_enough():
    """The server text is no longer trusted: a message containing
    ``UNKNOWN_TABLE`` / ``Code: 60.`` with no matching ``.code`` must not
    classify, so a similarly worded error can't be misread."""
    assert _is_unknown_table(_FakeDatabaseError(_UNKNOWN_TABLE_TEXT)) is False


def test_scrubbed_message_still_recognised():
    """With ``show_clickhouse_errors=False`` the server sends a generic message
    and the text markers vanish, but ``.code`` survives."""
    assert _is_unknown_table(_FakeDatabaseError("A database error occurred", code=60)) is True


def test_a_real_engine_failure_is_not_classified_as_not_provisioned():
    """The whole point of the split: network/auth/connectivity failures must keep
    their warning + traceback. A quieter log for those would hide real breakage."""
    refused = _FakeDatabaseError(
        "Code: 1002. DB::Exception: Connection refused (POSTGRESQL_CONNECTION_FAILURE) (version 26.1.1.1)",
        code=1002,
    )
    assert _is_unknown_table(refused) is False
    assert _is_unknown_table(None) is False
    assert _is_unknown_table(RuntimeError("engine down")) is False


def test_the_marker_is_found_down_the_exception_chain():
    """The driver may re-wrap the server error before it reaches us, so the
    classifier walks ``__cause__``/``__context__`` rather than only the top."""
    inner = _FakeDatabaseError("Table doesn't exist", code=60)
    wrapped = RuntimeError("clickhouse query failed")
    wrapped.__cause__ = inner
    assert _is_unknown_table(wrapped) is True

    outer = RuntimeError("read failed")
    outer.__context__ = wrapped
    assert _is_unknown_table(outer) is True


def test_a_self_referential_chain_terminates():
    """Defensive: the classifier runs inside an exception handler on the read
    path, so it must not be able to spin on a cyclic chain."""
    a, b = RuntimeError("a"), RuntimeError("b")
    a.__cause__ = b
    b.__cause__ = a
    assert _is_unknown_table(a) is False


@pytestmark_live
def test_a_real_scrubbed_driver_error_is_recognised():
    """Against a real server, ``show_clickhouse_errors=False`` scrubs the text
    markers from the exception but the structured ``.code`` survives, which is
    the exact scenario this item exists for."""
    import clickhouse_connect

    ch = clickhouse_connect.get_client(dsn=os.environ["TIMEDB_CH_URL"], show_clickhouse_errors=False)
    missing_table = f"energydb_test_missing_{uuid.uuid4().hex}"
    with pytest.raises(Exception) as excinfo:
        ch.query(f"SELECT * FROM {missing_table}")
    assert _is_unknown_table(excinfo.value) is True


def _fail_engine_with(monkeypatch, exc: BaseException):
    """Make only the engine-backed CH read fail, with ``exc`` as the cause.

    The sequential path is left intact, so the read under test still returns the
    correct result, which is what lets these tests assert the log *and* the
    fallback in one go.
    """
    import energydb._io as _io

    orig = _io._td_call

    def fake_td_call(td, *, relative, kwargs, meta_source=None):
        if meta_source is None:
            return orig(td, relative=relative, kwargs=kwargs)

        def boom(*args, **kw):
            raise exc

        return boom

    monkeypatch.setattr(_io, "_td_call", fake_td_call)
    monkeypatch.setattr(_io, "_ENGINE_STRICT", False)


@pytestmark_live
def test_not_provisioned_logs_one_info_line_without_a_traceback(client, monkeypatch, caplog):
    """**The regression this item exists for.** A deployment that never provisions
    the engine table used to get a scary traceback on its first read of every
    process, and learned to silence it with ENERGYDB_DISABLE_ENGINE=1, which then
    hid real engine failures too."""
    _fail_engine_with(monkeypatch, _FakeDatabaseError(_UNKNOWN_TABLE_TEXT, code=60))
    expected = client.get_node("P").read(data_type="actual", name="power")
    client._async._engine_unavailable = False

    with caplog.at_level(logging.INFO, logger="energydb._io"):
        out = client.get_node("P").read(data_type="actual", name="power")

    records = [r for r in caplog.records if r.name == "energydb._io"]
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    assert records[0].exc_info is None  # no traceback
    assert "not provisioned" in records[0].getMessage()
    assert "setup_ch_meta_engine" in records[0].getMessage()
    # Behaviour is unchanged: still degraded, still the correct sequential result.
    assert client._async._engine_unavailable is True
    assert out.equals(expected)


@pytestmark_live
def test_a_real_failure_still_warns_with_the_cause_inline(client, monkeypatch, caplog):
    """The other half of the split, asserted so the quieting can't creep.

    The WARNING line carries ``str(err.__cause__)`` inline and no traceback; the
    traceback moves to a DEBUG record on the same logger.
    """
    _fail_engine_with(monkeypatch, _FakeDatabaseError("Code: 1002. Connection refused", code=1002))
    client._async._engine_unavailable = False

    with caplog.at_level(logging.DEBUG, logger="energydb._io"):
        client.get_node("P").read(data_type="actual", name="power")

    records = [r for r in caplog.records if r.name == "energydb._io"]
    warnings = [r for r in records if r.levelno == logging.WARNING]
    debugs = [r for r in records if r.levelno == logging.DEBUG]
    assert len(warnings) == 1
    assert "Connection refused" in warnings[0].getMessage()
    assert warnings[0].exc_info is None  # traceback moved off the WARNING record
    assert len(debugs) == 1
    assert debugs[0].exc_info is not None  # traceback retained, just quieter


@pytestmark_live
def test_the_info_line_fires_once_per_session(client, monkeypatch, caplog):
    """The session latches ``_engine_unavailable``, so later reads never re-probe.
    Asserted here because the whole justification for info-level is that it is a
    one-off statement of configuration, not a per-read complaint."""
    _fail_engine_with(monkeypatch, _FakeDatabaseError(_UNKNOWN_TABLE_TEXT, code=60))
    client._async._engine_unavailable = False

    with caplog.at_level(logging.INFO, logger="energydb._io"):
        client.get_node("P").read(data_type="actual", name="power")
        client.get_node("P").read(data_type="actual", name="power")
        client.get_node("P").read(data_type="actual", name="power")

    assert len([r for r in caplog.records if r.name == "energydb._io"]) == 1


@pytestmark_live
def test_strict_mode_still_raises_for_the_not_provisioned_case(client, monkeypatch):
    """The classifier must not soften strict mode: ENERGYDB_ENGINE_STRICT=1 exists
    so a broken engine is loud, and "not provisioned" is still broken to someone
    who explicitly asked for the engine."""
    import energydb._io as _io

    _fail_engine_with(monkeypatch, _FakeDatabaseError(_UNKNOWN_TABLE_TEXT, code=60))
    monkeypatch.setattr(_io, "_ENGINE_STRICT", True)
    client._async._engine_unavailable = False

    with pytest.raises(_FakeDatabaseError, match="UNKNOWN_TABLE"):
        client.get_node("P").read(data_type="actual", name="power")


@pytestmark_live
def test_a_genuinely_unprovisioned_table_takes_the_info_path(client, caplog):
    """End-to-end without faking the failure: drop the engine table and read. This
    is the exact configuration the item is about, so it is worth one test that
    does not stub the driver."""
    client.td._ch.command(f"DROP TABLE IF EXISTS {CH_ENGINE_TABLE}")
    client._async._engine_unavailable = False

    with caplog.at_level(logging.INFO, logger="energydb._io"):
        out = client.get_node("P").read(data_type="actual", name="power")

    records = [r for r in caplog.records if r.name == "energydb._io"]
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    assert records[0].exc_info is None
    assert out.height == 2  # the sequential fallback returned the data

    client.setup_ch_meta_engine()  # restore the fixture invariant


# ---------------------------------------------------------------------------
# Inline-credential warning at provisioning time
# ---------------------------------------------------------------------------

_PASSWORDLESS_DSN = "postgresql://app_user@db.example.com:6543/proddb"


def test_a_dsn_with_a_password_inlines_it():
    assert inlines_pg_password(DSN) is True


def test_a_named_collection_keeps_the_password_out_of_the_ddl(monkeypatch):
    """The secure path: nothing is inlined, so there is nothing to warn about."""
    monkeypatch.setenv("ENERGYDB_CH_PG_COLLECTION", "pg_energydb")
    assert inlines_pg_password(DSN) is False
    # And the DDL really does omit it, the property the warning is a proxy for.
    assert "s3cret" not in engine_table_ddl(DSN, "public")


def test_a_passwordless_dsn_is_not_worth_warning_about():
    """Trust auth / local compose inlines no secret. Warning there would fire on
    every dev ``create()`` and train people to ignore the message that matters."""
    assert inlines_pg_password(_PASSWORDLESS_DSN) is False
    assert inlines_pg_password("postgresql://app_user:@db.example.com/proddb") is False


def test_a_percent_encoded_password_still_counts():
    """The DSN parser must decode before deciding, an encoded password is still a
    password, and it is what lands in the DDL."""
    assert inlines_pg_password("postgresql://u:p%40ss@db.example.com/proddb") is True


@pytestmark_live
def test_provisioning_warns_that_the_password_lands_in_the_ddl(client, caplog):
    """The residual ask behind #100: the insecure default was silent."""
    with caplog.at_level(logging.WARNING, logger="energydb.client"):
        client.setup_ch_meta_engine()

    records = [r for r in caplog.records if r.name == "energydb.client"]
    assert len(records) == 1
    message = records[0].getMessage()
    assert "SHOW CREATE TABLE" in message  # names the exposure
    assert "ENERGYDB_CH_PG_COLLECTION" in message  # names the fix
    # Provisioning still succeeded: this is a warning, not a new requirement.
    assert _engine_table_exists(client)
    assert client.get_node("P").read(data_type="actual", name="power").height == 2


@pytestmark_live
def test_create_emits_it_too(client, caplog):
    """One funnel serves both entry points, but assert both: create() is the one
    every deployment runs, and its provisioning is best-effort/exception-swallowing."""
    with caplog.at_level(logging.WARNING, logger="energydb.client"):
        client.create()

    warnings = [r for r in caplog.records if r.name == "energydb.client" and "SHOW CREATE TABLE" in r.getMessage()]
    assert len(warnings) == 1


@pytestmark_live
def test_no_warning_when_a_named_collection_is_configured(client, monkeypatch, caplog):
    """The whole point of recommending the collection is that it silences this."""
    monkeypatch.setenv("ENERGYDB_CH_PG_COLLECTION", "pg_energydb_absent")
    # The collection does not exist on this server, so the DDL fails, but the
    # warning decision happens first, which is exactly what is under test.
    with caplog.at_level(logging.WARNING, logger="energydb.client"), contextlib.suppress(Exception):
        client.setup_ch_meta_engine()

    assert not [r for r in caplog.records if "SHOW CREATE TABLE" in r.getMessage()]
    # No restore needed: the client fixture is per-test and re-provisions on setup.
