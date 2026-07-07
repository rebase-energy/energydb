"""ClickHouse ↔ PG metadata-bridge infrastructure tests.

Covers the ``_ch_meta_engine`` DDL (credential/vantage resolution), the
best-effort engine-table provisioning in ``Client.create()`` /  teardown in
``Client.delete()``, and the session-cached degrade of the ``concurrent``
read path.

DDL tests are pure (no DB). The live tests follow the suite convention:
skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL`` are not set.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import energydb as edb
import polars as pl
import pytest
from energydb import Client
from energydb._ch_meta_engine import CH_ENGINE_TABLE, engine_table_ddl, series_meta_view_ddl

DSN = "postgresql://app_user:s3cret@db.example.com:6543/proddb"


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
    ddl = engine_table_ddl(DSN, "public")
    assert "ENGINE = PostgreSQL(energydb_pg, table = 'series_meta')" in ddl
    assert "s3cret" not in ddl and "postgres:5432" not in ddl


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
