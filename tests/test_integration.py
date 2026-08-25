"""Integration tests for the energydb → timedb stack.

Skipped if TIMEDB_PG_DSN or TIMEDB_CH_URL are not set. Tests run against a
fresh schema and clean up after themselves.
"""

import asyncio
import os
from datetime import UTC, datetime, timedelta

import pandas as pd
import polars as pl
import pytest
from energydb import AsyncClient, Client

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set: skipping integration tests",
        allow_module_level=True,
    )


BASE_VT = datetime(2026, 1, 1, tzinfo=UTC)
KT_1 = datetime(2026, 1, 1, 6, tzinfo=UTC)
KT_2 = datetime(2026, 1, 1, 7, tzinfo=UTC)


def _ts_df(n: int = 4) -> pl.DataFrame:
    times = pl.datetime_range(
        start=BASE_VT,
        end=BASE_VT + timedelta(hours=n - 1),
        interval="1h",
        time_unit="us",
        time_zone="UTC",
        eager=True,
    )
    return pl.DataFrame(
        {
            "valid_time": times,
            "value": [float(i) for i in range(n)],
        }
    )


@pytest.fixture
def edb():
    client = Client()
    client.delete()
    client.create()
    # Seed a simple hierarchy root via register_tree (uuid-aware).
    import energydb as edb_mod

    asset = edb_mod.wind.WindTurbine(name="asset_a", capacity=1.0)
    root = edb_mod.Portfolio(name="root", members=[asset])
    client.register_tree(root)
    yield client
    client.delete()
    client.close()


def test_register_flat_series_and_write_read(edb):
    sid = (
        edb.get_node("root")
        .get_node("asset_a")
        .register_series(
            name="capacity",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
            retention="medium",
        )
    )
    assert isinstance(sid, int) and sid > 0

    edb.get_node("root").get_node("asset_a").write(
        _ts_df(3),
        data_type="actual",
        name="capacity",
    )

    result = edb.get_node("root").get_node("asset_a").read(data_type="actual", name="capacity")
    # series_id is no longer surfaced; the public result is slim.
    assert "series_id" not in result.columns
    assert result["value"].to_list() == [0.0, 1.0, 2.0]


def test_register_overlapping_series_requires_knowledge_time(edb):
    edb.get_node("root").get_node("asset_a").register_series(
        name="power",
        canonical_unit="MW",
        data_type="forecast",
        timeseries_type="OVERLAPPING",
        retention="medium",
    )

    with pytest.raises(ValueError, match="knowledge_time is required for OVERLAPPING"):
        edb.get_node("root").get_node("asset_a").write(
            _ts_df(2),
            data_type="forecast",
            name="power",
        )


def test_overlapping_write_read_with_kt(edb):
    edb.get_node("root").get_node("asset_a").register_series(
        name="power",
        canonical_unit="MW",
        data_type="forecast",
        timeseries_type="OVERLAPPING",
        retention="medium",
    )

    edb.get_node("root").get_node("asset_a").write(
        _ts_df(2),
        data_type="forecast",
        name="power",
        knowledge_time=KT_1,
    )
    edb.get_node("root").get_node("asset_a").write(
        _ts_df(2).with_columns(pl.col("value") + 100),
        data_type="forecast",
        name="power",
        knowledge_time=KT_2,
    )

    # Latest: KT_2 wins
    latest = (
        edb.get_node("root")
        .get_node("asset_a")
        .read(
            data_type="forecast",
            name="power",
        )
    )
    assert latest["value"].to_list() == [100.0, 101.0]

    # History: both kts present
    history = (
        edb.get_node("root")
        .get_node("asset_a")
        .read(
            data_type="forecast",
            name="power",
            include_knowledge_time=True,
        )
    )
    assert len(history) == 4


def test_cross_retention_read_is_single_query(edb):
    """Registering FLAT and OVERLAPPING series under one asset and reading all
    of them should return a combined result via one td.read (no partition_by).
    """
    asset = edb.get_node("root").get_node("asset_a")
    asset.register_series(
        name="capacity",
        canonical_unit="MW",
        data_type="actual",
        timeseries_type="FLAT",
        retention="medium",
    )
    asset.register_series(
        name="power",
        canonical_unit="MW",
        data_type="forecast",
        timeseries_type="OVERLAPPING",
        retention="medium",
    )
    asset.write(_ts_df(2), data_type="actual", name="capacity")
    asset.write(_ts_df(2), data_type="forecast", name="power", knowledge_time=KT_1)

    result = asset.read()
    # Two series × 2 rows = 4 rows
    assert len(result) == 4
    # Two distinct (data_type, name) pairs on the result; series_id is internal
    # and no longer surfaced.
    pairs = set(zip(result["data_type"].to_list(), result["name"].to_list(), strict=True))
    assert pairs == {("actual", "capacity"), ("forecast", "power")}


def test_read_runs_for_series_hydrates_pg_metadata(edb):
    sid = (
        edb.get_node("root")
        .get_node("asset_a")
        .register_series(
            name="capacity",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
            retention="medium",
        )
    )
    run_id = (
        edb.get_node("root")
        .get_node("asset_a")
        .write(
            _ts_df(2),
            data_type="actual",
            name="capacity",
            workflow_id="test_workflow",
            model_name="test_model",
        )
    )
    assert isinstance(run_id, int) and run_id > 0

    runs = edb.read_runs_for_series(series_id=sid)
    assert len(runs) == 1
    assert runs[0]["run_id"] == run_id
    assert runs[0]["workflow_id"] == "test_workflow"
    assert runs[0]["model_name"] == "test_model"


def test_retention_immutable_on_reregister(edb):
    # Immutability of retention / canonical_unit is enforced in Python by
    # register_series (there is no DB trigger). Re-registering the same series
    # with a different retention is rejected.
    scope = edb.get_node("root").get_node("asset_a")
    scope.register_series(
        name="capacity",
        canonical_unit="MW",
        data_type="actual",
        timeseries_type="FLAT",
        retention="medium",
    )
    with pytest.raises(ValueError, match="immutable"):
        scope.register_series(
            name="capacity",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
            retention="long",
        )


def test_pandas_in_pandas_out(edb):
    """A pandas DataFrame goes in, a pandas DataFrame comes out when
    ``backend="pandas"`` is requested."""
    edb.get_node("root").get_node("asset_a").register_series(
        name="capacity",
        canonical_unit="MW",
        data_type="actual",
        timeseries_type="FLAT",
        retention="medium",
    )

    pdf = pd.DataFrame(
        {
            "valid_time": pd.to_datetime(
                [BASE_VT + timedelta(hours=i) for i in range(3)],
                utc=True,
            ),
            "value": [10.0, 11.0, 12.0],
        }
    )
    edb.get_node("root").get_node("asset_a").write(pdf, data_type="actual", name="capacity")

    result = edb.get_node("root").get_node("asset_a").read(data_type="actual", name="capacity", backend="pandas")
    assert isinstance(result, pd.DataFrame)
    assert "valid_time" in result.columns
    assert "value" in result.columns
    assert result["value"].tolist() == [10.0, 11.0, 12.0]


def test_default_polars_empty_read(edb):
    """An empty-result read with the default polars output returns an empty
    polars DataFrame."""
    result = edb.get_node("root").get_node("asset_a").read(data_type="actual", name="nonexistent")
    assert isinstance(result, pl.DataFrame)
    assert len(result) == 0


# ---------------------------------------------------------------------------
# write_manifest run-row semantics (autocommit fold, W2)
# ---------------------------------------------------------------------------


def _runs_rows() -> list[int]:
    import psycopg

    with psycopg.connect(os.environ["TIMEDB_PG_DSN"]) as conn:
        return [r[0] for r in conn.execute("SELECT run_id FROM energydb.runs").fetchall()]


def _bad_manifest() -> pl.DataFrame:
    """Routes to a series that was never registered."""
    return _ts_df(2).with_columns(
        pl.lit("root/asset_a").alias("path"),
        pl.lit("actual").alias("data_type"),
        pl.lit("never_registered").alias("name"),
    )


def test_failed_write_with_kt_leaves_no_runs_row(edb):
    """kt-known writes run the resolve on an autocommit connection; a resolve
    failure must compensate the folded runs-upsert, not orphan the row."""
    with pytest.raises(ValueError, match="not registered"):
        edb.write(_bad_manifest(), knowledge_time=KT_1)
    assert _runs_rows() == []


def test_failed_write_with_explicit_run_id_keeps_runs_row(edb):
    """An explicit run_id may reference earlier successful batches, so the
    upserted row is deliberately left in place on failure."""
    with pytest.raises(ValueError, match="not registered"):
        edb.write(_bad_manifest(), knowledge_time=KT_1, run_id=424242)
    assert _runs_rows() == [424242]


def test_overlapping_write_without_kt_records_no_run(edb):
    """The kt-unknown path stays transactional: the OVERLAPPING error rolls
    back the folded run row."""
    edb.get_node("root").get_node("asset_a").register_series(
        name="power",
        canonical_unit="MW",
        data_type="forecast",
        timeseries_type="OVERLAPPING",
        retention="medium",
    )
    manifest = _ts_df(2).with_columns(
        pl.lit("root/asset_a").alias("path"),
        pl.lit("forecast").alias("data_type"),
        pl.lit("power").alias("name"),
    )
    with pytest.raises(ValueError, match="knowledge_time is required"):
        edb.write(manifest)
    assert _runs_rows() == []


def _node_uuid(path: str) -> str:
    import psycopg

    with psycopg.connect(os.environ["TIMEDB_PG_DSN"]) as conn:
        return conn.execute("SELECT uuid::text FROM energydb.node WHERE path = %s", (path,)).fetchone()[0]


def test_uuid_routed_write_records_run_in_folded_statement(edb):
    """Owner routes fold the runs upsert into the resolve statement (W3);
    a successful node_uuid-routed write must still record its run row."""
    edb.get_node("root").get_node("asset_a").register_series(
        name="capacity",
        canonical_unit="MW",
        data_type="actual",
        timeseries_type="FLAT",
        retention="medium",
    )
    manifest = _ts_df(2).with_columns(
        pl.lit(_node_uuid("root/asset_a")).alias("node_uuid"),
        pl.lit("actual").alias("data_type"),
        pl.lit("capacity").alias("name"),
    )
    run_id = edb.write(manifest, knowledge_time=KT_1)
    assert _runs_rows() == [int(run_id)]


def test_failed_uuid_routed_write_with_kt_leaves_no_runs_row(edb):
    """The compensating delete also covers the owner route now that its
    runs upsert commits with the folded resolve statement."""
    manifest = _ts_df(2).with_columns(
        pl.lit(_node_uuid("root/asset_a")).alias("node_uuid"),
        pl.lit("actual").alias("data_type"),
        pl.lit("never_registered").alias("name"),
    )
    with pytest.raises(ValueError, match="not registered"):
        edb.write(manifest, knowledge_time=KT_1)
    assert _runs_rows() == []


def test_concurrent_reads_on_one_async_client(edb):
    """Regression for issue #88: independent reads gathered on a single
    ``AsyncClient`` must overlap instead of raising ``ProgrammingError``
    ("concurrent queries within the same session")."""
    for name, offset in (("capacity", 0), ("power", 100)):
        edb.get_node("root").get_node("asset_a").register_series(
            name=name,
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
            retention="medium",
        )
        edb.get_node("root").get_node("asset_a").write(
            _ts_df(3).with_columns(pl.col("value") + offset),
            data_type="actual",
            name=name,
        )

    manifest = pl.DataFrame(
        {
            "path": ["root/asset_a"] * 2,
            "data_type": ["actual"] * 2,
            "name": ["capacity", "power"],
        }
    )

    async def _gathered():
        client = AsyncClient()
        await client.open()
        try:
            return await asyncio.gather(
                client.get_node("root").get_node("asset_a").read(data_type="actual", name="capacity"),
                client.get_node("root").get_node("asset_a").read(data_type="actual", name="power"),
                client.read(manifest),
            )
        finally:
            await client.close()

    capacity, power, both = asyncio.run(_gathered())
    assert capacity["value"].to_list() == [0.0, 1.0, 2.0]
    assert power["value"].to_list() == [100.0, 101.0, 102.0]
    assert sorted(both["value"].to_list()) == [0.0, 1.0, 2.0, 100.0, 101.0, 102.0]
