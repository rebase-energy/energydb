"""Integration tests for the energydb → timedb stack.

Skipped if TIMEDB_PG_DSN or TIMEDB_CH_URL are not set. Tests run against a
fresh schema and clean up after themselves.
"""

import os
from datetime import UTC, datetime, timedelta

import pandas as pd
import polars as pl
import pytest
from energydb import Client

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set — skipping integration tests",
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

    result = (
        edb.get_node("root")
        .get_node("asset_a")
        .read(
            data_type="actual",
            name="capacity",
            output="polars",
        )
    )
    assert "series_id" in result.columns
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
            output="polars",
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
            output="polars",
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

    result = asset.read(output="polars")
    # Two series × 2 rows = 4 rows
    assert len(result) == 4
    assert set(result["series_id"].unique().to_list()) == {1, 2} or len(result["series_id"].unique()) == 2


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


def test_retention_immutable_trigger(edb):
    edb.get_node("root").get_node("asset_a").register_series(
        name="capacity",
        canonical_unit="MW",
        data_type="actual",
        timeseries_type="FLAT",
        retention="medium",
    )
    import psycopg

    with edb._pool.connection() as conn, pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        conn.execute("UPDATE energydb.series SET retention = 'long' WHERE name = 'capacity'")


def test_pandas_in_pandas_out(edb):
    """A pandas DataFrame goes in, a pandas DataFrame comes out by default."""
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

    result = edb.get_node("root").get_node("asset_a").read(data_type="actual", name="capacity")
    assert isinstance(result, pd.DataFrame)
    assert "valid_time" in result.columns
    assert "value" in result.columns
    assert result["value"].tolist() == [10.0, 11.0, 12.0]


def test_pandas_empty_read(edb):
    """An empty-result read with the default pandas output returns an empty
    pandas DataFrame, not a polars one."""
    result = edb.get_node("root").get_node("asset_a").read(data_type="actual", name="nonexistent")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0
