"""Output-mode contract tests for ``Client.read`` / ``Client.read_relative``.

Covers all four ``(output, backend)`` combinations plus the documented
edge cases:

* ``output="frame"``  + ``backend="polars"`` → ``pl.DataFrame``
* ``output="frame"``  + ``backend="pandas"`` → ``pd.DataFrame``
* ``output="by_path"`` + ``backend="polars"`` → ``dict[tuple, pl.DataFrame]``
* ``output="by_path"`` + ``backend="pandas"`` → ``dict[tuple, pd.DataFrame]``
* Empty CH result in ``by_path`` mode → dict with an empty sub-frame per series
* Per-series ``valid_time`` ordering preserved (with secondary ``kt``/``ct`` keys)
* Invalid ``output`` / ``backend`` strings raise

Skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL`` are not set.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import energydb as edb
import pandas as pd
import polars as pl
import pytest
from energydb import Client

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set: skipping output-mode tests",
        allow_module_level=True,
    )


BASE = datetime(2026, 1, 1, tzinfo=UTC)
KT_1 = datetime(2026, 1, 1, 6, tzinfo=UTC)
KT_2 = datetime(2026, 1, 1, 7, tzinfo=UTC)


def _hours(n: int) -> list[datetime]:
    return [BASE + timedelta(hours=i) for i in range(n)]


@pytest.fixture
def client():
    c = Client()
    c.delete()
    c.create()
    # Two assets with one FLAT series each, plus one third asset that we
    # leave empty to exercise the "no CH rows" branch.
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.wind.WindTurbine(name="T1", capacity=3.5),
            edb.wind.WindTurbine(name="T2", capacity=3.5),
            edb.wind.WindTurbine(name="T3", capacity=3.5),
        ],
    )
    c.register_tree(tree)
    for tname in ("T1", "T2", "T3"):
        c.get_node("P", tname).register_series(
            name="power",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
            retention="forever",
        )
    # Write data only to T1 and T2.
    write_df = pl.DataFrame(
        {
            "path": ["P/T1", "P/T1", "P/T2", "P/T2"],
            "data_type": ["actual"] * 4,
            "name": ["power"] * 4,
            "valid_time": _hours(2) * 2,
            "value": [1.0, 2.0, 10.0, 20.0],
        }
    )
    c.write(write_df)
    yield c
    c.delete()
    c.close()


def _manifest(*paths: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "path": list(paths),
            "data_type": ["actual"] * len(paths),
            "name": ["power"] * len(paths),
        }
    )


# ---------------------------------------------------------------------------
# (output, backend) matrix
# ---------------------------------------------------------------------------


def test_frame_polars_default(client):
    out = client.read(_manifest("P/T1", "P/T2"))
    assert isinstance(out, pl.DataFrame)
    assert set(out.columns) == {"path", "data_type", "name", "valid_time", "value"}
    # series_id, node_uuid, node_type, node are no longer exposed
    assert "series_id" not in out.columns
    assert "node_uuid" not in out.columns
    assert "node_type" not in out.columns


def test_frame_pandas(client):
    out = client.read(_manifest("P/T1", "P/T2"), backend="pandas")
    assert isinstance(out, pd.DataFrame)
    assert set(out.columns) == {"path", "data_type", "name", "valid_time", "value"}


def test_by_path_polars(client):
    out = client.read(_manifest("P/T1", "P/T2"), output="by_path")
    assert isinstance(out, dict)
    assert set(out.keys()) == {
        ("P/T1", "actual", "power"),
        ("P/T2", "actual", "power"),
    }
    sub = out[("P/T1", "actual", "power")]
    assert isinstance(sub, pl.DataFrame)
    # Sub-frame carries only the data columns; identity is the dict key.
    assert set(sub.columns) == {"valid_time", "value"}
    assert sub["value"].to_list() == [1.0, 2.0]


def test_by_path_pandas(client):
    out = client.read(_manifest("P/T1", "P/T2"), output="by_path", backend="pandas")
    assert isinstance(out, dict)
    sub = out[("P/T1", "actual", "power")]
    assert isinstance(sub, pd.DataFrame)
    assert sub["value"].to_list() == [1.0, 2.0]


# ---------------------------------------------------------------------------
# by_path edge cases
# ---------------------------------------------------------------------------


def test_by_path_empty_series_returns_empty_subframe(client):
    """T3 has no data; the key still appears with an empty DataFrame."""
    out = client.read(_manifest("P/T1", "P/T3"), output="by_path")
    assert isinstance(out, dict)
    assert ("P/T3", "actual", "power") in out
    sub = out[("P/T3", "actual", "power")]
    assert sub.is_empty()
    assert set(sub.columns) == {"valid_time", "value"}


def test_by_path_with_knowledge_time_columns(client):
    """The opt-in `include_knowledge_time` column is preserved per-series.

    For FLAT series the result has the latest row per valid_time across
    knowledge_times, so the sub-frame still carries valid_time + value
    + knowledge_time.
    """
    out = client.read(
        _manifest("P/T1", "P/T2"),
        output="by_path",
        include_knowledge_time=True,
    )
    sub = out[("P/T1", "actual", "power")]
    assert "knowledge_time" in sub.columns
    # Sorted by valid_time ascending
    vts = sub["valid_time"].to_list()
    assert vts == sorted(vts)


def test_by_path_sort_order_within_series(client):
    """``valid_time`` must be ascending within each per-series sub-frame.

    Inserts deliberately out of natural order; reads must return them sorted.
    """
    c = client
    # Append rows in reverse order to confirm the SQL ORDER BY is doing
    # the work (not the insert order).
    rev_df = pl.DataFrame(
        {
            "path": ["P/T1"] * 3,
            "data_type": ["actual"] * 3,
            "name": ["power"] * 3,
            # valid_times outside the existing fixture window
            "valid_time": [BASE + timedelta(hours=h) for h in (10, 8, 9)],
            "value": [99.0, 97.0, 98.0],
        }
    )
    c.write(rev_df)
    out = c.read(_manifest("P/T1"), output="by_path")
    sub = out[("P/T1", "actual", "power")]
    vts = sub["valid_time"].to_list()
    assert vts == sorted(vts)


# ---------------------------------------------------------------------------
# Invalid arguments
# ---------------------------------------------------------------------------


def test_invalid_output_raises(client):
    with pytest.raises(ValueError, match="output"):
        client.read(_manifest("P/T1"), output="not_a_mode")


def test_invalid_backend_raises(client):
    with pytest.raises(ValueError, match="backend"):
        client.read(_manifest("P/T1"), backend="numpy")


def test_manifest_with_list_utf8_paths_is_rejected(client):
    """The hard-break migration message must point users at the new shape."""
    bad = pl.DataFrame(
        {
            "path": [["P", "T1"]],
            "data_type": ["actual"],
            "name": ["power"],
        },
        schema={"path": pl.List(pl.Utf8), "data_type": pl.Utf8, "name": pl.Utf8},
    )
    with pytest.raises(ValueError, match="Utf8 joined with '/'"):
        client.read(bad)


# ---------------------------------------------------------------------------
# Scope-style auto-strip
# ---------------------------------------------------------------------------


def test_scope_single_series_read_strips_identity(client):
    """Scope-style read on a fully-qualified single series drops path/data_type/name.

    The caller already knows them via the scope expression, re-broadcasting
    on every row is pure noise.
    """
    out = client.get_node("P", "T1").read(data_type="actual", name="power")
    assert isinstance(out, pl.DataFrame)
    assert set(out.columns) == {"valid_time", "value"}
    assert out["value"].to_list() == [1.0, 2.0]


def test_scope_single_series_read_pandas_strips_identity(client):
    out = client.get_node("P", "T1").read(data_type="actual", name="power", backend="pandas")
    assert isinstance(out, pd.DataFrame)
    assert set(out.columns) == {"valid_time", "value"}


def test_scope_subtree_read_keeps_full_shape(client):
    """When the scope resolves to multiple series, identity cols stay.

    ``get_node("P").read()`` spans all WindTurbines under P (T1, T2, T3, each
    with one series), so the manifest has 3 rows. With a multi-series read we
    can't infer a single identity, so ``path`` / ``data_type`` / ``name``
    remain on every row.
    """
    out = client.get_node("P").read()
    assert isinstance(out, pl.DataFrame)
    # T3 has no data, so only T1+T2 contribute rows.
    assert "path" in out.columns
    assert "data_type" in out.columns
    assert "name" in out.columns
    assert set(out["path"].unique().to_list()) == {"P/T1", "P/T2"}


def test_scope_single_series_with_by_path_returns_dict(client):
    """``output='by_path'`` skips auto-strip: caller asked for the dict shape explicitly."""
    out = client.get_node("P", "T1").read(data_type="actual", name="power", output="by_path")
    assert isinstance(out, dict)
    assert ("P/T1", "actual", "power") in out
    sub = out[("P/T1", "actual", "power")]
    assert set(sub.columns) == {"valid_time", "value"}


# ---------------------------------------------------------------------------
# Empty-read schema stability (energydb#5): a zero-row result must keep the
# same columns a non-empty result would carry.
# ---------------------------------------------------------------------------


def test_scope_read_zero_rows_in_range_keeps_schema(client):
    """Regression repro: a far-future start_valid resolves the series but CH
    returns no rows; the historical bug returned a schema-less frame here,
    so df["valid_time"] would raise."""
    out = client.get_node("P", "T1").read(
        data_type="actual",
        name="power",
        start_valid=datetime(2099, 1, 1, tzinfo=UTC),
    )
    assert isinstance(out, pl.DataFrame)
    assert set(out.columns) == {"valid_time", "value"}
    assert out.height == 0
    list(zip(out["valid_time"], out["value"], strict=True))


def test_scope_read_zero_rows_pandas_keeps_schema(client):
    out = client.get_node("P", "T1").read(
        data_type="actual",
        name="power",
        backend="pandas",
        start_valid=datetime(2099, 1, 1, tzinfo=UTC),
    )
    assert isinstance(out, pd.DataFrame)
    assert list(out.columns) == ["valid_time", "value"]
    assert len(out) == 0


def test_scope_read_no_matching_series_keeps_full_identity_shape(client):
    """Zero resolved series (not just zero CH rows) keeps the full identity
    shape: there is no single series to strip the identity columns for."""
    out = client.get_node("P", "T1").read(data_type="does-not-exist", name="power")
    assert isinstance(out, pl.DataFrame)
    assert set(out.columns) == {"path", "data_type", "name", "valid_time", "value"}
    assert out.height == 0


def test_client_read_zero_rows_in_range_keeps_schema(client):
    """Same regression via the manifest (``client.read``) path."""
    out = client.read(_manifest("P/T1"), start_valid=datetime(2099, 1, 1, tzinfo=UTC))
    assert isinstance(out, pl.DataFrame)
    assert set(out.columns) == {"path", "data_type", "name", "valid_time", "value"}
    assert out.height == 0
    list(zip(out["valid_time"], out["value"], strict=True))


def test_by_path_zero_rows_in_range_unchanged(client):
    """``output='by_path'`` already kept the right per-series shape; pin it."""
    out = client.read(_manifest("P/T1"), output="by_path", start_valid=datetime(2099, 1, 1, tzinfo=UTC))
    sub = out[("P/T1", "actual", "power")]
    assert sub.is_empty()
    assert set(sub.columns) == {"valid_time", "value"}


# ---------------------------------------------------------------------------
# SeriesKey / EdgeSeriesKey NamedTuple result keys
# ---------------------------------------------------------------------------


def test_by_path_keys_are_series_key(client):
    """``output='by_path'`` keys are :class:`SeriesKey` instances.

    They are still tuple-equal to ``(path, data_type, name)``, so old
    positional access keeps working. New code can use attribute access.
    """
    out = client.read(_manifest("P/T1", "P/T2"), output="by_path")
    keys = list(out.keys())
    assert all(isinstance(k, edb.SeriesKey) for k in keys)
    # Backwards-compat: positional access still works (NamedTuple == tuple).
    assert out[("P/T1", "actual", "power")]["value"].to_list() == [1.0, 2.0]
    # Attribute access: the whole point.
    one = [k for k in keys if k.path == "P/T1"][0]
    assert one.path == "P/T1"
    assert one.data_type == "actual"
    assert one.name == "power"


def test_by_path_series_key_constructor_matches_dict_key(client):
    """``edb.SeriesKey("P/T1","actual","power")`` indexes the dict directly."""
    out = client.read(_manifest("P/T1"), output="by_path")
    key = edb.SeriesKey(path="P/T1", data_type="actual", name="power")
    assert key in out
    assert out[key]["value"].to_list() == [1.0, 2.0]


def test_find_filters_by_name(client):
    """``edb.find(result, name="power")`` returns matching (key, df) pairs."""
    out = client.read(_manifest("P/T1", "P/T2"), output="by_path")
    matches = edb.find(out, name="power")
    assert len(matches) == 2
    assert {k.path for k, _ in matches} == {"P/T1", "P/T2"}


def test_find_filters_by_path(client):
    out = client.read(_manifest("P/T1", "P/T2"), output="by_path")
    matches = edb.find(out, path="P/T1")
    assert len(matches) == 1
    assert matches[0][0].path == "P/T1"


def test_find_empty_match(client):
    """Filter that matches nothing returns []."""
    out = client.read(_manifest("P/T1"), output="by_path")
    assert edb.find(out, name="does-not-exist") == []


def test_find_unknown_attribute_matches_nothing(client):
    """Unknown filter attribute matches nothing (defensive)."""
    out = client.read(_manifest("P/T1"), output="by_path")
    assert edb.find(out, nonexistent_attr="x") == []


# ---------------------------------------------------------------------------
# Engine-parallel reads must be byte-identical to the sequential path
# ---------------------------------------------------------------------------


def test_engine_read_matches_sequential(client, monkeypatch):
    """Engine-parallel reads return the identical labelled result as the sequential path:
    node scope (4 bitemporal modes + by_path) and the path-routed manifest. (Edge reads run
    through the same engine path in the live edge tests.)

    Skips if the engine can't reach Postgres from ClickHouse's network vantage (the
    CREATE TABLE never dials PG, so an unreachable engine only surfaces on first use,
    e.g. local docker without ENERGYDB_CH_PG_HOST=postgres:5432). Runs in strict mode so
    a broken engine path raises instead of silently falling back (which would trivially pass).
    """
    import energydb._io as _io
    from energydb._ch_meta_engine import CH_ENGINE_TABLE
    from polars.testing import assert_frame_equal

    try:
        client.setup_ch_meta_engine()
        client.td._ch.command(f"SELECT count() FROM {CH_ENGINE_TABLE}")  # force a CH->PG connection
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"CH meta engine not usable in this env: {exc}")
    monkeypatch.setattr(_io, "_ENGINE_STRICT", True)

    def seq(fn):
        """Run ``fn`` with the engine disabled (the sequential baseline), then re-enable."""
        client._async._engine_unavailable = True
        try:
            return fn()
        finally:
            client._async._engine_unavailable = False

    scope = client.get_node("P")
    for kw in (
        {},  # read_latest
        {"include_updates": True},  # latest + changes
        {"include_knowledge_time": True},  # overlapping
        {"include_updates": True, "include_knowledge_time": True},  # overlapping + changes
    ):
        base = seq(lambda kw=kw: scope.read(data_type="actual", name="power", **kw))
        eng = scope.read(data_type="actual", name="power", **kw)
        assert_frame_equal(base.sort(base.columns), eng.sort(eng.columns))

    base_bp = seq(lambda: scope.read(data_type="actual", name="power", output="by_path"))
    eng_bp = scope.read(data_type="actual", name="power", output="by_path")
    assert base_bp.keys() == eng_bp.keys()
    for key, sub in base_bp.items():
        assert_frame_equal(sub.sort(sub.columns), eng_bp[key].sort(eng_bp[key].columns))

    manifest = _manifest("P/T1", "P/T2")
    base_m = seq(lambda: client.read(manifest))
    eng_m = client.read(manifest)
    assert_frame_equal(base_m.sort(base_m.columns), eng_m.sort(eng_m.columns))

    # read_relative rides the same engine path; the huge issue_offset keeps every
    # row inside its window even though the fixture's knowledge_time is now().
    rel_kw = {"window_length": timedelta(hours=1), "issue_offset": timedelta(days=365), "start_window": BASE}
    base_r = seq(lambda: scope.read_relative(data_type="actual", name="power", **rel_kw))
    eng_r = scope.read_relative(data_type="actual", name="power", **rel_kw)
    assert eng_r.height > 0  # the engine leg must self-resolve (guards the meta_source wiring)
    assert_frame_equal(base_r.sort(base_r.columns), eng_r.sort(eng_r.columns))
