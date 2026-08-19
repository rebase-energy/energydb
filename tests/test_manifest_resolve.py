"""Regression tests for ``paths.resolve_manifest``.

The original join-based implementation could leak internal ``_dt`` /
``_name`` columns into the resolved frame; the current hash-prejoin
implementation can't, but the no-leak contract is still pinned here.
Also covers the ``(resolved, summary)`` return shape and per-row column
contract (``series_id`` / ``retention`` / ``canonical_unit`` only —
``timeseries_type`` lives in the summary now).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import polars as pl
from energydb.paths import resolve_manifest


def _mock_conn_with_series(rows: list[tuple]) -> MagicMock:
    """An async-aware mock connection for the ``await (await execute).fetchall()`` path."""
    cursor = MagicMock()
    cursor.fetchall = AsyncMock(return_value=rows)
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=cursor)
    return conn


def test_node_manifest_resolve_does_not_leak_internal_join_columns():
    node_uuid = uuid4()
    # PG returns owner uuid as ``::text`` after the SQL cast — mock with str.
    # Column order: owner::text, data_type, name, series_id, canonical_unit,
    #               timeseries_type, retention, [path when attach_path=True].
    rows = [
        (
            str(node_uuid),  # node_uuid::text
            "actual",  # data_type
            "power",  # name
            42,  # series_id
            "MW",  # canonical_unit
            "FLAT",  # timeseries_type
            "forever",  # retention
            "Europe/Sweden",  # path (attached via JOIN node)
        ),
    ]
    conn = _mock_conn_with_series(rows)

    manifest = pl.DataFrame(
        {
            "node_uuid": [str(node_uuid)],
            "data_type": ["actual"],
            "name": ["power"],
        }
    )
    resolved, summary = asyncio.run(resolve_manifest(conn, manifest))

    assert "_dt" not in resolved.columns
    assert "_name" not in resolved.columns
    assert "_triple_k" not in resolved.columns
    assert "series_id" in resolved.columns
    assert "canonical_unit" in resolved.columns
    assert "retention" in resolved.columns
    # path now rides along on the resolve so the post-read attach is PG-free.
    assert "path" in resolved.columns
    # timeseries_type now lives in the summary, not per-row.
    assert "timeseries_type" not in resolved.columns
    assert summary.has_overlapping is False


def test_edge_manifest_resolve_does_not_leak_internal_join_columns():
    edge_uuid = uuid4()
    rows = [
        (
            str(edge_uuid),  # edge_uuid::text
            "actual",
            "power",
            7,
            "MW",
            "FLAT",
            "forever",
            "Cable",  # edge_type
            "circuit-1",  # edge_name
            "Europe/Sweden/A",  # from_path
            "Europe/Sweden/B",  # to_path
        ),
    ]
    conn = _mock_conn_with_series(rows)

    manifest = pl.DataFrame(
        {
            "edge_uuid": [str(edge_uuid)],
            "data_type": ["actual"],
            "name": ["power"],
        }
    )
    resolved, summary = asyncio.run(resolve_manifest(conn, manifest))

    assert "_dt" not in resolved.columns
    assert "_name" not in resolved.columns
    assert "_triple_k" not in resolved.columns
    assert "series_id" in resolved.columns
    assert "canonical_unit" in resolved.columns
    assert "retention" in resolved.columns
    # Endpoint paths + edge_type + the edge's own name ride along on the resolve.
    assert "edge_type" in resolved.columns
    assert resolved["edge_name"].to_list() == ["circuit-1"]
    assert "from_path" in resolved.columns
    assert "to_path" in resolved.columns
    assert "timeseries_type" not in resolved.columns
    assert summary.has_overlapping is False


def test_overlapping_surfaces_in_summary():
    """Set-level OVERLAPPING signal lives on the summary, not per-row."""
    node_uuid = uuid4()
    rows = [
        (str(node_uuid), "forecast", "v1", 99, "MW", "OVERLAPPING", "medium", "P/Site"),
    ]
    conn = _mock_conn_with_series(rows)

    manifest = pl.DataFrame(
        {
            "node_uuid": [str(node_uuid)] * 3,
            "data_type": ["forecast"] * 3,
            "name": ["v1"] * 3,
        }
    )
    resolved, summary = asyncio.run(resolve_manifest(conn, manifest))

    assert summary.has_overlapping is True
    assert summary.overlapping_series_ids == frozenset({99})
    # series_id attached to every row.
    assert resolved["series_id"].to_list() == [99, 99, 99]


def test_summary_names_only_the_overlapping_series_by_uuid_route():
    """``overlapping_series_ids`` is what lets one write dedupe FLAT and
    OVERLAPPING series by different keys, so it must name exactly the
    OVERLAPPING ones — not all resolved series, not just a boolean."""
    n1, n2, n3 = uuid4(), uuid4(), uuid4()
    rows = [
        (str(n1), "actual", "power", 1, "MW", "FLAT", "forever", "P/A"),
        (str(n2), "forecast", "power", 2, "MW", "OVERLAPPING", "medium", "P/B"),
        (str(n3), "forecast", "power", 3, "MW", "OVERLAPPING", "medium", "P/C"),
    ]
    manifest = pl.DataFrame(
        {
            "node_uuid": [str(n1), str(n2), str(n3)],
            "data_type": ["actual", "forecast", "forecast"],
            "name": ["power"] * 3,
        }
    )
    _resolved, summary = asyncio.run(resolve_manifest(_mock_conn_with_series(rows), manifest))

    assert summary.overlapping_series_ids == frozenset({2, 3})
    assert summary.has_overlapping is True


def test_summary_names_only_the_overlapping_series_by_path_route():
    """Same contract on the path route — the ids come from the resolved metadata
    either way, so both routes must agree."""
    rows = [
        ("P/A", "actual", "power", 10, "MW", "FLAT", "forever", str(uuid4())),
        ("P/B", "forecast", "power", 20, "MW", "OVERLAPPING", "medium", str(uuid4())),
    ]
    manifest = pl.DataFrame(
        {
            "path": ["P/A", "P/B"],
            "data_type": ["actual", "forecast"],
            "name": ["power", "power"],
        }
    )
    _resolved, summary = asyncio.run(resolve_manifest(_mock_conn_with_series(rows), manifest))

    assert summary.overlapping_series_ids == frozenset({20})


def test_summary_is_empty_for_a_flat_only_manifest():
    """Empty set → has_overlapping False → 'auto' behaves exactly like 'valid_time'."""
    rows = [("P/A", "actual", "power", 10, "MW", "FLAT", "forever", str(uuid4()))]
    manifest = pl.DataFrame({"path": ["P/A"], "data_type": ["actual"], "name": ["power"]})
    _resolved, summary = asyncio.run(resolve_manifest(_mock_conn_with_series(rows), manifest))

    assert summary.overlapping_series_ids == frozenset()
    assert summary.has_overlapping is False
    # ``missing`` is always present — zero-row, with the route's schema — so a
    # caller can select on it without first checking whether anything was skipped.
    assert summary.missing.is_empty()
    assert summary.missing.columns == ["path", "data_type", "name"]


def test_unresolved_triple_raises_with_owner_message():
    """A triple not returned by PG raises with the historical owner/dt/name message."""
    node_uuid = uuid4()
    # PG returns nothing — every triple in the manifest is unresolved.
    conn = _mock_conn_with_series([])

    manifest = pl.DataFrame(
        {
            "node_uuid": [str(node_uuid)],
            "data_type": ["actual"],
            "name": ["power"],
        }
    )

    import pytest

    with pytest.raises(ValueError, match="Series not registered for node_uuid="):
        asyncio.run(resolve_manifest(conn, manifest))


def test_path_manifest_read_resolves_in_one_round_trip():
    """The path route is a single PG statement for reads (attach_path=True),
    and surfaces node_uuid on the resolved frame for the hierarchy attach."""
    node_uuid = str(uuid4())
    # New path-route row layout: path, data_type, name, series_id,
    # canonical_unit, timeseries_type, retention, node_uuid::text.
    rows = [("P/T1", "actual", "power", 7, "MW", "FLAT", "forever", node_uuid)]
    conn = _mock_conn_with_series(rows)

    manifest = pl.DataFrame({"path": ["P/T1"], "data_type": ["actual"], "name": ["power"]})
    resolved, summary = asyncio.run(resolve_manifest(conn, manifest))

    assert conn.execute.await_count == 1  # the collapse: one statement, not path->uuid + owner scan
    assert resolved["node_uuid"].to_list() == [node_uuid]
    assert resolved["series_id"].to_list() == [7]
    assert resolved["path"].to_list() == ["P/T1"]  # manifest path kept (== DB path by the join)
    assert summary.has_overlapping is False


def test_path_manifest_missing_path_raises_series_not_registered():
    """A path PG can't match raises the same 'Series not registered for path=...'
    contract as the write route (formerly a separate 'Could not resolve path(s)')."""
    import pytest

    conn = _mock_conn_with_series([])
    manifest = pl.DataFrame({"path": ["P/NOPE"], "data_type": ["actual"], "name": ["power"]})

    with pytest.raises(ValueError, match="Series not registered for path="):
        asyncio.run(resolve_manifest(conn, manifest))


def test_edge_triple_manifest_resolves_in_one_round_trip():
    """(from_path, to_path, edge_type) routing is a single PG statement and
    attaches edge_uuid so the read pipeline detects/projects it as an edge."""
    edge_uuid = str(uuid4())
    # Edge-triple row layout: from_path, to_path, edge_type, edge name, data_type,
    # series name, series_id, canonical_unit, timeseries_type, retention,
    # edge_uuid::text.
    rows = [("Grid/A", "Grid/B", "Line", None, "actual", "flow", 11, "MW", "FLAT", "forever", edge_uuid)]
    conn = _mock_conn_with_series(rows)

    manifest = pl.DataFrame(
        {
            "from_path": ["Grid/A"],
            "to_path": ["Grid/B"],
            "edge_type": ["Line"],
            "data_type": ["actual"],
            "name": ["flow"],
        }
    )
    resolved, summary = asyncio.run(resolve_manifest(conn, manifest))

    assert conn.execute.await_count == 1  # one edge⋈node⋈node⋈series statement
    assert "_triple_k" not in resolved.columns
    assert resolved["edge_uuid"].to_list() == [edge_uuid]  # needed by is_edge detection / projection
    assert resolved["series_id"].to_list() == [11]
    # Endpoint paths + type kept (== DB values by the join equality).
    assert resolved["from_path"].to_list() == ["Grid/A"]
    assert resolved["to_path"].to_list() == ["Grid/B"]
    assert resolved["edge_type"].to_list() == ["Line"]
    # The edge's own name rides along too — null here, for an unnamed edge.
    assert resolved["edge_name"].to_list() == [None]
    assert summary.has_overlapping is False


def test_edge_triple_partial_columns_raises():
    """A strict subset of the triple columns is a usage error, not another route."""
    import pytest

    conn = _mock_conn_with_series([])
    manifest = pl.DataFrame({"from_path": ["Grid/A"], "to_path": ["Grid/B"], "data_type": ["actual"], "name": ["flow"]})

    with pytest.raises(ValueError, match=r"Edge-triple routing requires all of .*missing \['edge_type'\]"):
        asyncio.run(resolve_manifest(conn, manifest))


def test_edge_triple_plus_edge_uuid_is_ambiguous():
    import pytest

    conn = _mock_conn_with_series([])
    manifest = pl.DataFrame(
        {
            "edge_uuid": [str(uuid4())],
            "from_path": ["Grid/A"],
            "to_path": ["Grid/B"],
            "edge_type": ["Line"],
            "data_type": ["actual"],
            "name": ["flow"],
        }
    )

    with pytest.raises(ValueError, match="ambiguous routing columns"):
        asyncio.run(resolve_manifest(conn, manifest))


def test_edge_triple_unresolved_raises_with_quintuple_message():
    import pytest

    conn = _mock_conn_with_series([])  # PG matches nothing
    manifest = pl.DataFrame(
        {
            "from_path": ["Grid/A"],
            "to_path": ["Grid/NOPE"],
            "edge_type": ["Line"],
            "data_type": ["actual"],
            "name": ["flow"],
        }
    )

    with pytest.raises(ValueError, match="Series not registered for from_path='Grid/A', to_path='Grid/NOPE'"):
        asyncio.run(resolve_manifest(conn, manifest))
