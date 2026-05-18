"""Regression tests for ``paths.resolve_manifest``.

The original join-based implementation could leak internal ``_dt`` /
``_name`` columns into the resolved frame; the current hash-prejoin
implementation can't, but the no-leak contract is still pinned here.
Also covers the ``(resolved, summary)`` return shape and per-row column
contract (``series_id`` / ``retention`` / ``canonical_unit`` only —
``timeseries_type`` lives in the summary now).
"""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import polars as pl
from energydb.paths import resolve_manifest


def _mock_conn_with_series(rows: list[tuple]) -> MagicMock:
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = rows
    return conn


def test_node_manifest_resolve_does_not_leak_internal_join_columns():
    node_uuid = uuid4()
    rows = [
        (
            node_uuid,  # node_uuid
            "actual",  # data_type
            "power",  # name
            42,  # series_id
            "MW",  # canonical_unit
            "FLAT",  # timeseries_type
            "forever",  # retention
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
    resolved, summary = resolve_manifest(conn, manifest)

    assert "_dt" not in resolved.columns
    assert "_name" not in resolved.columns
    assert "_triple_k" not in resolved.columns
    assert "series_id" in resolved.columns
    assert "canonical_unit" in resolved.columns
    assert "retention" in resolved.columns
    # timeseries_type now lives in the summary, not per-row.
    assert "timeseries_type" not in resolved.columns
    assert summary.has_overlapping is False


def test_edge_manifest_resolve_does_not_leak_internal_join_columns():
    edge_uuid = uuid4()
    rows = [
        (
            edge_uuid,
            "actual",
            "power",
            7,
            "MW",
            "FLAT",
            "forever",
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
    resolved, summary = resolve_manifest(conn, manifest)

    assert "_dt" not in resolved.columns
    assert "_name" not in resolved.columns
    assert "_triple_k" not in resolved.columns
    assert "series_id" in resolved.columns
    assert "canonical_unit" in resolved.columns
    assert "retention" in resolved.columns
    assert "timeseries_type" not in resolved.columns
    assert summary.has_overlapping is False


def test_overlapping_surfaces_in_summary():
    """Set-level OVERLAPPING signal lives on the summary, not per-row."""
    node_uuid = uuid4()
    rows = [
        (node_uuid, "forecast", "v1", 99, "MW", "OVERLAPPING", "medium"),
    ]
    conn = _mock_conn_with_series(rows)

    manifest = pl.DataFrame(
        {
            "node_uuid": [str(node_uuid)] * 3,
            "data_type": ["forecast"] * 3,
            "name": ["v1"] * 3,
        }
    )
    resolved, summary = resolve_manifest(conn, manifest)

    assert summary.has_overlapping is True
    # series_id attached to every row.
    assert resolved["series_id"].to_list() == [99, 99, 99]


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
        resolve_manifest(conn, manifest)
