"""Regression tests for ``paths.resolve_manifest``.

In particular: the join against the series-lookup frame must not leak
internal ``_dt`` / ``_name`` columns into the resolved frame. Today
polars' join semantics consume those columns even though the existing
``.drop(...)`` call uses the wrong frame to check for them — this test
pins down the contract so a future polars upgrade can't silently
regress it.
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
    resolved = resolve_manifest(conn, manifest)

    assert "_dt" not in resolved.columns
    assert "_name" not in resolved.columns
    assert "series_id" in resolved.columns
    assert "canonical_unit" in resolved.columns


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
    resolved = resolve_manifest(conn, manifest)

    assert "_dt" not in resolved.columns
    assert "_name" not in resolved.columns
    assert "series_id" in resolved.columns
    assert "canonical_unit" in resolved.columns
