"""Series-table operations: register, resolve for write, resolve for read.

The series table is owned by exactly one of ``node_uuid`` / ``edge_uuid``
(DB CHECK enforces). ``series_id BIGINT`` stays as the timedb-internal
handle — it never leaves the energydb / timedb pair.

Retention tier names are owned by timedb (see :data:`timedb.RETENTION_TIERS`).
energydb consumes the set as a runtime guard but does not encode the values
into its PG schema — adding a tier in timedb does not require an energydb
migration.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import polars as pl
from timedb import RETENTION_TIERS

_VALID_TIMESERIES_TYPES = {"FLAT", "OVERLAPPING"}

# Defaults are picked by series shape: actuals (FLAT) should never expire,
# forecasts (OVERLAPPING) age out under the medium tier.
_DEFAULT_RETENTION_BY_SHAPE = {
    "FLAT": "forever",
    "OVERLAPPING": "medium",
}


def _validate_timeseries_type(ts_type: str) -> None:
    if ts_type not in _VALID_TIMESERIES_TYPES:
        raise ValueError(f"Unknown timeseries_type {ts_type!r}. Valid values: {sorted(_VALID_TIMESERIES_TYPES)}")


def _validate_retention(retention: str) -> None:
    if retention not in RETENTION_TIERS:
        raise ValueError(f"Unknown retention {retention!r}. Valid values: {sorted(RETENTION_TIERS)}")


def register_series(
    conn,
    *,
    node_uuid: UUID | None,
    edge_uuid: UUID | None,
    data_type: str,
    name: str,
    canonical_unit: str,
    timeseries_type: str,
    retention: str | None = None,
    description: str | None = None,
) -> int:
    """Insert a new series row; return its series_id.

    Owner is exactly one of node_uuid/edge_uuid (DB CHECK enforces).
    ``retention``, ``canonical_unit``, and the owner are immutable after
    insert (DB trigger enforces). ``timeseries_type`` is mutable.

    If ``retention`` is omitted, it is derived from ``timeseries_type``:
    FLAT (actuals) → ``"forever"``, OVERLAPPING (forecasts) → ``"medium"``.
    """
    if (node_uuid is None) == (edge_uuid is None):
        raise ValueError("Exactly one of node_uuid or edge_uuid must be set.")
    _validate_timeseries_type(timeseries_type)
    if retention is None:
        retention = _DEFAULT_RETENTION_BY_SHAPE[timeseries_type]
    _validate_retention(retention)

    conflict_constraint = "series_node_uniq" if node_uuid is not None else "series_edge_uniq"
    row = conn.execute(
        f"""
        INSERT INTO energydb.series
            (node_uuid, edge_uuid, data_type, name, canonical_unit,
             timeseries_type, retention, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT {conflict_constraint} DO NOTHING
        RETURNING series_id
        """,
        (node_uuid, edge_uuid, data_type, name, canonical_unit, timeseries_type, retention, description),
    ).fetchone()

    if row is not None:
        return row[0]

    # Conflict: fetch the existing row and verify the immutable fields agree.
    owner_col = "node_uuid" if node_uuid is not None else "edge_uuid"
    owner_val = node_uuid if node_uuid is not None else edge_uuid
    existing = conn.execute(
        f"SELECT series_id, canonical_unit, retention "
        f"FROM energydb.series "
        f"WHERE {owner_col} = %s AND data_type = %s AND name = %s",
        (owner_val, data_type, name),
    ).fetchone()
    if existing is None:
        raise RuntimeError("Insert conflict but no existing row found — concurrency bug")
    existing_sid, existing_unit, existing_retention = existing
    if existing_unit != canonical_unit or existing_retention != retention:
        raise ValueError(
            f"Series ({owner_col}={owner_val}, data_type={data_type!r}, name={name!r}) "
            f"already exists with canonical_unit={existing_unit!r}, "
            f"retention={existing_retention!r}; cannot re-register with "
            f"canonical_unit={canonical_unit!r}, retention={retention!r}. "
            f"These fields are immutable — register a new series instead."
        )
    return existing_sid


def resolve_for_read(
    conn,
    *,
    node_uuids: list[UUID] | None = None,
    edge_uuids: list[UUID] | None = None,
    data_type: str | None = None,
    name: str | None = None,
) -> pl.DataFrame:
    """Bulk resolve series rows for a read.

    Returns Polars DataFrame with columns series_id, canonical_unit,
    timeseries_type, retention, node_uuid, edge_uuid, data_type, name.
    UUIDs are returned as ``Utf8`` so they join cleanly against the
    string-form ids the manifest pipeline carries. Empty df if nothing
    matches.
    """
    if (node_uuids is None) == (edge_uuids is None):
        raise ValueError("Exactly one of node_uuids or edge_uuids must be set.")

    owner_col = "node_uuid" if node_uuids is not None else "edge_uuid"
    owner_vals = node_uuids if node_uuids is not None else edge_uuids

    conditions = [f"{owner_col} = ANY(%s)"]
    params: list[Any] = [owner_vals]

    if data_type:
        conditions.append("data_type = %s")
        params.append(data_type)
    if name:
        conditions.append("name = %s")
        params.append(name)

    sql = (
        "SELECT series_id, canonical_unit, timeseries_type, retention, "
        "node_uuid, edge_uuid, data_type, name "
        "FROM energydb.series WHERE " + " AND ".join(conditions)
    )
    rows = conn.execute(sql, params).fetchall()

    return pl.DataFrame(
        [
            {
                "series_id": r[0],
                "canonical_unit": r[1],
                "timeseries_type": r[2],
                "retention": r[3],
                "node_uuid": str(r[4]) if r[4] is not None else None,
                "edge_uuid": str(r[5]) if r[5] is not None else None,
                "data_type": r[6],
                "name": r[7],
            }
            for r in rows
        ],
        schema={
            "series_id": pl.Int64,
            "canonical_unit": pl.Utf8,
            "timeseries_type": pl.Utf8,
            "retention": pl.Utf8,
            "node_uuid": pl.Utf8,
            "edge_uuid": pl.Utf8,
            "data_type": pl.Utf8,
            "name": pl.Utf8,
        },
    )
