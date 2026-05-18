"""Series-table operations: register, resolve for read.

The series table is owned by exactly one of ``node_uuid`` / ``edge_uuid``
(DB CHECK enforces). Internal APIs use a single ``(owner_col, owner_uuid)``
shape — callers always know which side they're on, so encoding "set one,
leave the other None" buys nothing.

``series_id BIGINT`` stays as the timedb-internal handle — it never leaves
the energydb / timedb pair.

Retention tier names are owned by timedb (see :data:`timedb.RETENTION_TIERS`).
energydb consumes the set as a runtime guard but does not encode the values
into its PG schema — adding a tier in timedb does not require an energydb
migration.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

import polars as pl
from timedb import RETENTION_TIERS

from energydb._resolve_cache import SeriesMeta, SeriesRegistry

OwnerCol = Literal["node_uuid", "edge_uuid"]

_VALID_TIMESERIES_TYPES = {"FLAT", "OVERLAPPING"}

# Defaults are picked by series shape: actuals (FLAT) should never expire,
# forecasts (OVERLAPPING) age out under the medium tier.
_DEFAULT_RETENTION_BY_SHAPE = {
    "FLAT": "forever",
    "OVERLAPPING": "medium",
}

_CONFLICT_CONSTRAINT_BY_OWNER = {
    "node_uuid": "series_node_uniq",
    "edge_uuid": "series_edge_uniq",
}


def _validate_timeseries_type(ts_type: str) -> None:
    if ts_type not in _VALID_TIMESERIES_TYPES:
        raise ValueError(f"Unknown timeseries_type {ts_type!r}. Valid values: {sorted(_VALID_TIMESERIES_TYPES)}")


def _validate_retention(retention: str) -> None:
    if retention not in RETENTION_TIERS:
        raise ValueError(f"Unknown retention {retention!r}. Valid values: {sorted(RETENTION_TIERS)}")


def validate_name(name: str, *, kind: str) -> None:
    """Reject empty names or names containing ``/``.

    ``/`` is the path separator on the read API (``path: pl.Utf8`` joined
    with ``/``), so allowing it inside a node/edge/series name would make
    the joined path ambiguous. The same constraint is enforced at the PG
    level by ``CheckConstraint("name !~ '/' AND length(name) > 0")``.
    """
    if not isinstance(name, str):
        raise TypeError(f"{kind} name must be a string, got {type(name).__name__}")
    if len(name) == 0:
        raise ValueError(f"{kind} name must be non-empty.")
    if "/" in name:
        raise ValueError(
            f"{kind} name {name!r} contains '/'. '/' is reserved as the path "
            f"separator and may not appear inside a node, edge, or series name."
        )


def register_series(
    conn,
    *,
    owner_col: OwnerCol,
    owner_uuid: UUID,
    data_type: str,
    name: str,
    canonical_unit: str,
    timeseries_type: str,
    retention: str | None = None,
    description: str | None = None,
    registry: SeriesRegistry | None = None,
) -> int:
    """Insert a new series row owned by ``owner_uuid``; return its series_id.

    ``owner_col`` is one of ``"node_uuid"`` / ``"edge_uuid"``. The DB
    CHECK enforces that exactly one of those columns is non-null on each
    row. ``retention``, ``canonical_unit``, and the owner are immutable
    after insert (DB trigger enforces). ``timeseries_type`` is mutable.

    If ``retention`` is omitted, it is derived from ``timeseries_type``:
    FLAT (actuals) → ``"forever"``, OVERLAPPING (forecasts) → ``"medium"``.

    When ``registry`` is provided, the resolved metadata is inserted into
    the cache (write-through). On a conflict-with-existing-row the cached
    entry reflects the row already in the DB, not the rejected new args.
    """
    _validate_timeseries_type(timeseries_type)
    validate_name(name, kind="series")
    if retention is None:
        retention = _DEFAULT_RETENTION_BY_SHAPE[timeseries_type]
    _validate_retention(retention)

    # Populate the right column based on owner_col; the other stays NULL.
    node_uuid = owner_uuid if owner_col == "node_uuid" else None
    edge_uuid = owner_uuid if owner_col == "edge_uuid" else None
    conflict_constraint = _CONFLICT_CONSTRAINT_BY_OWNER[owner_col]

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
        sid = row[0]
        if registry is not None:
            registry.insert(
                str(owner_uuid),
                data_type,
                name,
                SeriesMeta(
                    series_id=sid,
                    canonical_unit=canonical_unit,
                    timeseries_type=timeseries_type,
                    retention=retention,
                ),
            )
        return sid

    # Conflict: fetch the existing row and verify the immutable fields agree.
    existing = conn.execute(
        f"SELECT series_id, canonical_unit, retention, timeseries_type "
        f"FROM energydb.series "
        f"WHERE {owner_col} = %s AND data_type = %s AND name = %s",
        (owner_uuid, data_type, name),
    ).fetchone()
    if existing is None:
        raise RuntimeError("Insert conflict but no existing row found — concurrency bug")
    existing_sid, existing_unit, existing_retention, existing_ts_type = existing
    if existing_unit != canonical_unit or existing_retention != retention:
        raise ValueError(
            f"Series ({owner_col}={owner_uuid}, data_type={data_type!r}, name={name!r}) "
            f"already exists with canonical_unit={existing_unit!r}, "
            f"retention={existing_retention!r}; cannot re-register with "
            f"canonical_unit={canonical_unit!r}, retention={retention!r}. "
            f"These fields are immutable — register a new series instead."
        )
    if registry is not None:
        registry.insert(
            str(owner_uuid),
            data_type,
            name,
            SeriesMeta(
                series_id=existing_sid,
                canonical_unit=existing_unit,
                timeseries_type=existing_ts_type,
                retention=existing_retention,
            ),
        )
    return existing_sid


def resolve_for_read(
    conn,
    *,
    owner_col: OwnerCol,
    owner_uuids: list[UUID],
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
    conditions = [f"{owner_col} = ANY(%s)"]
    params: list[Any] = [owner_uuids]

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
