"""Series-table operations: register, resolve for write, resolve for read.

All queries target ``energydb.series``. ``target_table`` is validated against
``timedb.TABLES`` at register time — energydb trusts the catalog, timedb owns it.
"""

from __future__ import annotations

from typing import Any

import polars as pl
import timedb


def _validate_target_table(target_table: str) -> None:
    if target_table not in timedb.TABLES:
        raise ValueError(f"Unknown target_table {target_table!r}. Valid tables: {sorted(timedb.TABLES)}")


def register_series(
    conn,
    *,
    node_id: int | None,
    edge_id: int | None,
    data_type: str,
    name: str,
    canonical_unit: str,
    target_table: str,
    description: str | None = None,
) -> int:
    """Insert a new series row; return its series_id.

    Owner is exactly one of node_id/edge_id (DB CHECK enforces).
    ``target_table``, ``canonical_unit``, and the owner are immutable after
    insert (DB trigger enforces).
    """
    if (node_id is None) == (edge_id is None):
        raise ValueError("Exactly one of node_id or edge_id must be set.")
    _validate_target_table(target_table)

    conflict_constraint = "series_node_uniq" if node_id is not None else "series_edge_uniq"
    row = conn.execute(
        f"""
        INSERT INTO energydb.series
            (node_id, edge_id, data_type, name, canonical_unit, target_table, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT {conflict_constraint} DO NOTHING
        RETURNING series_id
        """,
        (node_id, edge_id, data_type, name, canonical_unit, target_table, description),
    ).fetchone()

    if row is not None:
        return row[0]

    # Conflict: fetch the existing row and verify the immutable fields agree.
    owner_col = "node_id" if node_id is not None else "edge_id"
    owner_val = node_id if node_id is not None else edge_id
    existing = conn.execute(
        f"SELECT series_id, canonical_unit, target_table "
        f"FROM energydb.series "
        f"WHERE {owner_col} = %s AND data_type = %s AND name = %s",
        (owner_val, data_type, name),
    ).fetchone()
    if existing is None:
        raise RuntimeError("Insert conflict but no existing row found — concurrency bug")
    existing_sid, existing_unit, existing_table = existing
    if existing_unit != canonical_unit or existing_table != target_table:
        raise ValueError(
            f"Series ({owner_col}={owner_val}, data_type={data_type!r}, name={name!r}) "
            f"already exists with canonical_unit={existing_unit!r}, "
            f"target_table={existing_table!r}; cannot re-register with "
            f"canonical_unit={canonical_unit!r}, target_table={target_table!r}. "
            f"These fields are immutable — register a new series instead."
        )
    return existing_sid


def resolve_for_write(
    conn,
    *,
    node_id: int | None = None,
    edge_id: int | None = None,
    data_type: str,
    name: str,
) -> dict[str, Any]:
    """Resolve a single series for a write. Returns dict with series_id,
    canonical_unit, target_table. Raises if not found."""
    if (node_id is None) == (edge_id is None):
        raise ValueError("Exactly one of node_id or edge_id must be set.")
    owner_col = "node_id" if node_id is not None else "edge_id"
    owner_val = node_id if node_id is not None else edge_id
    row = conn.execute(
        f"SELECT series_id, canonical_unit, target_table "
        f"FROM energydb.series "
        f"WHERE {owner_col} = %s AND data_type = %s AND name = %s",
        (owner_val, data_type, name),
    ).fetchone()
    if row is None:
        raise ValueError(f"No series found for {owner_col}={owner_val}, data_type={data_type!r}, name={name!r}")
    return {
        "series_id": row[0],
        "canonical_unit": row[1],
        "target_table": row[2],
    }


def resolve_for_read(
    conn,
    *,
    node_ids: list[int] | None = None,
    edge_ids: list[int] | None = None,
    data_type: str | None = None,
    name: str | None = None,
) -> pl.DataFrame:
    """Bulk resolve series rows for a read. Returns Polars DataFrame with columns
    series_id, canonical_unit, target_table, node_id, edge_id, data_type, name.
    Empty df if nothing matches.
    """
    if (node_ids is None) == (edge_ids is None):
        raise ValueError("Exactly one of node_ids or edge_ids must be set.")

    owner_col = "node_id" if node_ids is not None else "edge_id"
    owner_vals = node_ids if node_ids is not None else edge_ids

    conditions = [f"{owner_col} = ANY(%s)"]
    params: list[Any] = [owner_vals]

    if data_type:
        conditions.append("data_type = %s")
        params.append(data_type)
    if name:
        conditions.append("name = %s")
        params.append(name)

    sql = (
        "SELECT series_id, canonical_unit, target_table, node_id, edge_id, "
        "data_type, name FROM energydb.series WHERE " + " AND ".join(conditions)
    )
    rows = conn.execute(sql, params).fetchall()

    return pl.DataFrame(
        {
            "series_id": [r[0] for r in rows],
            "canonical_unit": [r[1] for r in rows],
            "target_table": [r[2] for r in rows],
            "node_id": [r[3] for r in rows],
            "edge_id": [r[4] for r in rows],
            "data_type": [r[5] for r in rows],
            "name": [r[6] for r in rows],
        },
        schema={
            "series_id": pl.Int64,
            "canonical_unit": pl.Utf8,
            "target_table": pl.Utf8,
            "node_id": pl.Int64,
            "edge_id": pl.Int64,
            "data_type": pl.Utf8,
            "name": pl.Utf8,
        },
    )
