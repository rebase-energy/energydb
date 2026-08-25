"""Series-table operations: register, resolve for read.

The series table is owned by exactly one of ``node_uuid`` / ``edge_uuid``
(DB CHECK enforces). Internal APIs use a single ``(owner_col, owner_uuid)``
shape: callers always know which side they're on, so encoding "set one, leave
the other None" buys nothing.

``series_id BIGINT`` stays as the timedb-internal handle; it never leaves the
energydb / timedb pair.

Retention tier names are owned by timedb (see :data:`timedb.RETENTION_TIERS`).
energydb consumes the set as a runtime guard but does not encode the values
into its PG schema, so adding a tier in timedb does not require an energydb
migration.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

import polars as pl
from timedb import RETENTION_TIERS

from energydb.errors import AlreadyExistsError, EdgeNotFoundError, ValidationError
from energydb.models import SQL_SCHEMA_PREFIX as P
from energydb.paths import ambiguous_edge_error, edge_address_repr

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
        raise ValidationError(f"Unknown timeseries_type {ts_type!r}. Valid values: {sorted(_VALID_TIMESERIES_TYPES)}")


def _validate_retention(retention: str) -> None:
    if retention not in RETENTION_TIERS:
        raise ValidationError(f"Unknown retention {retention!r}. Valid values: {sorted(RETENTION_TIERS)}")


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
        raise ValidationError(f"{kind} name must be non-empty.")
    if "/" in name:
        raise ValidationError(
            f"{kind} name {name!r} contains '/'. '/' is reserved as the path "
            f"separator and may not appear inside a node, edge, or series name."
        )


# Column order of SERIES_INSERT_SQL params: shared by the single-row upsert
# below and the batched register_tree path in _persist.
SERIES_INSERT_COLUMNS = (
    "node_uuid",
    "edge_uuid",
    "data_type",
    "name",
    "canonical_unit",
    "timeseries_type",
    "retention",
    "description",
)


def prepare_series_row(
    *,
    owner_col: OwnerCol,
    owner_uuid: UUID,
    data_type: str,
    name: str,
    canonical_unit: str,
    timeseries_type: str,
    retention: str | None = None,
    description: str | None = None,
) -> tuple:
    """Validate + normalize one series declaration; return the INSERT param tuple.

    Pure (no I/O): validation and the shape-derived retention default
    (FLAT → ``"forever"``, OVERLAPPING → ``"medium"``) live here so the
    single-row upsert and the batched ``register_tree`` insert agree exactly.
    Param order is :data:`SERIES_INSERT_COLUMNS`.
    """
    _validate_timeseries_type(timeseries_type)
    validate_name(name, kind="series")
    if retention is None:
        retention = _DEFAULT_RETENTION_BY_SHAPE[timeseries_type]
    _validate_retention(retention)

    node_uuid = owner_uuid if owner_col == "node_uuid" else None
    edge_uuid = owner_uuid if owner_col == "edge_uuid" else None
    return (node_uuid, edge_uuid, data_type, name, canonical_unit, timeseries_type, retention, description)


async def register_series(
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
) -> int:
    """Insert a new series row owned by ``owner_uuid``; return its series_id.

    ``owner_col`` is one of ``"node_uuid"`` / ``"edge_uuid"``. The DB
    CHECK enforces that exactly one of those columns is non-null on each
    row. ``retention``, ``canonical_unit``, and the owner are immutable
    after insert (DB trigger enforces). ``timeseries_type`` is mutable.

    If ``retention`` is omitted, it is derived from ``timeseries_type``:
    FLAT (actuals) → ``"forever"``, OVERLAPPING (forecasts) → ``"medium"``.
    """
    params = prepare_series_row(
        owner_col=owner_col,
        owner_uuid=owner_uuid,
        data_type=data_type,
        name=name,
        canonical_unit=canonical_unit,
        timeseries_type=timeseries_type,
        retention=retention,
        description=description,
    )
    retention = params[SERIES_INSERT_COLUMNS.index("retention")]
    conflict_constraint = _CONFLICT_CONSTRAINT_BY_OWNER[owner_col]

    row = await (
        await conn.execute(
            f"""
            INSERT INTO {P}series
                (node_uuid, edge_uuid, data_type, name, canonical_unit,
                 timeseries_type, retention, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT {conflict_constraint} DO NOTHING
            RETURNING series_id
            """,
            params,
        )
    ).fetchone()

    if row is not None:
        return row[0]

    existing = await (
        await conn.execute(
            f"SELECT series_id, canonical_unit, retention "
            f"FROM {P}series "
            f"WHERE {owner_col} = %s AND data_type = %s AND name = %s",
            (owner_uuid, data_type, name),
        )
    ).fetchone()
    if existing is None:
        raise RuntimeError("Insert conflict but no existing row found: concurrency bug")
    existing_sid, existing_unit, existing_retention = existing
    if existing_unit != canonical_unit or existing_retention != retention:
        raise AlreadyExistsError(
            f"Series ({owner_col}={owner_uuid}, data_type={data_type!r}, name={name!r}) "
            f"already exists with canonical_unit={existing_unit!r}, "
            f"retention={existing_retention!r}; cannot re-register with "
            f"canonical_unit={canonical_unit!r}, retention={retention!r}. "
            f"These fields are immutable, register a new series instead."
        )
    return existing_sid


async def resolve_subtree_series_for_read(
    conn,
    *,
    root_path: str | None = None,
    start_uuid: UUID | None = None,
    rel_path: str | None = None,
    where_conds: list[str] | None = None,
    where_params: list[Any] | None = None,
    data_type: str | None = None,
    name: str | None = None,
) -> pl.DataFrame:
    """Resolve a node subtree straight to per-series read meta in ONE round-trip.

    A single query walks root → subtree (materialized-path prefix scan) →
    ``series`` ⋈ ``node``, replacing the former ``resolve_node_uuid`` +
    ``resolve_subtree_uuids`` + per-owner series scan chain on the node
    read path (which round-tripped the resolved uuid set back to PG as an
    ``ANY()`` param). The root is given either as ``root_path`` (a ``/``-joined
    path the caller already holds) or as ``start_uuid`` (+ optional ``rel_path``
    for relative navigation), whose path is derived inline via a CTE so the
    resolve stays one round-trip.

    ``where_conds`` / ``where_params`` are extra subtree-node predicates from
    :func:`build_filter_conditions` (qualified with ``table_alias="n"``).
    Returns one row per series with the per-series read-meta columns
    (``series_id``, ``canonical_unit``, ``timeseries_type``, ``retention``,
    ``node_uuid``, ``data_type``, ``name``, ``path``). Empty df if the subtree
    is empty or nothing matches.
    """
    cte_params: list[Any] = []
    if root_path is not None:
        root_cte = "SELECT %s::text AS rp"
        cte_params.append(root_path)
    elif start_uuid is not None and rel_path:
        root_cte = f"SELECT (path || '/' || %s) AS rp FROM {P}node WHERE uuid = %s"
        cte_params.extend([rel_path, start_uuid])
    elif start_uuid is not None:
        root_cte = f"SELECT path AS rp FROM {P}node WHERE uuid = %s"
        cte_params.append(start_uuid)
    else:
        raise ValidationError("resolve_subtree_series_for_read needs root_path or start_uuid.")

    where_parts = list(where_conds or [])
    where_vals: list[Any] = list(where_params or [])
    if data_type:
        where_parts.append("s.data_type = %s")
        where_vals.append(data_type)
    if name:
        where_parts.append("s.name = %s")
        where_vals.append(name)
    where_clause = (" AND " + " AND ".join(where_parts)) if where_parts else ""

    # The prefix can be DB-derived, so it cannot always be escaped Python-side;
    # escaping LIKE metacharacters in SQL keeps this to one round-trip.
    sql = rf"""
        WITH root AS ({root_cte})
        SELECT s.series_id, s.canonical_unit, s.timeseries_type, s.retention,
               s.node_uuid::text AS node_uuid, s.data_type, s.name, n.path AS path
        FROM root r
        JOIN {P}node n
          ON (n.path = r.rp
              OR n.path LIKE
                 replace(replace(replace(r.rp, E'\\', E'\\\\'), '%%', E'\\%%'), '_', E'\\_')
                 || '/%%' ESCAPE '\')
        JOIN {P}series s ON s.node_uuid = n.uuid
        WHERE TRUE{where_clause}
    """
    rows = await (await conn.execute(sql, [*cte_params, *where_vals])).fetchall()

    return pl.DataFrame(
        [
            {
                "series_id": r[0],
                "canonical_unit": r[1],
                "timeseries_type": r[2],
                "retention": r[3],
                "node_uuid": r[4],
                "data_type": r[5],
                "name": r[6],
                "path": r[7],
            }
            for r in rows
        ],
        schema={
            "series_id": pl.Int64,
            "canonical_unit": pl.Utf8,
            "timeseries_type": pl.Utf8,
            "retention": pl.Utf8,
            "node_uuid": pl.Utf8,
            "data_type": pl.Utf8,
            "name": pl.Utf8,
            "path": pl.Utf8,
        },
    )


async def resolve_edge_series_for_read(
    conn,
    *,
    edge_uuid: UUID | None = None,
    from_path: str | None = None,
    to_path: str | None = None,
    edge_type: str | None = None,
    edge_name: str | None = None,
    data_type: str | None = None,
    name: str | None = None,
) -> pl.DataFrame:
    """Resolve an edge's series for a read in ONE round-trip.

    The edge is addressed by ``edge_uuid`` OR by its
    ``(from_path, to_path, edge_type)`` triple, optionally narrowed by
    ``edge_name``; the triple form collapses the former three-step chain
    (paths → uuids, edge lookup, series scan) into a single query. The
    endpoint paths, ``edge_type`` and ``edge_name`` ride along on the join, so
    the post-read attach step needs no further PG calls.

    ``name`` / ``data_type`` filter the *series*; ``edge_name`` identifies the
    *edge*, the two names are deliberately separate.

    ``series`` is LEFT-JOINed so an existing edge with no (matching) series
    still returns a row, distinguishing "edge not found" from "no series":

    * triple-addressed + edge missing → raises
      :class:`~energydb.errors.EdgeNotFoundError` (the same contract as a
      standalone ``resolve_edge_uuid`` lookup);
    * triple-addressed + several parallel edges matched → raises
      :class:`~energydb.errors.AmbiguousEdgeError` (pass ``edge_name``);
    * uuid-addressed + edge missing → empty df, by contract;
    * edge exists, nothing matches → empty df.
    """
    join_conds = ["s.edge_uuid = e.uuid"]
    join_params: list[Any] = []
    if data_type:
        join_conds.append("s.data_type = %s")
        join_params.append(data_type)
    if name:
        join_conds.append("s.name = %s")
        join_params.append(name)

    if edge_uuid is not None:
        where_sql = "e.uuid = %s"
        where_params: list[Any] = [edge_uuid]
    elif from_path is not None and to_path is not None and edge_type is not None:
        where_sql = "fn.path = %s AND tn.path = %s AND e.edge_type = %s"
        where_params = [from_path, to_path, edge_type]
        if edge_name is not None:
            where_sql += " AND e.name = %s"
            where_params.append(edge_name)
    else:
        raise ValidationError("resolve_edge_series_for_read needs edge_uuid or (from_path, to_path, edge_type).")

    # ::text skips psycopg's per-row UUID parse. The edge uuid comes from e, not
    # s, so it is populated on the LEFT-JOIN row of a series-less edge, which is
    # the row the ambiguity check counts.
    sql = (
        "SELECT s.series_id, s.canonical_unit, s.timeseries_type, s.retention, "
        "e.uuid::text, s.data_type, s.name, "
        "e.edge_type AS edge_type, e.name AS edge_name, fn.path AS from_path, tn.path AS to_path "
        f"FROM {P}edge e "
        f"JOIN {P}node fn ON fn.uuid = e.from_node_uuid "
        f"JOIN {P}node tn ON tn.uuid = e.to_node_uuid "
        f"LEFT JOIN {P}series s ON " + " AND ".join(join_conds) + " "
        "WHERE " + where_sql
    )
    rows = await (await conn.execute(sql, [*join_params, *where_params])).fetchall()

    if edge_uuid is None:
        assert from_path is not None and to_path is not None and edge_type is not None
        if not rows:
            raise EdgeNotFoundError(
                f"Edge not found: {edge_address_repr(from_path, to_path, edge_type, edge_name)}",
                from_path=from_path,
                to_path=to_path,
                edge_type=edge_type,
                name=edge_name,
            )
        # One row per (edge, matching series): several edges is ambiguous,
        # several series on one edge is ordinary.
        matched = {r[4]: r[8] for r in rows}
        if len(matched) > 1:
            raise ambiguous_edge_error(
                from_path=from_path,
                to_path=to_path,
                edge_type=edge_type,
                matches=list(matched.items()),
                fix="pass name= to address one of them",
            )

    return pl.DataFrame(
        [
            {
                "series_id": r[0],
                "canonical_unit": r[1],
                "timeseries_type": r[2],
                "retention": r[3],
                "edge_uuid": r[4],
                "data_type": r[5],
                "name": r[6],
                "edge_type": r[7],
                "edge_name": r[8],
                "from_path": r[9],
                "to_path": r[10],
            }
            for r in rows
            if r[0] is not None  # LEFT-JOIN row for a series-less edge
        ],
        schema={
            "series_id": pl.Int64,
            "canonical_unit": pl.Utf8,
            "timeseries_type": pl.Utf8,
            "retention": pl.Utf8,
            "edge_uuid": pl.Utf8,
            "data_type": pl.Utf8,
            "name": pl.Utf8,
            "edge_type": pl.Utf8,
            "edge_name": pl.Utf8,
            "from_path": pl.Utf8,
            "to_path": pl.Utf8,
        },
    )
