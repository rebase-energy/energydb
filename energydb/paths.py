"""Path-based addressing primitives for the fluent CLI.

After the UUID identity rewrite, ``node.uuid`` is the row PK and the value
held by every :class:`energydatamodel.Reference`. Paths are still a
user-friendly addressing form though — the fluent CLI lets you write
``client.get_node("Europe", "Sweden", "Lillgrund")`` and resolve the path
chain to a UUID with one indexed recursive CTE on ``(parent_uuid, name)``.

A node is identified at the storage layer by its ``uuid``; an edge by its
``uuid`` (or by the ``(from_node_uuid, to_node_uuid, edge_type)`` triple,
which is the upsert key on :class:`energydb.models.Edge`).
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import polars as pl

from energydb._resolve_cache import EdgeMeta, NodeMeta, SeriesMeta, SeriesRegistry

# A path is a tuple of names from the tree root.
Path = tuple[str, ...]


# ---------------------------------------------------------------------------
# Node resolution: path -> uuid
# ---------------------------------------------------------------------------


# Shared recursive tail for ``resolve_node_uuid`` — the walk and final
# SELECT are identical in both seed cases; only the seed row differs.
_RESOLVE_NODE_UUID_TAIL = """
        UNION ALL
        SELECT n.uuid, w.depth + 1
        FROM walk w
        JOIN energydb.node n
             ON n.parent_uuid = w.uuid
            AND n.name = (%s::text[])[w.depth + 1]
        WHERE w.depth < array_length(%s::text[], 1)
    )
    SELECT uuid FROM walk WHERE depth = array_length(%s::text[], 1)
"""


def resolve_node_uuid(conn, path: Path, *, start_uuid: UUID | None = None) -> UUID:
    """Resolve a path tuple like ``("Europe", "Sweden", "Lillgrund")`` to a uuid.

    One recursive CTE walking down the chain — single round-trip regardless
    of depth. With ``UNIQUE (parent_uuid, name)`` and the partial unique
    index on roots, the path either resolves to exactly one row or raises.

    When ``start_uuid`` is given the walk begins as a child of that node
    rather than at the root; this supports
    ``client.get_node(uuid=...).get_node("Child")``-style relative navigation.
    """
    if not path:
        if start_uuid is not None:
            return start_uuid
        raise ValueError("Empty path cannot be resolved.")

    p = list(path)
    if start_uuid is None:
        seed_sql = "SELECT uuid, 1 AS depth FROM energydb.node WHERE name = (%s::text[])[1] AND parent_uuid IS NULL"
        seed_params: tuple = (p,)
    else:
        seed_sql = "SELECT %s::uuid AS uuid, 0 AS depth"
        seed_params = (start_uuid,)

    sql = f"WITH RECURSIVE walk AS (\n        {seed_sql}{_RESOLVE_NODE_UUID_TAIL}"
    rows = conn.execute(sql, (*seed_params, p, p, p)).fetchall()

    if len(rows) == 0:
        raise ValueError(f"Node not found: {'/'.join(path)}")
    if len(rows) > 1:
        # Defensive: schema constraints should make this impossible.
        ids = [r[0] for r in rows]
        raise RuntimeError(f"Path {path!r} resolved to multiple uuids {ids}")
    return rows[0][0]


def resolve_paths_to_uuids(conn, paths: list[Path]) -> dict[Path, UUID]:
    """Bulk-resolve a list of path tuples to uuids in one round-trip.

    Input may contain duplicates; the returned dict is keyed by deduplicated
    path tuples. Raises ``ValueError`` if any path fails to resolve.
    """
    if not paths:
        return {}

    unique: list[Path] = []
    seen: set[Path] = set()
    for p in paths:
        t = tuple(p)
        if t not in seen:
            seen.add(t)
            unique.append(t)

    payload = json.dumps([list(p) for p in unique])

    rows = conn.execute(
        """
        WITH RECURSIVE inputs AS (
            SELECT
                idx::int AS idx,
                ARRAY(SELECT jsonb_array_elements_text(p)) AS path
            FROM jsonb_array_elements(%s::jsonb) WITH ORDINALITY AS t(p, idx)
        ),
        walk AS (
            SELECT i.idx, i.path, n.uuid, 1 AS depth
            FROM inputs i
            JOIN energydb.node n
                ON n.name = i.path[1] AND n.parent_uuid IS NULL
            WHERE array_length(i.path, 1) > 0
            UNION ALL
            SELECT w.idx, w.path, n.uuid, w.depth + 1
            FROM walk w
            JOIN energydb.node n
                ON n.parent_uuid = w.uuid
               AND n.name = w.path[w.depth + 1]
            WHERE w.depth < array_length(w.path, 1)
        )
        SELECT idx, uuid FROM walk WHERE depth = array_length(path, 1)
        """,
        (payload,),
    ).fetchall()

    out: dict[Path, UUID] = {}
    for idx, node_uuid in rows:
        out[unique[idx - 1]] = node_uuid

    missing = [p for p in unique if p not in out]
    if missing:
        rendered = ", ".join("/".join(p) for p in missing)
        raise ValueError(f"Could not resolve path(s): {rendered}")
    return out


# ---------------------------------------------------------------------------
# Node filter predicates (shared between Client.query_nodes and
# NodeScope._resolve_target_node_uuids)
# ---------------------------------------------------------------------------


def build_filter_conditions(
    where_filters: dict[str, Any],
    *,
    type_col: str,
    table_alias: str = "",
) -> tuple[list[str], list[Any]]:
    """Translate ``where_filters`` into SQL fragments + bind params.

    Recognized structural keys: ``type_col`` (typically ``"node_type"`` or
    ``"edge_type"``) and ``"name"`` — both compared against their own
    columns. Everything else is treated as a JSONB ``data->>`` predicate.
    Fragments must be joined with ``AND`` by the caller. ``table_alias``
    (e.g. ``"n"``) is prefixed to each column reference when the filter
    is composed against an aliased table in a join.
    """
    prefix = f"{table_alias}." if table_alias else ""
    conditions: list[str] = []
    params: list[Any] = []
    if type_col in where_filters:
        conditions.append(f"{prefix}{type_col} = %s")
        params.append(where_filters[type_col])
    if "name" in where_filters:
        conditions.append(f"{prefix}name = %s")
        params.append(where_filters["name"])
    for key, value in where_filters.items():
        if key in (type_col, "name"):
            continue
        conditions.append(f"{prefix}data->>%s = %s")
        params.append(key)
        params.append(str(value))
    return conditions, params


# ---------------------------------------------------------------------------
# Subtree
# ---------------------------------------------------------------------------


def resolve_subtree_uuids(conn, node_uuid: UUID) -> list[UUID]:
    rows = conn.execute(
        """
        WITH RECURSIVE subtree AS (
            SELECT uuid FROM energydb.node WHERE uuid = %s
            UNION ALL
            SELECT n.uuid FROM energydb.node n
            JOIN subtree s ON n.parent_uuid = s.uuid
        ) CYCLE uuid SET _is_cycle USING _cycle_path
        SELECT uuid FROM subtree WHERE NOT _is_cycle
        """,
        (node_uuid,),
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# uuid -> path
# ---------------------------------------------------------------------------


def resolve_path(conn, node_uuid: UUID) -> Path:
    """Return the full path tuple from root → node."""
    rows = conn.execute(
        """
        WITH RECURSIVE ancestors AS (
            SELECT uuid, name, parent_uuid, 0 AS depth
            FROM energydb.node WHERE uuid = %s
            UNION ALL
            SELECT n.uuid, n.name, n.parent_uuid, a.depth + 1
            FROM energydb.node n
            JOIN ancestors a ON n.uuid = a.parent_uuid
        ) CYCLE uuid SET _is_cycle USING _cycle_path
        SELECT name FROM ancestors WHERE NOT _is_cycle ORDER BY depth DESC
        """,
        (node_uuid,),
    ).fetchall()
    return tuple(r[0] for r in rows)


def fetch_node_hierarchy_bulk(
    conn,
    node_uuid_strs: list[str],
) -> dict[str, tuple[NodeMeta, str | None]]:
    """Bulk: ``uuid → (NodeMeta, parent_uuid_str | None)`` in one round-trip.

    Returns ``name``, ``node_type``, ``parent_uuid`` and the root→leaf
    ``path`` for each requested uuid. Used as the cold-miss fetch behind
    :func:`energydb._join.join_hierarchy`; one recursive CTE replaces the
    earlier flat-SELECT + :func:`resolve_paths_bulk` round-trip pair.

    Input uuids are passed as strings (the form the manifest carries) and
    cast to ``uuid[]`` on the PG side.
    """
    if not node_uuid_strs:
        return {}
    rows = conn.execute(
        """
        WITH RECURSIVE ancestors AS (
            SELECT uuid AS target_uuid, uuid, name, node_type, parent_uuid, 0 AS depth
            FROM energydb.node WHERE uuid = ANY(%s::uuid[])
            UNION ALL
            SELECT a.target_uuid, n.uuid, n.name, n.node_type, n.parent_uuid, a.depth + 1
            FROM energydb.node n
            JOIN ancestors a ON n.uuid = a.parent_uuid
        ) CYCLE uuid SET _is_cycle USING _cycle_path
        SELECT target_uuid, name, node_type, parent_uuid, depth FROM ancestors
        WHERE NOT _is_cycle
        ORDER BY target_uuid, depth DESC
        """,
        (node_uuid_strs,),
    ).fetchall()

    # depth=0 is the requested node itself; higher depths are ancestors,
    # iterated DESC so the leaf row arrives last with the correct
    # parent_uuid for the requested uuid.
    parts: dict[str, list[str]] = {}
    leaves: dict[str, tuple[str, str, str | None]] = {}
    for target_uuid, name, node_type, parent_uuid, depth in rows:
        key = str(target_uuid)
        parts.setdefault(key, []).append(name)
        if depth == 0:
            leaves[key] = (name, node_type, str(parent_uuid) if parent_uuid is not None else None)

    out: dict[str, tuple[NodeMeta, str | None]] = {}
    for uid, (name, node_type, parent_uuid) in leaves.items():
        out[uid] = (NodeMeta(path=tuple(parts[uid]), name=name, node_type=node_type), parent_uuid)
    return out


def fetch_edge_hierarchy_bulk(conn, edge_uuid_strs: list[str]) -> dict[str, EdgeMeta]:
    """Bulk: ``uuid → EdgeMeta`` for many edges in one round-trip.

    Endpoint *paths* aren't fetched here — the caller resolves them via the
    node cache on ``from_node_uuid`` / ``to_node_uuid``.
    """
    if not edge_uuid_strs:
        return {}
    rows = conn.execute(
        "SELECT uuid, name, edge_type, from_node_uuid, to_node_uuid FROM energydb.edge WHERE uuid = ANY(%s::uuid[])",
        (edge_uuid_strs,),
    ).fetchall()
    return {
        str(uuid_): EdgeMeta(
            name=name,
            edge_type=edge_type,
            from_node_uuid=str(from_uuid),
            to_node_uuid=str(to_uuid),
        )
        for uuid_, name, edge_type, from_uuid, to_uuid in rows
    }


def resolve_paths_bulk(conn, node_uuids: list[UUID]) -> dict[UUID, Path]:
    """Bulk: ``uuid → path tuple`` for many nodes in one CTE."""
    if not node_uuids:
        return {}
    rows = conn.execute(
        """
        WITH RECURSIVE ancestors AS (
            SELECT uuid AS target_uuid, uuid, name, parent_uuid, 0 AS depth
            FROM energydb.node WHERE uuid = ANY(%s)
            UNION ALL
            SELECT a.target_uuid, n.uuid, n.name, n.parent_uuid, a.depth + 1
            FROM energydb.node n
            JOIN ancestors a ON n.uuid = a.parent_uuid
        ) CYCLE uuid SET _is_cycle USING _cycle_path
        SELECT target_uuid, name, depth FROM ancestors
        WHERE NOT _is_cycle
        ORDER BY target_uuid, depth DESC
        """,
        (node_uuids,),
    ).fetchall()

    parts: dict[UUID, list[str]] = {}
    for target_uuid, name, _depth in rows:
        parts.setdefault(target_uuid, []).append(name)
    return {nid: tuple(p) for nid, p in parts.items()}


# ---------------------------------------------------------------------------
# Edge resolution: (from_path, to_path, edge_type) -> uuid
# ---------------------------------------------------------------------------


def resolve_edge_uuid(conn, from_path: Path, to_path: Path, edge_type: str) -> UUID:
    """Resolve an edge by its triple identity to its uuid."""
    paths = resolve_paths_to_uuids(conn, [from_path, to_path])
    from_uuid = paths[tuple(from_path)]
    to_uuid = paths[tuple(to_path)]
    rows = conn.execute(
        "SELECT uuid FROM energydb.edge WHERE edge_type = %s AND from_node_uuid = %s AND to_node_uuid = %s",
        (edge_type, from_uuid, to_uuid),
    ).fetchall()
    if not rows:
        raise ValueError(f"Edge not found: type={edge_type!r} from={'/'.join(from_path)!r} to={'/'.join(to_path)!r}")
    return rows[0][0]


# ---------------------------------------------------------------------------
# Manifest resolution
# ---------------------------------------------------------------------------


_MANIFEST_REQUIRED = ("data_type", "name")
_MANIFEST_ROUTES = ("node_uuid", "path", "edge_uuid")


def resolve_manifest(
    conn,
    manifest: pl.DataFrame,
    *,
    registry: SeriesRegistry | None = None,
) -> pl.DataFrame:
    """Resolve a routing manifest to series metadata.

    Detects routing mode from the columns present:

    * ``node_uuid`` — direct lookup against ``energydb.series.node_uuid``.
    * ``path``      — ``List(Utf8)`` path; resolved to ``node_uuid`` first.
    * ``edge_uuid`` — direct lookup against ``energydb.series.edge_uuid``.

    The manifest must also carry ``data_type`` and ``name`` columns. Returns
    the original frame plus ``series_id``, ``retention``, ``canonical_unit``,
    and ``timeseries_type``.

    When ``registry`` is provided, cached entries are served without a PG
    round-trip and freshly-fetched entries are inserted into the cache.
    """
    present_routes = [c for c in _MANIFEST_ROUTES if c in manifest.columns]
    if len(present_routes) == 0:
        raise ValueError(f"Manifest must include one of {list(_MANIFEST_ROUTES)} as a routing column.")
    if len(present_routes) > 1:
        raise ValueError(f"Manifest has ambiguous routing columns {present_routes}; provide exactly one.")
    route = present_routes[0]

    missing_required = [c for c in _MANIFEST_REQUIRED if c not in manifest.columns]
    if missing_required:
        raise ValueError(f"Manifest is missing required columns: {sorted(missing_required)}")

    manifest = manifest.with_columns(pl.col("data_type").cast(pl.Utf8).str.to_lowercase())

    if route == "edge_uuid":
        return _resolve_manifest_by_owner(conn, manifest, owner_col="edge_uuid", registry=registry)
    if route == "path":
        manifest = _attach_node_uuid_from_path(conn, manifest)
    return _resolve_manifest_by_owner(conn, manifest, owner_col="node_uuid", registry=registry)


def _attach_node_uuid_from_path(conn, manifest: pl.DataFrame) -> pl.DataFrame:
    """Resolve every ``path`` (List(Utf8)) to a ``node_uuid`` and attach it."""
    if manifest["path"].dtype != pl.List(pl.Utf8):
        raise ValueError(f"Manifest 'path' column must be List(Utf8); got {manifest['path'].dtype}")
    paths_lists = manifest["path"].to_list()
    unique_paths = list({tuple(p) for p in paths_lists if p is not None})
    if not unique_paths:
        return manifest.with_columns(pl.lit(None, dtype=pl.Utf8).cast(pl.Utf8).alias("node_uuid"))

    path_to_uuid = resolve_paths_to_uuids(conn, unique_paths)
    node_uuids = [str(path_to_uuid[tuple(p)]) if p is not None else None for p in paths_lists]
    return manifest.with_columns(pl.Series("node_uuid", node_uuids, dtype=pl.Utf8))


def _coerce_uuid_col(manifest: pl.DataFrame, col: str) -> pl.DataFrame:
    """Normalize a uuid routing column to ``str`` form for joining.

    Manifests may carry uuids as ``UUID`` objects, strings, or polars Utf8.
    Joins against PG-returned uuids work cleanly when both sides are str.
    Fast-path the common case where the column is already ``Utf8``.
    """
    if manifest[col].dtype == pl.Utf8:
        return manifest
    values = [str(v) if v is not None else None for v in manifest[col].to_list()]
    return manifest.with_columns(pl.Series(col, values, dtype=pl.Utf8))


def _resolve_manifest_by_owner(
    conn,
    manifest: pl.DataFrame,
    *,
    owner_col: str,
    registry: SeriesRegistry | None = None,
) -> pl.DataFrame:
    """Resolve a manifest routed by ``owner_col`` (``node_uuid`` or ``edge_uuid``)
    against the series table.

    Workflow:

    1. Build the unique ``(owner_uuid, data_type, name)`` triples present in
       the manifest. With ``registry`` provided, partition into cache hits
       and misses; without it, every triple is a miss.
    2. If misses remain, issue one round-trip — a join against
       ``unnest(uuid[], text[], text[])`` so each triple is served by a
       single probe of the ``series_node_uniq`` / ``series_edge_uniq`` index.
       Populate the registry from the result.
    3. Build the resolved frame by joining the per-triple meta lookup back
       onto the manifest. The lookup is at most ``unique_triples`` rows
       (typically thousands at most), so the join is hash-join over a small
       right side — orders of magnitude cheaper than a per-row Python loop
       on write manifests, which carry one row per data point (millions).
    """
    manifest = _coerce_uuid_col(manifest, owner_col)

    triples_df = manifest.select([owner_col, "data_type", "name"]).drop_nulls(subset=[owner_col]).unique()
    if triples_df.height == 0:
        raise ValueError(f"No {owner_col} values to resolve in manifest.")

    triples: list[tuple[str, str, str]] = list(
        zip(
            triples_df[owner_col].to_list(),
            triples_df["data_type"].to_list(),
            triples_df["name"].to_list(),
            strict=True,
        )
    )

    if registry is not None:
        hits, misses = registry.lookup_triples(triples)
        meta_map: dict[tuple[str, str, str], SeriesMeta] = dict(hits)
    else:
        meta_map = {}
        misses = triples

    if misses:
        owner_vals = [t[0] for t in misses]
        dt_vals = [t[1] for t in misses]
        name_vals = [t[2] for t in misses]
        rows = conn.execute(
            f"""
            SELECT s.{owner_col}, s.data_type, s.name, s.series_id,
                   s.canonical_unit, s.timeseries_type, s.retention
            FROM unnest(%s::uuid[], %s::text[], %s::text[]) AS q(owner_uuid, data_type, name)
            JOIN energydb.series s
              ON s.{owner_col} = q.owner_uuid
             AND s.data_type   = q.data_type
             AND s.name        = q.name
            """,
            (owner_vals, dt_vals, name_vals),
        ).fetchall()
        for owner, dt, name, sid, unit, ts_type, retention in rows:
            owner_str = str(owner)
            meta = SeriesMeta(
                series_id=sid,
                canonical_unit=unit,
                timeseries_type=ts_type,
                retention=retention,
            )
            meta_map[(owner_str, dt, name)] = meta
            if registry is not None:
                registry.insert(owner_str, dt, name, meta)

    return _build_resolved_frame(manifest, owner_col, meta_map)


def _build_resolved_frame(
    manifest: pl.DataFrame,
    owner_col: str,
    meta_map: dict[tuple[str, str, str], SeriesMeta],
) -> pl.DataFrame:
    """Append ``(series_id, canonical_unit, timeseries_type, retention)`` to
    each manifest row by joining a small ``triple → meta`` lookup frame.

    Raises :class:`ValueError` on a null routing value or the first
    unresolved triple.
    """
    if manifest[owner_col].null_count() > 0:
        null_row = manifest.filter(pl.col(owner_col).is_null()).row(0, named=True)
        raise ValueError(
            f"Series not registered for {owner_col}=None, "
            f"data_type={null_row['data_type']!r}, name={null_row['name']!r}."
        )

    keys = list(meta_map.keys())
    metas = list(meta_map.values())
    lookup = pl.DataFrame(
        {
            owner_col: [k[0] for k in keys],
            "data_type": [k[1] for k in keys],
            "name": [k[2] for k in keys],
            "series_id": [m.series_id for m in metas],
            "canonical_unit": [m.canonical_unit for m in metas],
            "timeseries_type": [m.timeseries_type for m in metas],
            "retention": [m.retention for m in metas],
        },
        schema={
            owner_col: pl.Utf8,
            "data_type": pl.Utf8,
            "name": pl.Utf8,
            "series_id": pl.Int64,
            "canonical_unit": pl.Utf8,
            "timeseries_type": pl.Utf8,
            "retention": pl.Utf8,
        },
    )

    resolved = manifest.join(lookup, on=[owner_col, "data_type", "name"], how="left")

    unresolved = resolved.filter(pl.col("series_id").is_null())
    if unresolved.height > 0:
        bad = unresolved.row(0, named=True)
        raise ValueError(
            f"Series not registered for {owner_col}={bad[owner_col]!r}, "
            f"data_type={bad['data_type']!r}, name={bad['name']!r}."
        )
    return resolved


__all__ = [
    "Path",
    "fetch_edge_hierarchy_bulk",
    "fetch_node_hierarchy_bulk",
    "resolve_edge_uuid",
    "resolve_manifest",
    "resolve_node_uuid",
    "resolve_path",
    "resolve_paths_bulk",
    "resolve_paths_to_uuids",
    "resolve_subtree_uuids",
]
