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
from uuid import UUID

import polars as pl

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


def resolve_manifest(conn, manifest: pl.DataFrame) -> pl.DataFrame:
    """Resolve a routing manifest to series metadata.

    Detects routing mode from the columns present:

    * ``node_uuid`` — direct lookup against ``energydb.series.node_uuid``.
    * ``path``      — ``List(Utf8)`` path; resolved to ``node_uuid`` first.
    * ``edge_uuid`` — direct lookup against ``energydb.series.edge_uuid``.

    The manifest must also carry ``data_type`` and ``name`` columns. Returns
    the original frame plus ``series_id``, ``retention``, ``canonical_unit``,
    and ``timeseries_type``.
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
        return _resolve_manifest_by_owner(conn, manifest, owner_col="edge_uuid")
    if route == "path":
        manifest = _attach_node_uuid_from_path(conn, manifest)
    return _resolve_manifest_by_owner(conn, manifest, owner_col="node_uuid")


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


def _series_lookup_df(rows: list[tuple], owner_col: str) -> pl.DataFrame:
    """Build the (owner_uuid, data_type, name) → series-meta lookup frame."""
    return pl.DataFrame(
        {
            owner_col: [str(r[0]) for r in rows],
            "_dt": [r[1] for r in rows],
            "_name": [r[2] for r in rows],
            "series_id": [r[3] for r in rows],
            "canonical_unit": [r[4] for r in rows],
            "timeseries_type": [r[5] for r in rows],
            "retention": [r[6] for r in rows],
        },
        schema={
            owner_col: pl.Utf8,
            "_dt": pl.Utf8,
            "_name": pl.Utf8,
            "series_id": pl.Int64,
            "canonical_unit": pl.Utf8,
            "timeseries_type": pl.Utf8,
            "retention": pl.Utf8,
        },
    )


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
) -> pl.DataFrame:
    """Resolve a manifest routed by ``owner_col`` (``node_uuid`` or ``edge_uuid``)
    against the series table.

    The owner column is coerced to ``Utf8`` on entry so it can join against
    the PG-side ``::text`` cast cleanly.
    """
    manifest = _coerce_uuid_col(manifest, owner_col)
    owner_vals = [v for v in manifest[owner_col].unique().to_list() if v is not None]
    if not owner_vals:
        raise ValueError(f"No {owner_col} values to resolve in manifest.")

    rows = conn.execute(
        f"SELECT {owner_col}, data_type, name, series_id, canonical_unit, timeseries_type, retention "
        f"FROM energydb.series WHERE {owner_col}::text = ANY(%s)",
        (owner_vals,),
    ).fetchall()
    lookup = _series_lookup_df(rows, owner_col)

    resolved = manifest.join(
        lookup,
        left_on=[owner_col, "data_type", "name"],
        right_on=[owner_col, "_dt", "_name"],
        how="left",
    ).drop("_dt", "_name", strict=False)

    _raise_on_unresolved(resolved, owner_col)
    return resolved


def _raise_on_unresolved(resolved: pl.DataFrame, owner_col: str) -> None:
    missing = resolved.filter(pl.col("series_id").is_null())
    if missing.height == 0:
        return
    sample = missing.row(0, named=True)
    raise ValueError(
        f"Series not registered for {owner_col}={sample[owner_col]!r}, "
        f"data_type={sample['data_type']!r}, name={sample['name']!r}."
    )


__all__ = [
    "Path",
    "resolve_node_uuid",
    "resolve_paths_to_uuids",
    "resolve_subtree_uuids",
    "resolve_path",
    "resolve_paths_bulk",
    "resolve_edge_uuid",
    "resolve_manifest",
]
