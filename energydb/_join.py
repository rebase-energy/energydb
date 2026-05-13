"""Post-read hierarchy hydration: attach path / node / edge info to a
timedb read result. Polars-native; on a warm :class:`SeriesRegistry` no
PG round-trips are needed at all.
"""

from __future__ import annotations

import polars as pl

from energydb._resolve_cache import NodeMeta, SeriesRegistry
from energydb.paths import fetch_edge_hierarchy_bulk, fetch_node_hierarchy_bulk


def join_hierarchy(
    conn,
    result: pl.DataFrame,
    meta: pl.DataFrame,
    *,
    registry: SeriesRegistry | None = None,
) -> pl.DataFrame:
    """Attach path/node info to a timedb read result.

    *meta* has columns (series_id, node_uuid, data_type, name) from
    ``series.resolve_for_read``. Returns *result* joined with
    (path, node, node_type, node_uuid, data_type, name). ``path`` is
    ``List(Utf8)``.

    When ``registry`` is provided, cached entries are served without any
    PG round-trip and cold misses are loaded with one recursive CTE and
    written back through.
    """
    if result.is_empty() or meta.is_empty():
        return result

    node_uuid_strs = [u for u in meta["node_uuid"].to_list() if u is not None]
    if not node_uuid_strs:
        return result

    node_meta = _resolve_node_metas(conn, node_uuid_strs, registry)

    node_df = pl.DataFrame(
        {
            "node_uuid": list(node_meta.keys()),
            "node": [m.name for m in node_meta.values()],
            "node_type": [m.node_type for m in node_meta.values()],
            "path": [list(m.path) for m in node_meta.values()],
        },
        schema={
            "node_uuid": pl.Utf8,
            "node": pl.Utf8,
            "node_type": pl.Utf8,
            "path": pl.List(pl.Utf8),
        },
    )

    meta_with_path = meta.join(node_df, on="node_uuid", how="left")
    extra = meta_with_path.select(["series_id", "path", "node", "node_type", "node_uuid", "data_type", "name"]).unique(
        subset=["series_id"]
    )
    return result.join(extra, on="series_id", how="left")


def join_edge_hierarchy(
    conn,
    result: pl.DataFrame,
    meta: pl.DataFrame,
    *,
    registry: SeriesRegistry | None = None,
) -> pl.DataFrame:
    """Attach edge + endpoint info to a timedb read result.

    Endpoint paths are ``List(Utf8)``; the edge name is exposed as ``edge``.
    When ``registry`` is provided, both the edge metadata and the endpoint
    paths are served from the cache when warm; misses go through one PG
    round-trip per cache (edge + node) and are written back through.
    """
    if result.is_empty() or meta.is_empty():
        return result

    edge_uuid_strs = [u for u in meta["edge_uuid"].to_list() if u is not None]
    if not edge_uuid_strs:
        return result

    edge_meta = _resolve_edge_metas(conn, edge_uuid_strs, registry)
    endpoint_uuids = sorted(
        {m.from_node_uuid for m in edge_meta.values()} | {m.to_node_uuid for m in edge_meta.values()}
    )
    node_meta = _resolve_node_metas(conn, endpoint_uuids, registry)

    def _path_or_empty(uid: str) -> list[str]:
        nm = node_meta.get(uid)
        return list(nm.path) if nm is not None else []

    edge_df = pl.DataFrame(
        {
            "edge_uuid": list(edge_meta.keys()),
            "edge": [m.name for m in edge_meta.values()],
            "edge_type": [m.edge_type for m in edge_meta.values()],
            "from_node": [_path_or_empty(m.from_node_uuid) for m in edge_meta.values()],
            "to_node": [_path_or_empty(m.to_node_uuid) for m in edge_meta.values()],
        },
        schema={
            "edge_uuid": pl.Utf8,
            "edge": pl.Utf8,
            "edge_type": pl.Utf8,
            "from_node": pl.List(pl.Utf8),
            "to_node": pl.List(pl.Utf8),
        },
    )

    meta_with_edge = meta.join(edge_df, on="edge_uuid", how="left")
    extra = meta_with_edge.select(
        ["series_id", "edge_uuid", "edge", "edge_type", "from_node", "to_node", "data_type", "name"]
    ).unique(subset=["series_id"])
    return result.join(extra, on="series_id", how="left")


def meta_from_resolved_manifest(resolved: pl.DataFrame, *, is_edge: bool) -> pl.DataFrame:
    """Project the columns that ``join_hierarchy`` / ``join_edge_hierarchy`` expect."""
    cols = ["series_id", "data_type", "name", "canonical_unit", "retention"]
    cols.append("edge_uuid" if is_edge else "node_uuid")
    if not is_edge and "node_uuid" not in resolved.columns:
        # path-routed manifest: node_uuid was attached during resolve
        raise RuntimeError("resolve_manifest did not attach node_uuid for a node-routed manifest")
    return resolved.select(cols).unique()


# ---------------------------------------------------------------------------
# Internal: cache-aware bulk fetches
# ---------------------------------------------------------------------------


def _resolve_node_metas(
    conn,
    node_uuid_strs: list[str],
    registry: SeriesRegistry | None,
) -> dict[str, NodeMeta]:
    """Return ``uuid → NodeMeta`` for the given uuids, populating the cache.

    Without a registry every uuid is fetched. With a registry, hits are
    served from memory and misses go through one combined recursive-CTE
    round-trip.
    """
    if registry is None:
        fetched = fetch_node_hierarchy_bulk(conn, node_uuid_strs)
        return {uid: meta for uid, (meta, _parent) in fetched.items()}

    hits, misses = registry.lookup_nodes(node_uuid_strs)
    if misses:
        fetched = fetch_node_hierarchy_bulk(conn, misses)
        for uid, (meta, parent_uuid) in fetched.items():
            registry.insert_node(uid, meta, parent_uuid)
            hits[uid] = meta
    return hits


def _resolve_edge_metas(
    conn,
    edge_uuid_strs: list[str],
    registry: SeriesRegistry | None,
):
    """Return ``uuid → EdgeMeta`` for the given uuids, populating the cache."""
    if registry is None:
        return fetch_edge_hierarchy_bulk(conn, edge_uuid_strs)

    hits, misses = registry.lookup_edges(edge_uuid_strs)
    if misses:
        fetched = fetch_edge_hierarchy_bulk(conn, misses)
        for uid, meta in fetched.items():
            registry.insert_edge(uid, meta)
            hits[uid] = meta
    return hits
