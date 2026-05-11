"""Post-read hierarchy hydration: attach path / node / edge info to a
timedb read result. Polars-native; no SQL beyond a single bulk lookup.
"""

from __future__ import annotations

from uuid import UUID

import polars as pl

from energydb.paths import resolve_paths_bulk


def join_hierarchy(conn, result: pl.DataFrame, meta: pl.DataFrame) -> pl.DataFrame:
    """Attach path/node info to a timedb read result.

    *meta* has columns (series_id, node_uuid, data_type, name) from
    ``series.resolve_for_read``. Returns *result* joined with
    (path, node, node_type, node_uuid, data_type, name). ``path`` is
    ``List(Utf8)``.
    """
    if result.is_empty() or meta.is_empty():
        return result

    node_uuid_strs = [u for u in meta["node_uuid"].to_list() if u is not None]
    if not node_uuid_strs:
        return result
    node_uuids = [UUID(u) for u in node_uuid_strs]

    rows = conn.execute(
        "SELECT uuid, name, node_type FROM energydb.node WHERE uuid = ANY(%s)",
        (node_uuids,),
    ).fetchall()
    node_df = pl.DataFrame(
        {
            "node_uuid": [str(r[0]) for r in rows],
            "node": [r[1] for r in rows],
            "node_type": [r[2] for r in rows],
        },
        schema={"node_uuid": pl.Utf8, "node": pl.Utf8, "node_type": pl.Utf8},
    )

    paths = resolve_paths_bulk(conn, sorted(set(node_uuids), key=str))
    paths_df = pl.DataFrame(
        {
            "node_uuid": [str(k) for k in paths],
            "path": [list(p) for p in paths.values()],
        },
        schema={"node_uuid": pl.Utf8, "path": pl.List(pl.Utf8)},
    )

    meta_with_path = meta.join(node_df, on="node_uuid", how="left").join(paths_df, on="node_uuid", how="left")
    extra = meta_with_path.select(["series_id", "path", "node", "node_type", "node_uuid", "data_type", "name"]).unique(
        subset=["series_id"]
    )
    return result.join(extra, on="series_id", how="left")


def join_edge_hierarchy(conn, result: pl.DataFrame, meta: pl.DataFrame) -> pl.DataFrame:
    """Attach edge + endpoint info to a timedb read result.

    Endpoint paths are ``List(Utf8)``; the edge name is exposed as ``edge``.
    """
    if result.is_empty() or meta.is_empty():
        return result

    edge_uuid_strs = [u for u in meta["edge_uuid"].to_list() if u is not None]
    if not edge_uuid_strs:
        return result
    edge_uuids = [UUID(u) for u in edge_uuid_strs]

    rows = conn.execute(
        "SELECT uuid, name, edge_type, from_node_uuid, to_node_uuid FROM energydb.edge WHERE uuid = ANY(%s)",
        (edge_uuids,),
    ).fetchall()
    node_uuids = list({r[3] for r in rows} | {r[4] for r in rows})
    paths = resolve_paths_bulk(conn, node_uuids)

    edge_df = pl.DataFrame(
        {
            "edge_uuid": [str(r[0]) for r in rows],
            "edge": [r[1] for r in rows],
            "edge_type": [r[2] for r in rows],
            "from_node": [list(paths.get(r[3], ())) for r in rows],
            "to_node": [list(paths.get(r[4], ())) for r in rows],
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
