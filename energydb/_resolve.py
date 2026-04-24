"""Hierarchy resolution for energydb.

Only hierarchy + path logic lives here now. Series lookups moved to
``energydb.series``.
"""

from __future__ import annotations

from typing import Any

import polars as pl

# ---------------------------------------------------------------------------
# Node resolution
# ---------------------------------------------------------------------------


def resolve_node_id(conn, name_chain: list[str], *, start_id: int | None = None) -> int:
    """Resolve a lazy path like ["Europe", "Sweden", "Lillgrund"] to a node_id."""
    current_id = start_id
    for i, name in enumerate(name_chain):
        if current_id is None:
            rows = conn.execute(
                "SELECT node_id FROM energydb.node WHERE name = %s AND parent_id IS NULL",
                (name,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT node_id FROM energydb.node WHERE name = %s AND parent_id = %s",
                (name, current_id),
            ).fetchall()

        if len(rows) == 0:
            path_so_far = "/".join(name_chain[: i + 1])
            raise ValueError(f"Node not found: {path_so_far}")
        if len(rows) > 1:
            ids = [r[0] for r in rows]
            raise ValueError(f"Multiple nodes named {name!r} (ids: {ids}). Use node(id=...) to disambiguate.")
        current_id = rows[0][0]

    assert current_id is not None
    return current_id


def resolve_node_id_by_name(conn, name: str, parent_id: int | None = None) -> int:
    if parent_id is not None:
        rows = conn.execute(
            "SELECT node_id FROM energydb.node WHERE name = %s AND parent_id = %s",
            (name, parent_id),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT node_id FROM energydb.node WHERE name = %s",
            (name,),
        ).fetchall()

    if len(rows) == 0:
        raise ValueError(f"Node not found: {name}")
    if len(rows) > 1:
        ids = [r[0] for r in rows]
        raise ValueError(f"Multiple nodes named {name!r} (ids: {ids}). Use node(id=...) to disambiguate.")
    return rows[0][0]


# ---------------------------------------------------------------------------
# Subtree + path
# ---------------------------------------------------------------------------


def resolve_subtree_ids(conn, node_id: int) -> list[int]:
    rows = conn.execute(
        """
        WITH RECURSIVE subtree AS (
            SELECT node_id FROM energydb.node WHERE node_id = %s
            UNION ALL
            SELECT n.node_id FROM energydb.node n
            JOIN subtree s ON n.parent_id = s.node_id
        )
        SELECT node_id FROM subtree
        """,
        (node_id,),
    ).fetchall()
    return [r[0] for r in rows]


def resolve_path(conn, node_id: int) -> str:
    rows = conn.execute(
        """
        WITH RECURSIVE ancestors AS (
            SELECT node_id, name, parent_id, 0 AS depth
            FROM energydb.node WHERE node_id = %s
            UNION ALL
            SELECT n.node_id, n.name, n.parent_id, a.depth + 1
            FROM energydb.node n
            JOIN ancestors a ON n.node_id = a.parent_id
        )
        SELECT name FROM ancestors ORDER BY depth DESC
        """,
        (node_id,),
    ).fetchall()
    return "/".join(r[0] for r in rows)


def resolve_paths_bulk(conn, node_ids: list[int]) -> dict[int, str]:
    if not node_ids:
        return {}
    rows = conn.execute(
        """
        WITH RECURSIVE ancestors AS (
            SELECT node_id AS target_id, node_id, name, parent_id, 0 AS depth
            FROM energydb.node WHERE node_id = ANY(%s)
            UNION ALL
            SELECT a.target_id, n.node_id, n.name, n.parent_id, a.depth + 1
            FROM energydb.node n
            JOIN ancestors a ON n.node_id = a.parent_id
        )
        SELECT target_id, name, depth FROM ancestors ORDER BY target_id, depth DESC
        """,
        (node_ids,),
    ).fetchall()

    paths: dict[int, list[str]] = {}
    for target_id, name, _depth in rows:
        paths.setdefault(target_id, []).append(name)
    return {nid: "/".join(parts) for nid, parts in paths.items()}


# ---------------------------------------------------------------------------
# Edge resolution
# ---------------------------------------------------------------------------


def resolve_edge_id_by_name(conn, name: str) -> int:
    rows = conn.execute(
        "SELECT edge_id FROM energydb.edge WHERE name = %s",
        (name,),
    ).fetchall()
    if len(rows) == 0:
        raise ValueError(f"Edge not found: {name}")
    if len(rows) > 1:
        ids = [r[0] for r in rows]
        raise ValueError(f"Multiple edges named {name!r} (ids: {ids}). Use edge(id=...) to disambiguate.")
    return rows[0][0]


# ---------------------------------------------------------------------------
# Hierarchy join-back after a timedb read
# ---------------------------------------------------------------------------


def join_hierarchy(
    conn,
    result: pl.DataFrame,
    meta: pl.DataFrame,
) -> pl.DataFrame:
    """Attach path/node info to a timedb read result.

    *meta* has columns (series_id, node_id, data_type, name) from
    :func:`series.resolve_for_read`. Returns *result* joined with
    (path, node, node_type, node_id, data_type, name).
    """
    if result.is_empty() or meta.is_empty():
        return result

    node_ids = [nid for nid in meta["node_id"].to_list() if nid is not None]
    if not node_ids:
        return result

    rows = conn.execute(
        "SELECT node_id, name, node_type FROM energydb.node WHERE node_id = ANY(%s)",
        (node_ids,),
    ).fetchall()
    node_df = pl.DataFrame(
        {
            "node_id": [r[0] for r in rows],
            "node": [r[1] for r in rows],
            "node_type": [r[2] for r in rows],
        },
        schema={"node_id": pl.Int64, "node": pl.Utf8, "node_type": pl.Utf8},
    )

    paths = resolve_paths_bulk(conn, sorted(set(node_ids)))
    meta_with_path = meta.join(node_df, on="node_id", how="left").with_columns(
        pl.col("node_id").replace_strict(paths, default=None).alias("path")
    )

    extra = meta_with_path.select(["series_id", "path", "node", "node_type", "node_id", "data_type", "name"]).unique(
        subset=["series_id"]
    )
    return result.join(extra, on="series_id", how="left")


def join_edge_hierarchy(
    conn,
    result: pl.DataFrame,
    meta: pl.DataFrame,
) -> pl.DataFrame:
    """Attach edge + endpoint info to a timedb read result."""
    if result.is_empty() or meta.is_empty():
        return result

    edge_ids = [eid for eid in meta["edge_id"].to_list() if eid is not None]
    if not edge_ids:
        return result

    rows = conn.execute(
        "SELECT edge_id, name, edge_type, from_node_id, to_node_id FROM energydb.edge WHERE edge_id = ANY(%s)",
        (edge_ids,),
    ).fetchall()
    node_ids = list({r[3] for r in rows} | {r[4] for r in rows})
    paths = resolve_paths_bulk(conn, node_ids)

    edge_df = pl.DataFrame(
        {
            "edge_id": [r[0] for r in rows],
            "edge": [r[1] for r in rows],
            "edge_type": [r[2] for r in rows],
            "from_node": [paths.get(r[3], "") for r in rows],
            "to_node": [paths.get(r[4], "") for r in rows],
        },
        schema={
            "edge_id": pl.Int64,
            "edge": pl.Utf8,
            "edge_type": pl.Utf8,
            "from_node": pl.Utf8,
            "to_node": pl.Utf8,
        },
    )

    meta_with_edge = meta.join(edge_df, on="edge_id", how="left")
    extra = meta_with_edge.select(
        ["series_id", "edge_id", "edge", "edge_type", "from_node", "to_node", "data_type", "name"]
    ).unique(subset=["series_id"])
    return result.join(extra, on="series_id", how="left")


__all__ = [
    "resolve_node_id",
    "resolve_node_id_by_name",
    "resolve_subtree_ids",
    "resolve_path",
    "resolve_paths_bulk",
    "resolve_edge_id_by_name",
    "join_hierarchy",
    "join_edge_hierarchy",
]


# Keep name expected by existing scope code during transition (helper used here only)
_Any = Any
