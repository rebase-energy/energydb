"""Internal resolution utilities for energydb.

Handles: node path resolution, subtree traversal, series_id lookup,
manifest resolution, path construction, hierarchy join-back,
and edge resolution.
"""

from __future__ import annotations

from typing import Any

import polars as pl


# ---------------------------------------------------------------------------
# Node resolution
# ---------------------------------------------------------------------------


def resolve_node_id(conn, name_chain: list[str], *, start_id: int | None = None) -> int:
    """Resolve a lazy path like ["Europe", "Sweden", "Lillgrund"] to a node_id.

    Walks the chain top-down. If ``start_id`` is given, the first name is
    resolved as a child of that node (relative navigation). Otherwise the
    first name must be a root (parent_id IS NULL).

    Raises ValueError if any step fails or is ambiguous.
    """
    current_id = start_id
    for i, name in enumerate(name_chain):
        if current_id is None:
            # Root node lookup
            rows = conn.execute(
                "SELECT node_id FROM energydb.node "
                "WHERE name = %s AND parent_id IS NULL",
                (name,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT node_id FROM energydb.node "
                "WHERE name = %s AND parent_id = %s",
                (name, current_id),
            ).fetchall()

        if len(rows) == 0:
            path_so_far = "/".join(name_chain[: i + 1])
            raise ValueError(f"Node not found: {path_so_far}")
        if len(rows) > 1:
            ids = [r[0] for r in rows]
            raise ValueError(
                f"Multiple nodes named '{name}' (ids: {ids}). "
                f"Use node(id=...) to disambiguate."
            )
        current_id = rows[0][0]

    return current_id


def resolve_node_id_by_name(conn, name: str, parent_id: int | None = None) -> int:
    """Resolve a single node name to a node_id.

    If parent_id is given, scopes to children of that parent.
    Raises on not found or ambiguous.
    """
    if parent_id is not None:
        rows = conn.execute(
            "SELECT node_id FROM energydb.node "
            "WHERE name = %s AND parent_id = %s",
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
        raise ValueError(
            f"Multiple nodes named '{name}' (ids: {ids}). "
            f"Use node(id=...) to disambiguate."
        )
    return rows[0][0]


# ---------------------------------------------------------------------------
# Subtree traversal
# ---------------------------------------------------------------------------


def resolve_subtree_ids(conn, node_id: int) -> list[int]:
    """Return all descendant node_ids (including the node itself) via recursive CTE."""
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


# ---------------------------------------------------------------------------
# Series resolution (node)
# ---------------------------------------------------------------------------


def resolve_series_ids(
    conn,
    node_ids: list[int],
    *,
    data_type: str | None = None,
    name: str | None = None,
) -> list[int]:
    """Find series_ids linked to the given node_ids, optionally filtered."""
    conditions = ["node_id = ANY(%s)"]
    params: list[Any] = [node_ids]

    if data_type:
        conditions.append("data_type = %s")
        params.append(data_type.lower() if hasattr(data_type, "lower") else str(data_type).lower())
    if name:
        conditions.append("name = %s")
        params.append(name)

    where = " AND ".join(conditions)
    rows = conn.execute(
        f"SELECT series_id FROM energydb.node_series WHERE {where}",
        params,
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Path construction
# ---------------------------------------------------------------------------


def resolve_path(conn, node_id: int) -> str:
    """Walk ancestors from node_id to root, return "Europe/Sweden/Lillgrund/T01"."""
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
    """Resolve paths for multiple node_ids in one query.

    Returns {node_id: "Europe/Sweden/Lillgrund/T01"}.
    """
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
# Manifest resolution
# ---------------------------------------------------------------------------


def resolve_manifest(conn, manifest: pl.DataFrame) -> pl.DataFrame:
    """Resolve a manifest DataFrame to series_ids.

    Detects routing mode from columns:
    - "node_id" column → direct lookup
    - "path" column → parse paths, resolve to node_ids, then lookup
    - "edge_id" column → direct lookup via edge_series

    Required columns: data_type, name (plus one of node_id, path, or edge_id).
    Returns manifest with "series_id" column added.
    """
    has_node_id = "node_id" in manifest.columns
    has_path = "path" in manifest.columns
    has_edge_id = "edge_id" in manifest.columns

    mode_count = sum([has_node_id, has_path, has_edge_id])
    if mode_count > 1:
        raise ValueError("Manifest must have exactly one of 'node_id', 'path', or 'edge_id' columns")
    if mode_count == 0:
        raise ValueError("Manifest must have one of 'node_id', 'path', or 'edge_id' column")

    required = {"data_type", "name"}
    missing = required - set(manifest.columns)
    if missing:
        raise ValueError(f"Manifest is missing required columns: {sorted(missing)}")

    # Normalize data_type to lowercase strings (DB stores lowercase)
    manifest = manifest.with_columns(
        pl.col("data_type").cast(pl.Utf8).str.to_lowercase()
    )

    if has_edge_id:
        return _resolve_manifest_edge(conn, manifest)

    if has_path:
        # Resolve paths to node_ids
        unique_paths = manifest["path"].unique().to_list()
        path_to_id: dict[str, int] = {}
        for path in unique_paths:
            parts = path.split("/")
            path_to_id[path] = resolve_node_id(conn, parts)

        manifest = manifest.with_columns(
            pl.col("path").replace_strict(path_to_id).alias("node_id")
        )

    # Now we have node_id — resolve to series_ids via node_series
    unique_routes = manifest.select(["node_id", "data_type", "name"]).unique()

    # One query: get all matching node_series rows
    node_ids = unique_routes["node_id"].to_list()
    rows = conn.execute(
        "SELECT node_id, series_id, data_type, name "
        "FROM energydb.node_series WHERE node_id = ANY(%s)",
        (node_ids,),
    ).fetchall()

    # Build lookup: (node_id, data_type, name) → series_id
    lookup_df = pl.DataFrame(
        {"node_id": [r[0] for r in rows], "series_id": [r[1] for r in rows],
         "_dt": [r[2] for r in rows], "_met": [r[3] for r in rows]}
    )

    # Join to get series_id on each manifest row
    result = manifest.join(
        lookup_df,
        left_on=["node_id", "data_type", "name"],
        right_on=["node_id", "_dt", "_met"],
        how="left",
    )

    missing = result.filter(pl.col("series_id").is_null())
    if missing.height > 0:
        sample = missing.row(0)
        raise ValueError(
            f"No series found for node_id={sample[0]}, "
            f"data_type={sample[1]}, name={sample[2]}"
        )

    return result


def _resolve_manifest_edge(conn, manifest: pl.DataFrame) -> pl.DataFrame:
    """Resolve an edge-routed manifest to series_ids."""
    unique_routes = manifest.select(["edge_id", "data_type", "name"]).unique()
    edge_ids = unique_routes["edge_id"].to_list()

    rows = conn.execute(
        "SELECT edge_id, series_id, data_type, name "
        "FROM energydb.edge_series WHERE edge_id = ANY(%s)",
        (edge_ids,),
    ).fetchall()

    lookup_df = pl.DataFrame(
        {"edge_id": [r[0] for r in rows], "series_id": [r[1] for r in rows],
         "_dt": [r[2] for r in rows], "_met": [r[3] for r in rows]}
    )

    result = manifest.join(
        lookup_df,
        left_on=["edge_id", "data_type", "name"],
        right_on=["edge_id", "_dt", "_met"],
        how="left",
    )

    missing = result.filter(pl.col("series_id").is_null())
    if missing.height > 0:
        sample = missing.row(0)
        raise ValueError(
            f"No series found for edge_id={sample[0]}, "
            f"data_type={sample[1]}, name={sample[2]}"
        )

    return result


# ---------------------------------------------------------------------------
# Hierarchy join-back
# ---------------------------------------------------------------------------


def join_hierarchy(conn, result: pl.DataFrame, series_ids: list[int]) -> pl.DataFrame:
    """Enrich a timedb result with hierarchy context.

    Given a DataFrame with 'series_id' column (from td.read()), joins back:
    path, node_id, node (name), node_type, data_type, name.
    """
    if not series_ids:
        return result

    # Get node_series + node info for all series_ids
    rows = conn.execute(
        """
        SELECT ns.series_id, ns.data_type, ns.name,
               n.node_id, n.name, n.node_type
        FROM energydb.node_series ns
        JOIN energydb.node n ON ns.node_id = n.node_id
        WHERE ns.series_id = ANY(%s)
        """,
        (series_ids,),
    ).fetchall()

    if not rows:
        return result

    # Build hierarchy info DataFrame
    meta_df = pl.DataFrame({
        "series_id": [r[0] for r in rows],
        "data_type": [r[1] for r in rows],
        "name": [r[2] for r in rows],
        "node_id": [r[3] for r in rows],
        "node": [r[4] for r in rows],
        "node_type": [r[5] for r in rows],
    })

    # Resolve paths for all unique node_ids
    unique_node_ids = meta_df["node_id"].unique().to_list()
    paths = resolve_paths_bulk(conn, unique_node_ids)
    meta_df = meta_df.with_columns(
        pl.col("node_id").replace_strict(paths).alias("path")
    )

    # Join onto the result
    return result.join(meta_df, on="series_id", how="left")


# ---------------------------------------------------------------------------
# Edge resolution
# ---------------------------------------------------------------------------


def resolve_edge_id_by_name(conn, name: str) -> int:
    """Resolve an edge name to an edge_id.

    Raises on not found or ambiguous.
    """
    rows = conn.execute(
        "SELECT edge_id FROM energydb.edge WHERE name = %s",
        (name,),
    ).fetchall()

    if len(rows) == 0:
        raise ValueError(f"Edge not found: {name}")
    if len(rows) > 1:
        ids = [r[0] for r in rows]
        raise ValueError(
            f"Multiple edges named '{name}' (ids: {ids}). "
            f"Use edge(id=...) to disambiguate."
        )
    return rows[0][0]


def resolve_edge_series_ids(
    conn,
    edge_ids: list[int],
    *,
    data_type: str | None = None,
    name: str | None = None,
) -> list[int]:
    """Find series_ids linked to the given edge_ids, optionally filtered."""
    conditions = ["edge_id = ANY(%s)"]
    params: list[Any] = [edge_ids]

    if data_type:
        conditions.append("data_type = %s")
        params.append(data_type.lower() if hasattr(data_type, "lower") else str(data_type).lower())
    if name:
        conditions.append("name = %s")
        params.append(name)

    where = " AND ".join(conditions)
    rows = conn.execute(
        f"SELECT series_id FROM energydb.edge_series WHERE {where}",
        params,
    ).fetchall()
    return [r[0] for r in rows]


def build_edge_series_labels(conn, edge_id: int, data_type: str) -> dict[str, str]:
    """Construct the timedb label dict for an edge series registration."""
    # Get from/to node paths for discoverability
    row = conn.execute(
        "SELECT from_node_id, to_node_id, name FROM energydb.edge WHERE edge_id = %s",
        (edge_id,),
    ).fetchone()

    labels: dict[str, str] = {
        "edge_id": str(edge_id),
        "data_type": data_type.lower() if hasattr(data_type, "lower") else str(data_type).lower(),
    }

    if row:
        from_path = resolve_path(conn, row[0])
        to_path = resolve_path(conn, row[1])
        labels["from_node"] = from_path
        labels["to_node"] = to_path
        if row[2]:
            labels["edge"] = row[2]

    return labels


def join_edge_hierarchy(conn, result: pl.DataFrame, series_ids: list[int]) -> pl.DataFrame:
    """Enrich a timedb result with edge context.

    Given a DataFrame with 'series_id' column (from td.read()), joins back:
    edge_id, edge (name), edge_type, data_type, name,
    from_node path, to_node path.
    """
    if not series_ids:
        return result

    rows = conn.execute(
        """
        SELECT es.series_id, es.data_type, es.name,
               e.edge_id, e.name, e.edge_type,
               e.from_node_id, e.to_node_id
        FROM energydb.edge_series es
        JOIN energydb.edge e ON es.edge_id = e.edge_id
        WHERE es.series_id = ANY(%s)
        """,
        (series_ids,),
    ).fetchall()

    if not rows:
        return result

    # Collect all node_ids for path resolution
    all_node_ids = list(set(r[6] for r in rows) | set(r[7] for r in rows))
    paths = resolve_paths_bulk(conn, all_node_ids)

    meta_df = pl.DataFrame({
        "series_id": [r[0] for r in rows],
        "data_type": [r[1] for r in rows],
        "name": [r[2] for r in rows],
        "edge_id": [r[3] for r in rows],
        "edge": [r[4] for r in rows],
        "edge_type": [r[5] for r in rows],
        "from_node": [paths.get(r[6], "") for r in rows],
        "to_node": [paths.get(r[7], "") for r in rows],
    })

    return result.join(meta_df, on="series_id", how="left")


# ---------------------------------------------------------------------------
# Label construction for series registration
# ---------------------------------------------------------------------------


def build_series_labels(conn, node_id: int, data_type: str) -> dict[str, str]:
    """Construct the timedb label dict for a series registration.

    Includes node_id as immutable anchor + ancestor names for discoverability.
    """
    path = resolve_path(conn, node_id)
    parts = path.split("/")

    labels: dict[str, str] = {
        "node_id": str(node_id),
        "data_type": data_type.lower() if hasattr(data_type, "lower") else str(data_type).lower(),
    }

    # Add ancestor names for discoverability
    # The last part is the node itself, previous parts are ancestors
    if len(parts) >= 1:
        labels["node"] = parts[-1]
    if len(parts) >= 2:
        labels["parent"] = parts[-2]

    # Store the full path for easy reference
    labels["path"] = path

    return labels
