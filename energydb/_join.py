"""Post-read hierarchy attachment: stamp ``path`` (and edge endpoints) onto a
timedb read result.

Polars-native; on a warm :class:`SeriesRegistry` no PG round-trips are needed
at all. Cold misses go through one combined recursive CTE per cache (node
or edge) and are written back through the registry.

Output column contract:

* Node-routed reads: ``path: Utf8`` (joined with ``/``), plus ``data_type``
  and ``name`` carried through from the manifest.
* Edge-routed reads: ``from_path: Utf8``, ``to_path: Utf8``, ``edge_type``,
  plus ``data_type`` / ``name``.

Internal identifiers (``series_id``, ``node_uuid``, ``edge_uuid``,
``node_type``, etc.) are NOT exposed on the result — callers identify
series by ``(path, data_type, name)`` (or edge equivalent).
"""

from __future__ import annotations

import polars as pl

from energydb._resolve_cache import NodeMeta, SeriesRegistry
from energydb.paths import fetch_edge_hierarchy_bulk, fetch_node_hierarchy_bulk


def attach_node_hierarchy(
    pool,
    result: pl.DataFrame,
    meta: pl.DataFrame,
    *,
    registry: SeriesRegistry | None = None,
) -> pl.DataFrame:
    """Attach ``path`` (Utf8) to a node-routed read result.

    *meta* carries ``(series_id, node_uuid, data_type, name)`` from the
    resolved manifest. ``data_type`` and ``name`` are preserved on every
    row; ``path`` is broadcast from the warm-cache ``NodeMeta.joined_path``.
    ``series_id`` is dropped from the public result. A PG conn is borrowed
    from ``pool`` only on a cache miss.
    """
    if result.is_empty() or meta.is_empty():
        return result

    node_uuid_strs = [u for u in meta["node_uuid"].to_list() if u is not None]
    if not node_uuid_strs:
        return result

    node_meta = _resolve_node_metas(pool, node_uuid_strs, registry)

    sid_to_path = (
        meta.select(["series_id", "node_uuid", "data_type", "name"])
        .with_columns(
            pl.col("node_uuid")
            .replace_strict({u: m.joined_path for u, m in node_meta.items()}, default=None)
            .alias("path"),
        )
        .select(["series_id", "path", "data_type", "name"])
        .unique(subset=["series_id"])
    )
    return result.join(sid_to_path, on="series_id", how="left").drop("series_id")


def attach_edge_hierarchy(
    pool,
    result: pl.DataFrame,
    meta: pl.DataFrame,
    *,
    registry: SeriesRegistry | None = None,
) -> pl.DataFrame:
    """Attach ``from_path``, ``to_path``, ``edge_type`` to an edge-routed result.

    Endpoint paths come from the node cache via the edge's endpoint uuids.
    ``data_type`` / ``name`` are preserved from the manifest. ``series_id``,
    ``edge_uuid``, and the edge's own ``name`` (intentionally distinct from
    series ``name``) are dropped from the public result. PG conn is
    borrowed from ``pool`` only on a cache miss.
    """
    if result.is_empty() or meta.is_empty():
        return result

    edge_uuid_strs = [u for u in meta["edge_uuid"].to_list() if u is not None]
    if not edge_uuid_strs:
        return result

    edge_meta = _resolve_edge_metas(pool, edge_uuid_strs, registry)
    endpoints = {m.from_node_uuid for m in edge_meta.values()} | {m.to_node_uuid for m in edge_meta.values()}
    node_meta = _resolve_node_metas(pool, sorted(endpoints), registry)

    def _joined(uid: str) -> str | None:
        nm = node_meta.get(uid)
        return nm.joined_path if nm is not None else None

    edge_lookup = pl.DataFrame(
        {
            "edge_uuid": list(edge_meta.keys()),
            "from_path": [_joined(m.from_node_uuid) for m in edge_meta.values()],
            "to_path": [_joined(m.to_node_uuid) for m in edge_meta.values()],
            "edge_type": [m.edge_type for m in edge_meta.values()],
        },
        schema={
            "edge_uuid": pl.Utf8,
            "from_path": pl.Utf8,
            "to_path": pl.Utf8,
            "edge_type": pl.Utf8,
        },
    )

    sid_lookup = (
        meta.select(["series_id", "edge_uuid", "data_type", "name"])
        .join(edge_lookup, on="edge_uuid", how="left")
        .select(["series_id", "from_path", "to_path", "edge_type", "data_type", "name"])
        .unique(subset=["series_id"])
    )
    return result.join(sid_lookup, on="series_id", how="left").drop("series_id")


# ---------------------------------------------------------------------------
# By-path partition (output="by_path")
# ---------------------------------------------------------------------------


def partition_node_by_path(
    pool,
    result: pl.DataFrame,
    meta: pl.DataFrame,
    *,
    registry: SeriesRegistry | None = None,
) -> dict[tuple[str, str, str], pl.DataFrame]:
    """Partition a node-routed CH result into ``{(path, data_type, name): df}``.

    Skips the per-row broadcast that ``attach_node_hierarchy`` does. Each
    sub-frame carries only the CH data columns (``valid_time``, ``value``,
    plus opt-in time/audit columns) — ``series_id`` is dropped, and
    ``path`` / ``data_type`` / ``name`` live in the key, not the row.

    Series that appear in the manifest but for which CH returned no rows
    get an empty sub-frame with the documented schema — callers can index
    by key without ``KeyError``.
    """
    if meta.is_empty():
        return {}

    node_uuid_strs = [u for u in meta["node_uuid"].to_list() if u is not None]
    node_meta_map = _resolve_node_metas(pool, node_uuid_strs, registry)

    sid_to_key: dict[int, tuple[str, str, str]] = {}
    for row in meta.iter_rows(named=True):
        nm = node_meta_map.get(row["node_uuid"])
        if nm is None:
            continue
        sid_to_key[row["series_id"]] = (nm.joined_path, row["data_type"], row["name"])

    return _build_partition(result, sid_to_key)


def partition_edge_by_path(
    pool,
    result: pl.DataFrame,
    meta: pl.DataFrame,
    *,
    registry: SeriesRegistry | None = None,
) -> dict[tuple[str, str, str, str, str], pl.DataFrame]:
    """Partition an edge-routed CH result into
    ``{(from_path, to_path, edge_type, data_type, name): df}``.

    Same shape as :func:`partition_node_by_path` for the data side; the key
    is extended with the edge endpoint paths and ``edge_type``.
    """
    if meta.is_empty():
        return {}

    edge_uuid_strs = [u for u in meta["edge_uuid"].to_list() if u is not None]
    edge_meta_map = _resolve_edge_metas(pool, edge_uuid_strs, registry)
    endpoints = {m.from_node_uuid for m in edge_meta_map.values()} | {m.to_node_uuid for m in edge_meta_map.values()}
    node_meta_map = _resolve_node_metas(pool, sorted(endpoints), registry)

    def _joined(uid: str) -> str:
        nm = node_meta_map.get(uid)
        return nm.joined_path if nm is not None else ""

    sid_to_key: dict[int, tuple[str, str, str, str, str]] = {}
    for row in meta.iter_rows(named=True):
        em = edge_meta_map.get(row["edge_uuid"])
        if em is None:
            continue
        sid_to_key[row["series_id"]] = (
            _joined(em.from_node_uuid),
            _joined(em.to_node_uuid),
            em.edge_type,
            row["data_type"],
            row["name"],
        )

    return _build_partition(result, sid_to_key)


def _build_partition(
    result: pl.DataFrame,
    sid_to_key: dict[int, tuple],
) -> dict[tuple, pl.DataFrame]:
    """Common partition assembly for ``partition_*_by_path``.

    Splits ``result`` by ``series_id`` (dropping that column from the
    sub-frames) and re-keys by the caller-supplied identity tuple. Series
    in ``sid_to_key`` that have no rows in ``result`` get an empty
    sub-frame with the CH-side data schema (minus ``series_id``).
    """
    data_schema = {c: dtype for c, dtype in result.schema.items() if c != "series_id"}

    out: dict[tuple, pl.DataFrame] = {}
    if not result.is_empty():
        parts = result.partition_by("series_id", as_dict=True, include_key=False)
        # parts is keyed by tuple of partition values, here (series_id,).
        for k_tuple, sub in parts.items():
            sid = k_tuple[0]
            key = sid_to_key.get(sid)
            if key is not None:
                out[key] = sub

    # Fill in empty sub-frames for series with no CH rows.
    for key in sid_to_key.values():
        if key not in out:
            out[key] = pl.DataFrame(schema=data_schema)
    return out


# ---------------------------------------------------------------------------
# Internal: cache-aware bulk fetches
# ---------------------------------------------------------------------------


def _resolve_node_metas(
    pool,
    node_uuid_strs: list[str],
    registry: SeriesRegistry | None,
) -> dict[str, NodeMeta]:
    """Return ``uuid → NodeMeta`` for the given uuids, populating the cache.

    A PG connection is borrowed from ``pool`` only when there is at least
    one cache miss — fully warm reads do zero PG work.
    """
    if registry is None:
        with pool.connection() as conn:
            fetched = fetch_node_hierarchy_bulk(conn, node_uuid_strs)
        return {uid: meta for uid, (meta, _parent) in fetched.items()}

    hits, misses = registry.lookup_nodes(node_uuid_strs)
    if not misses:
        return hits
    with pool.connection() as conn:
        fetched = fetch_node_hierarchy_bulk(conn, misses)
    for uid, (meta, parent_uuid) in fetched.items():
        registry.insert_node(uid, meta, parent_uuid)
        hits[uid] = meta
    return hits


def _resolve_edge_metas(
    pool,
    edge_uuid_strs: list[str],
    registry: SeriesRegistry | None,
):
    """Return ``uuid → EdgeMeta`` for the given uuids, populating the cache.

    Borrows a PG conn only on a cache miss.
    """
    if registry is None:
        with pool.connection() as conn:
            return fetch_edge_hierarchy_bulk(conn, edge_uuid_strs)

    hits, misses = registry.lookup_edges(edge_uuid_strs)
    if not misses:
        return hits
    with pool.connection() as conn:
        fetched = fetch_edge_hierarchy_bulk(conn, misses)
    for uid, meta in fetched.items():
        registry.insert_edge(uid, meta)
        hits[uid] = meta
    return hits
