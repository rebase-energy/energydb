"""Post-read hierarchy attachment: stamp ``path`` (and edge endpoints) onto a
timedb read result.

Polars-native. Every call borrows one PG connection and runs one combined
recursive CTE (per node-uuid set, per edge-uuid set) to fetch the joined
paths.

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

from typing import NamedTuple

import polars as pl

from energydb.paths import fetch_edge_hierarchy_bulk, fetch_node_hierarchy_bulk


class SeriesKey(NamedTuple):
    """Typed key for node-routed ``output="by_path"`` result dicts.

    Tuple-compatible: existing positional access
    (``result[("P/T01", "actual", "power")]``) keeps working. New code can
    use attribute access (``key.path``, ``key.data_type``, ``key.name``).
    """

    path: str
    data_type: str
    name: str


class EdgeSeriesKey(NamedTuple):
    """Typed key for edge-routed ``output="by_path"`` result dicts.

    Tuple-compatible. Holds the 5-element identity of an edge-attached
    series — both endpoint paths, the edge type, and the series's own
    ``(data_type, name)`` pair.
    """

    from_path: str
    to_path: str
    edge_type: str
    data_type: str
    name: str


def find(result: dict, **filters):
    """Partial-match filter over a by_path result dict.

    ``filters`` are attribute-name → value pairs matched against
    :class:`SeriesKey` / :class:`EdgeSeriesKey` fields. Returns a list of
    ``(key, df)`` tuples in the result's iteration order.

    ``edb.find(result, name="power")`` returns all series named ``"power"``
    regardless of path / data_type. Unknown attribute names match nothing.
    """
    out = []
    for key, df in result.items():
        if all(getattr(key, k, _MISSING) == v for k, v in filters.items()):
            out.append((key, df))
    return out


_MISSING = object()


def attach_node_hierarchy(pool, result: pl.DataFrame, meta: pl.DataFrame) -> pl.DataFrame:
    """Attach ``path`` (Utf8) to a node-routed read result.

    *meta* carries ``(series_id, node_uuid, data_type, name)`` from the
    resolved manifest. ``data_type`` and ``name`` are preserved on every
    row; ``path`` is broadcast from the bulk-fetched node hierarchy.
    ``series_id`` is dropped from the public result.
    """
    if result.is_empty() or meta.is_empty():
        return result

    node_uuid_strs = [u for u in meta["node_uuid"].to_list() if u is not None]
    if not node_uuid_strs:
        return result

    with pool.connection() as conn:
        node_path_map = fetch_node_hierarchy_bulk(conn, node_uuid_strs)

    sid_to_path = (
        meta.select(["series_id", "node_uuid", "data_type", "name"])
        .with_columns(
            pl.col("node_uuid").replace_strict(node_path_map, default=None).alias("path"),
        )
        .select(["series_id", "path", "data_type", "name"])
        .unique(subset=["series_id"])
    )
    return result.join(sid_to_path, on="series_id", how="left").drop("series_id")


def attach_edge_hierarchy(pool, result: pl.DataFrame, meta: pl.DataFrame) -> pl.DataFrame:
    """Attach ``from_path``, ``to_path``, ``edge_type`` to an edge-routed result.

    Endpoint paths come from the bulk node-hierarchy fetch on the edge's
    endpoint uuids. ``data_type`` / ``name`` are preserved from the
    manifest. ``series_id``, ``edge_uuid``, and the edge's own ``name``
    (intentionally distinct from series ``name``) are dropped from the
    public result.
    """
    if result.is_empty() or meta.is_empty():
        return result

    edge_uuid_strs = [u for u in meta["edge_uuid"].to_list() if u is not None]
    if not edge_uuid_strs:
        return result

    with pool.connection() as conn:
        edge_meta = fetch_edge_hierarchy_bulk(conn, edge_uuid_strs)
        endpoints = sorted({m[1] for m in edge_meta.values()} | {m[2] for m in edge_meta.values()})
        node_path_map = fetch_node_hierarchy_bulk(conn, endpoints)

    edge_lookup = pl.DataFrame(
        {
            "edge_uuid": list(edge_meta.keys()),
            "from_path": [node_path_map.get(m[1]) for m in edge_meta.values()],
            "to_path": [node_path_map.get(m[2]) for m in edge_meta.values()],
            "edge_type": [m[0] for m in edge_meta.values()],
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


def partition_node_by_path(pool, result: pl.DataFrame, meta: pl.DataFrame) -> dict[SeriesKey, pl.DataFrame]:
    """Partition a node-routed CH result into ``{SeriesKey: df}``.

    Skips the per-row broadcast that ``attach_node_hierarchy`` does. Each
    sub-frame carries only the CH data columns (``valid_time``, ``value``,
    plus opt-in time/audit columns) — ``series_id`` is dropped, and
    ``path`` / ``data_type`` / ``name`` live in the key, not the row.

    Keys are :class:`SeriesKey` NamedTuples — tuple-compatible for
    backwards-compat positional access, plus attribute access
    (``key.path``, ``key.data_type``, ``key.name``).

    Series that appear in the manifest but for which CH returned no rows
    get an empty sub-frame with the documented schema — callers can index
    by key without ``KeyError``.
    """
    if meta.is_empty():
        return {}

    node_uuid_strs = [u for u in meta["node_uuid"].to_list() if u is not None]
    with pool.connection() as conn:
        node_path_map = fetch_node_hierarchy_bulk(conn, node_uuid_strs)

    sid_to_key: dict[int, SeriesKey] = {}
    for row in meta.iter_rows(named=True):
        joined = node_path_map.get(row["node_uuid"])
        if joined is None:
            continue
        sid_to_key[row["series_id"]] = SeriesKey(joined, row["data_type"], row["name"])

    return _build_partition(result, sid_to_key)


def partition_edge_by_path(pool, result: pl.DataFrame, meta: pl.DataFrame) -> dict[EdgeSeriesKey, pl.DataFrame]:
    """Partition an edge-routed CH result into ``{EdgeSeriesKey: df}``.

    Same shape as :func:`partition_node_by_path` for the data side; the key
    is :class:`EdgeSeriesKey`, extended with the edge endpoint paths and
    ``edge_type``. Tuple-compatible for backwards-compat positional access.
    """
    if meta.is_empty():
        return {}

    edge_uuid_strs = [u for u in meta["edge_uuid"].to_list() if u is not None]
    with pool.connection() as conn:
        edge_meta = fetch_edge_hierarchy_bulk(conn, edge_uuid_strs)
        endpoints = sorted({m[1] for m in edge_meta.values()} | {m[2] for m in edge_meta.values()})
        node_path_map = fetch_node_hierarchy_bulk(conn, endpoints)

    sid_to_key: dict[int, EdgeSeriesKey] = {}
    for row in meta.iter_rows(named=True):
        em = edge_meta.get(row["edge_uuid"])
        if em is None:
            continue
        edge_type, from_uuid, to_uuid = em
        sid_to_key[row["series_id"]] = EdgeSeriesKey(
            node_path_map.get(from_uuid, ""),
            node_path_map.get(to_uuid, ""),
            edge_type,
            row["data_type"],
            row["name"],
        )

    return _build_partition(result, sid_to_key)


def _build_partition[KeyT: tuple](
    result: pl.DataFrame,
    sid_to_key: dict[int, KeyT],
) -> dict[KeyT, pl.DataFrame]:
    """Common partition assembly for ``partition_*_by_path``.

    Splits ``result`` by ``series_id`` (dropping that column from the
    sub-frames) and re-keys by the caller-supplied identity tuple. Series
    in ``sid_to_key`` that have no rows in ``result`` get an empty
    sub-frame with the CH-side data schema (minus ``series_id``).

    Generic over the key type — ``KeyT`` is bound to ``tuple`` so both
    :class:`SeriesKey` and :class:`EdgeSeriesKey` (NamedTuples) are
    accepted and the return type carries through.
    """
    data_schema = {c: dtype for c, dtype in result.schema.items() if c != "series_id"}

    out: dict[KeyT, pl.DataFrame] = {}
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
