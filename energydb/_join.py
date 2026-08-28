"""Post-read hierarchy attachment: stamp ``path`` (and edge endpoints) onto a
timedb read result.

Polars-native. Path data rides along on the resolve-step meta frame (one
PG round-trip already paid by the resolve), so the attach step is pure
Python, with no extra PG calls.

Output column contract:

* Node-routed reads: ``path: Utf8`` (joined with ``/``), plus ``data_type``
  and ``name`` carried through from the manifest.
* Edge-routed reads: ``from_path: Utf8``, ``to_path: Utf8``, ``edge_type``,
  ``edge_name`` (the edge's own name, null for an unnamed edge; it is what
  tells parallel edges apart), plus ``data_type`` / ``name``.

Internal identifiers (``series_id``, ``node_uuid``, ``edge_uuid``,
``node_type``, etc.) are NOT exposed on the result; callers identify series
by ``(path, data_type, name)`` (or edge equivalent).
"""

from __future__ import annotations

from typing import NamedTuple

import polars as pl

# Trailing identity columns hstacked onto an empty read result; order must match
# the columns attach_node_hierarchy / attach_edge_hierarchy add via their join.
NODE_IDENTITY_COLUMNS: tuple[str, ...] = ("path", "data_type", "name")
EDGE_IDENTITY_COLUMNS: tuple[str, ...] = ("from_path", "to_path", "edge_type", "edge_name", "data_type", "name")


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

    Tuple-compatible. Holds the 6-element identity of an edge-attached
    series, both endpoint paths, the edge type, the edge's own ``name``
    (``None`` for an unnamed edge), and the series's own ``(data_type,
    name)`` pair.

    ``edge_name`` sits fourth, next to ``edge_type``, so the key reads as
    edge-identity-then-series-identity. It is what keeps two parallel
    circuits' series apart: without it they would collide on one key.

    .. versionchanged:: 0.11.0
       Gained ``edge_name`` (5 → 6 fields). Positional unpackers of the old
       5-tuple break loudly; keyword/attribute access is unaffected.
    """

    from_path: str
    to_path: str
    edge_type: str
    edge_name: str | None
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


def attach_node_hierarchy(client, result: pl.DataFrame, meta: pl.DataFrame) -> pl.DataFrame:
    """Attach ``path`` (Utf8) to a node-routed read result.

    *meta* carries ``(series_id, path, data_type, name)`` from the resolve
    step, ``path`` rode along on the JOIN that resolved series, so no
    extra PG round-trip is needed. ``data_type`` / ``name`` are preserved
    on every row; ``series_id`` is dropped from the public result.

    ``client`` is unused (left in the signature for symmetry with the edge
    variant and to keep call sites stable).
    """
    del client  # no PG round-trip needed; path rides on meta
    if result.is_empty() or meta.is_empty():
        return result

    sid_to_path = meta.select(["series_id", "path", "data_type", "name"]).unique(subset=["series_id"])
    return result.join(sid_to_path, on="series_id", how="left").drop("series_id")


def attach_edge_hierarchy(client, result: pl.DataFrame, meta: pl.DataFrame) -> pl.DataFrame:
    """Attach ``from_path``, ``to_path``, ``edge_type``, ``edge_name`` to an edge-routed result.

    *meta* carries the endpoint paths, edge type and edge name from the
    resolve step's JOIN through edge → from/to nodes, so no extra PG fetch is
    needed. ``data_type`` / ``name`` are preserved from the manifest.
    ``series_id`` and ``edge_uuid`` are dropped from the public result.

    The edge's own name surfaces as ``edge_name`` (never as ``name``, which
    stays the series name): two parallel circuits are otherwise
    indistinguishable rows.
    """
    del client  # no PG round-trip needed; endpoint paths ride on meta
    if result.is_empty() or meta.is_empty():
        return result

    sid_lookup = meta.select(
        ["series_id", "from_path", "to_path", "edge_type", "edge_name", "data_type", "name"]
    ).unique(subset=["series_id"])
    return result.join(sid_lookup, on="series_id", how="left").drop("series_id")


def partition_node_by_path(client, result: pl.DataFrame, meta: pl.DataFrame) -> dict[SeriesKey, pl.DataFrame]:
    """Partition a node-routed CH result into ``{SeriesKey: df}``.

    Skips the per-row broadcast that ``attach_node_hierarchy`` does. Each
    sub-frame carries only the CH data columns (``valid_time``, ``value``,
    plus opt-in time/audit columns), ``series_id`` is dropped, and
    ``path`` / ``data_type`` / ``name`` live in the key, not the row.

    Keys are :class:`SeriesKey` NamedTuples: tuple-compatible for
    backwards-compat positional access, plus attribute access
    (``key.path``, ``key.data_type``, ``key.name``).

    Series that appear in the manifest but for which CH returned no rows
    get an empty sub-frame with the documented schema, callers can index
    by key without ``KeyError``.
    """
    del client  # no PG round-trip needed; path rides on meta
    if meta.is_empty():
        return {}

    sid_to_key: dict[int, SeriesKey] = {}
    for row in meta.iter_rows(named=True):
        joined = row.get("path")
        if joined is None:
            continue
        sid_to_key[row["series_id"]] = SeriesKey(joined, row["data_type"], row["name"])

    return _build_partition(result, sid_to_key)


def partition_edge_by_path(client, result: pl.DataFrame, meta: pl.DataFrame) -> dict[EdgeSeriesKey, pl.DataFrame]:
    """Partition an edge-routed CH result into ``{EdgeSeriesKey: df}``.

    Same shape as :func:`partition_node_by_path` for the data side; the key
    is :class:`EdgeSeriesKey`, extended with the edge endpoint paths,
    ``edge_type`` and ``edge_name``. ``edge_name`` is part of the key
    precisely so parallel edges' series land in separate entries instead of
    silently overwriting one another.
    """
    del client  # no PG round-trip needed; endpoint paths ride on meta
    if meta.is_empty():
        return {}

    sid_to_key: dict[int, EdgeSeriesKey] = {}
    for row in meta.iter_rows(named=True):
        from_path = row.get("from_path")
        to_path = row.get("to_path")
        edge_type = row.get("edge_type")
        if from_path is None or to_path is None or edge_type is None:
            continue
        sid_to_key[row["series_id"]] = EdgeSeriesKey(
            from_path,
            to_path,
            edge_type,
            row.get("edge_name"),
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

    Generic over the key type: ``KeyT`` is bound to ``tuple`` so both
    :class:`SeriesKey` and :class:`EdgeSeriesKey` (NamedTuples) are
    accepted and the return type carries through.
    """
    data_schema = {c: dtype for c, dtype in result.schema.items() if c != "series_id"}

    out: dict[KeyT, pl.DataFrame] = {}
    if not result.is_empty():
        parts = result.partition_by("series_id", as_dict=True, include_key=False)
        # parts is keyed by a tuple of partition values, here (series_id,).
        for k_tuple, sub in parts.items():
            sid = k_tuple[0]
            key = sid_to_key.get(sid)
            if key is not None:
                out[key] = sub

    for key in sid_to_key.values():
        if key not in out:
            out[key] = pl.DataFrame(schema=data_schema)
    return out
