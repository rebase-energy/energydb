"""Per-``Client`` metadata caches.

Three indexes share one :class:`SeriesRegistry`:

* **Series** — ``(owner_uuid, data_type, name) → SeriesMeta`` plus a reverse
  ``series_id`` index. Hot path for ``resolve_manifest``; the values are
  immutable except for ``timeseries_type`` (see below).
* **Nodes** — ``node_uuid → NodeMeta`` (``path``, ``name``, ``node_type``).
  Hot path for :func:`energydb._join.join_hierarchy`. A parent index lets
  ``evict_node_subtree`` walk descendants without a DB round-trip.
* **Edges** — ``edge_uuid → EdgeMeta``. Hot path for
  :func:`energydb._join.join_edge_hierarchy`; endpoint paths come back via
  the node index.

Successor to the historical ``timedb.db.series.SeriesRegistry`` (pre-energydb
split). Read-through fill, write-through invalidation:

* :func:`series.register_series` and node/edge ``delete`` paths update the
  series cache directly; renames, moves, and deletes additionally evict the
  node-/edge-cache entries they affect.

Cross-process staleness is **accepted and not corrected**. If another
``Client`` instance modifies ``energydb.series`` / ``energydb.node`` /
``energydb.edge``, this client may serve stale metadata until restart or
explicit :meth:`SeriesRegistry.clear`. The user-visible failure modes match
the historical contract:

1. Another process deletes the owning node/edge — this client keeps writing
   to a dead ``series_id``. Detectable via CH-side orphan rows.
2. Another process flips ``timeseries_type`` from FLAT to OVERLAPPING — this
   client skips the knowledge_time check in :func:`_io.write_manifest`.
3. Another process renames or moves a node — this client returns the old
   ``path`` / ``name`` from :func:`join_hierarchy` until eviction.

All three are recoverable via :meth:`Client.invalidate_series_cache`;
cross-process invalidation (PG NOTIFY / Redis) is the upgrade path if these
failure modes ever become real.
"""

from __future__ import annotations

from typing import NamedTuple


class SeriesMeta(NamedTuple):
    """Immutable metadata snapshot for one series."""

    series_id: int
    canonical_unit: str
    timeseries_type: str
    retention: str


class NodeMeta(NamedTuple):
    """Hierarchy snapshot for one node — what :func:`join_hierarchy` needs."""

    path: tuple[str, ...]
    name: str
    node_type: str


class EdgeMeta(NamedTuple):
    """Hierarchy snapshot for one edge — what :func:`join_edge_hierarchy` needs.

    Endpoint paths are not stored here; callers resolve them via the
    node-cache lookup on ``from_node_uuid`` / ``to_node_uuid``.
    """

    name: str | None
    edge_type: str
    from_node_uuid: str
    to_node_uuid: str


_Triple = tuple[str, str, str]


class SeriesRegistry:
    """Positive-only cache of series + node + edge metadata.

    Series lookups via :meth:`lookup_triples` are the hot resolve path; node
    and edge lookups via :meth:`lookup_nodes` / :meth:`lookup_edges` are the
    hot hierarchy-join path. In all three cases the caller is responsible
    for fetching misses and feeding them back through the corresponding
    ``insert_*`` method.

    Misses are *not* cached, so an entry registered later by another process
    becomes visible on the next attempt.
    """

    __slots__ = (
        "_by_owner",
        "_by_id",
        "_owner_index",
        "_nodes",
        "_node_parent",
        "_node_children",
        "_edges",
        "_hits",
        "_misses",
        "_node_hits",
        "_node_misses",
        "_edge_hits",
        "_edge_misses",
    )

    def __init__(self) -> None:
        self._by_owner: dict[_Triple, SeriesMeta] = {}
        self._by_id: dict[int, SeriesMeta] = {}
        self._owner_index: dict[str, set[tuple[str, str]]] = {}
        self._nodes: dict[str, NodeMeta] = {}
        self._node_parent: dict[str, str | None] = {}
        self._node_children: dict[str, set[str]] = {}
        self._edges: dict[str, EdgeMeta] = {}
        self._hits = 0
        self._misses = 0
        self._node_hits = 0
        self._node_misses = 0
        self._edge_hits = 0
        self._edge_misses = 0

    # ------------------------------------------------------------------
    # Series (triple → SeriesMeta)
    # ------------------------------------------------------------------

    def lookup_triples(
        self,
        triples: list[_Triple],
    ) -> tuple[dict[_Triple, SeriesMeta], list[_Triple]]:
        """Partition ``triples`` into cached hits and remaining misses.

        ``triples`` may contain duplicates; the returned ``misses`` list is
        deduplicated. Stats are updated for every input triple (a duplicate
        hit counts as N hits, matching how callers are billed).
        """
        hits: dict[_Triple, SeriesMeta] = {}
        misses_set: set[_Triple] = set()
        for t in triples:
            meta = self._by_owner.get(t)
            if meta is not None:
                hits[t] = meta
                self._hits += 1
            else:
                misses_set.add(t)
                self._misses += 1
        return hits, list(misses_set)

    def insert(
        self,
        owner_uuid: str,
        data_type: str,
        name: str,
        meta: SeriesMeta,
    ) -> None:
        """Add or overwrite one entry. Called by read-through fills and by
        write-through hooks (``register_series``).
        """
        key = (owner_uuid, data_type, name)
        self._by_owner[key] = meta
        self._by_id[meta.series_id] = meta
        self._owner_index.setdefault(owner_uuid, set()).add((data_type, name))

    def get_by_id(self, series_id: int) -> SeriesMeta | None:
        """Return cached meta for a ``series_id`` or ``None``."""
        return self._by_id.get(series_id)

    def evict_owner(self, owner_uuid: str) -> None:
        """Drop every series entry owned by ``owner_uuid``.

        Called from the ``delete`` paths on :class:`NodeScope` and
        :class:`EdgeScope` after the DB delete commits. The node-/edge-cache
        entries are evicted separately via :meth:`evict_node_subtree` /
        :meth:`evict_edge`.
        """
        triples = self._owner_index.pop(owner_uuid, None)
        if not triples:
            return
        for dt, name in triples:
            key = (owner_uuid, dt, name)
            meta = self._by_owner.pop(key, None)
            if meta is not None:
                self._by_id.pop(meta.series_id, None)

    # ------------------------------------------------------------------
    # Nodes (uuid → NodeMeta)
    # ------------------------------------------------------------------

    def lookup_nodes(
        self,
        node_uuids: list[str],
    ) -> tuple[dict[str, NodeMeta], list[str]]:
        """Partition ``node_uuids`` into cache hits and deduplicated misses."""
        hits: dict[str, NodeMeta] = {}
        misses_set: set[str] = set()
        for u in node_uuids:
            meta = self._nodes.get(u)
            if meta is not None:
                hits[u] = meta
                self._node_hits += 1
            else:
                misses_set.add(u)
                self._node_misses += 1
        return hits, list(misses_set)

    def insert_node(self, node_uuid: str, meta: NodeMeta, parent_uuid: str | None) -> None:
        """Add or overwrite one node entry; track the parent link for subtree eviction."""
        self._nodes[node_uuid] = meta
        # Unlink from previous parent if the node was already known.
        old_parent = self._node_parent.get(node_uuid)
        if old_parent is not None and old_parent != parent_uuid:
            children = self._node_children.get(old_parent)
            if children is not None:
                children.discard(node_uuid)
        self._node_parent[node_uuid] = parent_uuid
        if parent_uuid is not None:
            self._node_children.setdefault(parent_uuid, set()).add(node_uuid)

    def get_node(self, node_uuid: str) -> NodeMeta | None:
        return self._nodes.get(node_uuid)

    def evict_node_subtree(self, root_uuid: str) -> None:
        """Evict ``root_uuid`` and every descendant present in the cache.

        BFS over the in-memory parent index — no DB round-trip. Safe to call
        with an unknown uuid (no-op). Children not currently in the cache
        are simply absent from the index and require no action.
        """
        queue = [root_uuid]
        while queue:
            uid = queue.pop()
            children = self._node_children.pop(uid, None)
            if children:
                queue.extend(children)
            if uid in self._nodes:
                self._nodes.pop(uid, None)
                parent = self._node_parent.pop(uid, None)
                if parent is not None:
                    siblings = self._node_children.get(parent)
                    if siblings is not None:
                        siblings.discard(uid)
            else:
                # Even if uid itself wasn't cached as a NodeMeta, it may
                # have been recorded as a parent of cached children — clear
                # those bookkeeping entries too.
                self._node_parent.pop(uid, None)

    # ------------------------------------------------------------------
    # Edges (uuid → EdgeMeta)
    # ------------------------------------------------------------------

    def lookup_edges(
        self,
        edge_uuids: list[str],
    ) -> tuple[dict[str, EdgeMeta], list[str]]:
        hits: dict[str, EdgeMeta] = {}
        misses_set: set[str] = set()
        for u in edge_uuids:
            meta = self._edges.get(u)
            if meta is not None:
                hits[u] = meta
                self._edge_hits += 1
            else:
                misses_set.add(u)
                self._edge_misses += 1
        return hits, list(misses_set)

    def insert_edge(self, edge_uuid: str, meta: EdgeMeta) -> None:
        self._edges[edge_uuid] = meta

    def evict_edge(self, edge_uuid: str) -> None:
        self._edges.pop(edge_uuid, None)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def clear(self) -> None:
        """Drop every cached entry across all three indexes. Stats are reset."""
        self._by_owner.clear()
        self._by_id.clear()
        self._owner_index.clear()
        self._nodes.clear()
        self._node_parent.clear()
        self._node_children.clear()
        self._edges.clear()
        self._hits = 0
        self._misses = 0
        self._node_hits = 0
        self._node_misses = 0
        self._edge_hits = 0
        self._edge_misses = 0

    def stats(self) -> dict[str, int]:
        """Cumulative hits/misses plus current sizes across all indexes."""
        return {
            "hits": self._hits,
            "misses": self._misses,
            "size": len(self._by_owner),
            "node_hits": self._node_hits,
            "node_misses": self._node_misses,
            "node_size": len(self._nodes),
            "edge_hits": self._edge_hits,
            "edge_misses": self._edge_misses,
            "edge_size": len(self._edges),
        }


__all__ = ["EdgeMeta", "NodeMeta", "SeriesMeta", "SeriesRegistry"]
