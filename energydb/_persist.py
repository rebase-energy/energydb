"""Write helpers — node/edge upserts, series registration, tree walks.

These helpers all take an open ``conn`` and do **not** commit. The caller
controls the transaction boundary, which is how ``client.register_tree``
gets the whole structure walk persisted atomically. ``register_tree_under``
is structure-only (no timeseries data); manifest data writes go through
``_io.write_manifest``.

Identity is the EDM ``Element.id`` (UUID7). ``create_node`` uses
``ON CONFLICT (uuid) DO UPDATE`` so renames, moves, and property edits all
fall out of one statement. Edge endpoints are written straight into the FK
columns ``from_node_uuid`` / ``to_node_uuid`` — no path resolution at write
time.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import energydatamodel as edm
import polars as pl
from energydatamodel.reference import Reference
from timedatamodel import TimeSeries, TimeSeriesType

from energydb import series as series_mod
from energydb.diff import EdgeChange, EdgeSnapshot, NodeChange, NodeSnapshot, TreeDiff
from energydb.serialization import serialize_edge, serialize_node

# ---------------------------------------------------------------------------
# Node / edge persistence
# ---------------------------------------------------------------------------


def create_node(conn, edm_obj, *, parent_uuid: UUID | None) -> UUID:
    """Upsert one node under ``parent_uuid`` (or as a root if ``None``).

    Identity is ``edm_obj.id`` (UUID7). ``ON CONFLICT (uuid)`` covers
    renames, moves (``parent_uuid`` change), and property edits in a single
    statement. Type changes for an existing uuid are rejected — the
    conditional ``WHERE`` clause excludes the row, the row count comes back
    zero, and we surface a clear error.

    The conditional ``WHERE`` clause also skips the no-op UPDATE so
    ``updated_at`` doesn't churn on idempotent re-writes with identical
    content.

    Same name + a *different* node_type under the same parent (different
    uuid) raises via the ``UNIQUE (parent_uuid, name)`` constraint.
    """
    row_data = serialize_node(edm_obj)
    uuid_val: UUID = row_data["uuid"]
    node_type = row_data["node_type"]
    name = row_data["name"]
    data = row_data["data"]

    sql = """
        INSERT INTO energydb.node (uuid, node_type, name, parent_uuid, data)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (uuid) DO UPDATE
          SET name        = EXCLUDED.name,
              parent_uuid = EXCLUDED.parent_uuid,
              data        = EXCLUDED.data,
              updated_at  = now()
          WHERE energydb.node.node_type = EXCLUDED.node_type
            AND (energydb.node.name, energydb.node.parent_uuid, energydb.node.data)
                IS DISTINCT FROM
                (EXCLUDED.name, EXCLUDED.parent_uuid, EXCLUDED.data)
        RETURNING uuid
    """
    result = conn.execute(sql, (uuid_val, node_type, name, parent_uuid, data)).fetchone()

    if result is None:
        # ON CONFLICT skipped UPDATE either because (a) data was identical
        # (idempotent re-write — fine) or (b) node_type differs (illegal —
        # raise). Disambiguate by re-fetching the existing row.
        existing = conn.execute(
            "SELECT node_type FROM energydb.node WHERE uuid = %s",
            (uuid_val,),
        ).fetchone()
        if existing is None:
            raise RuntimeError("upsert returned no row but follow-up SELECT found nothing — concurrency bug")
        existing_type = existing[0]
        if existing_type != node_type:
            raise ValueError(
                f"Cannot persist {node_type}({name!r}) with id={uuid_val}: a "
                f"{existing_type} with the same id already exists. "
                f"Element type is immutable for a given id — register a new id."
            )

    register_node_descriptors(conn, uuid_val, edm_obj)
    return uuid_val


def create_edge(conn, edm_obj, *, tree_root: edm.Element | None = None) -> UUID:
    """Upsert one edge.

    Endpoint UUIDs come from ``edm_obj.from_element`` / ``to_element``
    (:class:`Reference`). When ``tree_root`` is provided, every endpoint
    UUID must be reachable in ``tree_root``'s :class:`Index` — cross-tree
    edges are rejected here. Pass ``tree_root=None`` for standalone edges
    where the caller has already verified endpoint existence.

    Identity is ``edm_obj.id``. ``ON CONFLICT (uuid)`` updates the row's
    payload, name, endpoints, and edge_type (the latter only on insert —
    PG wouldn't actually let you change it via the unique key, but we let
    the same-uuid update through cleanly).
    """
    row_data = serialize_edge(edm_obj)
    uuid_val: UUID = row_data["uuid"]

    from_uuid = _endpoint_uuid(edm_obj, "from_element", tree_root)
    to_uuid = _endpoint_uuid(edm_obj, "to_element", tree_root)

    row = conn.execute(
        """
        INSERT INTO energydb.edge (uuid, edge_type, name, from_node_uuid, to_node_uuid, data)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (uuid) DO UPDATE
          SET name           = EXCLUDED.name,
              from_node_uuid = EXCLUDED.from_node_uuid,
              to_node_uuid   = EXCLUDED.to_node_uuid,
              data           = EXCLUDED.data,
              updated_at     = now()
          WHERE energydb.edge.edge_type = EXCLUDED.edge_type
        RETURNING uuid
        """,
        (uuid_val, row_data["edge_type"], row_data["name"], from_uuid, to_uuid, row_data["data"]),
    ).fetchone()

    if row is None:
        existing = conn.execute(
            "SELECT edge_type FROM energydb.edge WHERE uuid = %s",
            (uuid_val,),
        ).fetchone()
        if existing is None:
            raise RuntimeError("edge upsert returned no row and follow-up SELECT found nothing")
        if existing[0] != row_data["edge_type"]:
            raise ValueError(
                f"Cannot persist {row_data['edge_type']!r} with id={uuid_val}: a "
                f"{existing[0]!r} with the same id already exists. "
                f"Edge type is immutable for a given id."
            )

    register_edge_descriptors(conn, uuid_val, edm_obj)
    return uuid_val


def _endpoint_uuid(edm_obj, attr: str, tree_root: edm.Element | None) -> UUID:
    """Pull a ``Reference`` off ``edm_obj.<attr>`` and return its uuid.

    When ``tree_root`` is given, the endpoint uuid must resolve against the
    tree's index — cross-tree edges are rejected. When ``tree_root`` is
    None, only the uuid value is required (caller is responsible for
    ensuring endpoint existence — typically by having created standalone
    nodes in a previous call).
    """
    ref = getattr(edm_obj, attr, None)
    if ref is None:
        raise ValueError(f"Edge {attr} is unset; cannot persist edge.")
    if not isinstance(ref, Reference):
        raise TypeError(f"Edge {attr} must be a Reference, got {type(ref).__name__}.")
    uuid_val = ref.id

    if tree_root is not None:
        index = tree_root.index()
        if uuid_val not in index:
            raise ValueError(
                f"Edge {attr} refers to {uuid_val} which is not in the tree rooted at "
                f"{type(tree_root).__name__}({getattr(tree_root, 'name', None)!r}). "
                f"Cross-tree edges are not supported."
            )
    return uuid_val


# ---------------------------------------------------------------------------
# Structure registration — tree walk
# ---------------------------------------------------------------------------


def register_tree_under(
    conn,
    edm_obj,
    *,
    parent_uuid: UUID | None,
    dry_run: bool = False,
) -> tuple[UUID, TreeDiff]:
    """Walk the EDM tree DFS, insert nodes/edges, register series.

    Structure-only. Caller manages the transaction. Raises if any
    ``TimeSeries`` on the tree has a non-empty df attached — write data
    separately via ``client.write(df, ...)``.

    Creates only. Raises :class:`ValueError` if any node or edge UUID in
    the payload already exists in the DB. To modify existing rows, use the
    scope mutators (:meth:`NodeScope.rename`, ``.update``, ``.delete``,
    ``.move_to``) or batch them with :meth:`Client.transaction`.

    ``dry_run=True`` returns the computed :class:`TreeDiff` without
    writing anything. The transaction is left open; callers should roll
    back or commit nothing.

    Returns a ``(root_uuid, TreeDiff)`` pair. The diff is always populated
    (empty in trivial / no-op cases).
    """
    _validate_no_inline_data(edm_obj)

    target_nodes, target_edges, root_uuid = _collect_target_state(edm_obj, parent_uuid)
    current_nodes = _fetch_nodes_by_uuids(conn, list(target_nodes.keys()))
    current_edges = _fetch_edges_by_uuids(conn, list(target_edges.keys()))

    if current_nodes or current_edges:
        existing_nodes = ", ".join(f"{s.node_type}({s.name!r})" for s in current_nodes.values())
        existing_edges = ", ".join(f"{s.edge_type}({s.name!r})" for s in current_edges.values())
        parts = []
        if existing_nodes:
            parts.append(f"node(s): {existing_nodes}")
        if existing_edges:
            parts.append(f"edge(s): {existing_edges}")
        raise ValueError(
            f"register_tree is create-only; the payload contains {len(current_nodes)} "
            f"node(s) and {len(current_edges)} edge(s) whose UUIDs already exist "
            f"({'; '.join(parts)}). To modify existing rows use scope mutators "
            f"(client.get_node(...).rename(), .update(), .delete(), .move_to()) "
            f"or batch them with client.transaction()."
        )

    diff = _compute_diff(target_nodes, current_nodes, target_edges, current_edges)
    _validate_no_type_changes(diff)

    if dry_run:
        return root_uuid, diff

    _apply_diff(conn, edm_obj, diff, target_nodes, target_edges)
    return root_uuid, diff


def _validate_no_type_changes(diff: TreeDiff) -> None:
    """Raise if any update changes the node_type / edge_type of an existing
    uuid. ``ON CONFLICT (uuid) WHERE node_type = EXCLUDED.node_type`` would
    otherwise just silently no-op the row — better to fail loudly here so
    the diff stays internally consistent.
    """
    for c in diff.node_updates:
        assert c.old is not None and c.new is not None
        if c.old.node_type != c.new.node_type:
            raise ValueError(
                f"Cannot change node_type for uuid={c.uuid}: "
                f"{c.old.node_type!r} → {c.new.node_type!r}. "
                f"Element type is immutable for a given id."
            )
    for c in diff.edge_updates:
        assert c.old is not None and c.new is not None
        if c.old.edge_type != c.new.edge_type:
            raise ValueError(f"Cannot change edge_type for uuid={c.uuid}: {c.old.edge_type!r} → {c.new.edge_type!r}.")


# ---------------------------------------------------------------------------
# Collect target state from the EDM tree
# ---------------------------------------------------------------------------


def _collect_target_state(
    edm_obj,
    parent_uuid: UUID | None,
) -> tuple[dict[UUID, NodeSnapshot], dict[UUID, EdgeSnapshot], UUID]:
    """Walk the EDM tree DFS; return (nodes, edges, root_uuid).

    Nodes are keyed by their UUID; same for edges. Each value is a
    :class:`NodeSnapshot` / :class:`EdgeSnapshot` ready to compare to the
    persisted state.
    """
    nodes: dict[UUID, NodeSnapshot] = {}
    edges: dict[UUID, EdgeSnapshot] = {}

    def _visit_nodes(obj, parent_ref: UUID | None):
        if isinstance(obj, edm.Edge):
            return  # edges handled in pass 2
        if obj.id in nodes:
            raise ValueError(
                f"Duplicate UUID {obj.id} on two distinct nodes in the tree. Each Element must have a unique id."
            )
        row = serialize_node(obj)
        nodes[obj.id] = NodeSnapshot(
            uuid=obj.id,
            node_type=row["node_type"],
            name=row["name"],
            parent_uuid=parent_ref,
            data=row["data"].obj,
        )
        for child in obj.children():
            _visit_nodes(child, obj.id)

    def _visit_edges(obj):
        for child in obj.children():
            if isinstance(child, edm.Edge):
                if child.id in edges:
                    raise ValueError(f"Duplicate UUID {child.id} on two distinct edges in the tree.")
                row = serialize_edge(child)
                from_uuid = _endpoint_uuid(child, "from_element", obj)
                to_uuid = _endpoint_uuid(child, "to_element", obj)
                # Cross-tree edge check: endpoints must resolve in target nodes.
                if from_uuid not in nodes:
                    raise ValueError(f"Edge {child.id} from_element {from_uuid} is not in the tree.")
                if to_uuid not in nodes:
                    raise ValueError(f"Edge {child.id} to_element {to_uuid} is not in the tree.")
                edges[child.id] = EdgeSnapshot(
                    uuid=child.id,
                    edge_type=row["edge_type"],
                    name=row["name"],
                    from_node_uuid=from_uuid,
                    to_node_uuid=to_uuid,
                    data=row["data"].obj,
                )
            else:
                _visit_edges(child)

    _visit_nodes(edm_obj, parent_uuid)
    _visit_edges(edm_obj)

    return nodes, edges, edm_obj.id


# ---------------------------------------------------------------------------
# Fetch current DB state
# ---------------------------------------------------------------------------


def _fetch_subtree_state(
    conn,
    root_uuid: UUID,
) -> tuple[dict[UUID, NodeSnapshot], dict[UUID, EdgeSnapshot]]:
    """Recursive CTE: fetch every node and edge under ``root_uuid``."""
    node_rows = conn.execute(
        """
        WITH RECURSIVE subtree AS (
            SELECT uuid, node_type, name, parent_uuid, data
            FROM energydb.node WHERE uuid = %s
            UNION ALL
            SELECT n.uuid, n.node_type, n.name, n.parent_uuid, n.data
            FROM energydb.node n JOIN subtree s ON n.parent_uuid = s.uuid
        ) CYCLE uuid SET _is_cycle USING _cycle_path
        SELECT uuid, node_type, name, parent_uuid, data FROM subtree
        WHERE NOT _is_cycle
        """,
        (root_uuid,),
    ).fetchall()
    nodes = {
        r[0]: NodeSnapshot(uuid=r[0], node_type=r[1], name=r[2], parent_uuid=r[3], data=dict(r[4] or {}))
        for r in node_rows
    }
    if not nodes:
        return nodes, {}

    edge_rows = conn.execute(
        "SELECT uuid, edge_type, name, from_node_uuid, to_node_uuid, data "
        "FROM energydb.edge "
        "WHERE from_node_uuid = ANY(%s) OR to_node_uuid = ANY(%s)",
        (list(nodes.keys()), list(nodes.keys())),
    ).fetchall()
    edges = {
        r[0]: EdgeSnapshot(
            uuid=r[0],
            edge_type=r[1],
            name=r[2],
            from_node_uuid=r[3],
            to_node_uuid=r[4],
            data=dict(r[5] or {}),
        )
        for r in edge_rows
    }
    return nodes, edges


def _fetch_nodes_by_uuids(conn, uuids: list[UUID]) -> dict[UUID, NodeSnapshot]:
    if not uuids:
        return {}
    rows = conn.execute(
        "SELECT uuid, node_type, name, parent_uuid, data FROM energydb.node WHERE uuid = ANY(%s)",
        (uuids,),
    ).fetchall()
    return {
        r[0]: NodeSnapshot(uuid=r[0], node_type=r[1], name=r[2], parent_uuid=r[3], data=dict(r[4] or {})) for r in rows
    }


def _fetch_edges_by_uuids(conn, uuids: list[UUID]) -> dict[UUID, EdgeSnapshot]:
    if not uuids:
        return {}
    rows = conn.execute(
        "SELECT uuid, edge_type, name, from_node_uuid, to_node_uuid, data FROM energydb.edge WHERE uuid = ANY(%s)",
        (uuids,),
    ).fetchall()
    return {
        r[0]: EdgeSnapshot(
            uuid=r[0],
            edge_type=r[1],
            name=r[2],
            from_node_uuid=r[3],
            to_node_uuid=r[4],
            data=dict(r[5] or {}),
        )
        for r in rows
    }


# ---------------------------------------------------------------------------
# Compute diff
# ---------------------------------------------------------------------------


def _compute_diff(
    target_nodes: dict[UUID, NodeSnapshot],
    current_nodes: dict[UUID, NodeSnapshot],
    target_edges: dict[UUID, EdgeSnapshot],
    current_edges: dict[UUID, EdgeSnapshot],
) -> TreeDiff:
    diff = TreeDiff()

    target_keys = set(target_nodes.keys())
    current_keys = set(current_nodes.keys())

    for uuid_val in target_keys - current_keys:
        diff.node_changes.append(NodeChange(old=None, new=target_nodes[uuid_val]))
    for uuid_val in target_keys & current_keys:
        old = current_nodes[uuid_val]
        new = target_nodes[uuid_val]
        if (old.name, old.parent_uuid, old.data, old.node_type) != (
            new.name,
            new.parent_uuid,
            new.data,
            new.node_type,
        ):
            diff.node_changes.append(NodeChange(old=old, new=new))

    target_edge_keys = set(target_edges.keys())
    current_edge_keys = set(current_edges.keys())

    for uuid_val in target_edge_keys - current_edge_keys:
        diff.edge_changes.append(EdgeChange(old=None, new=target_edges[uuid_val]))
    for uuid_val in target_edge_keys & current_edge_keys:
        old_e = current_edges[uuid_val]
        new_e = target_edges[uuid_val]
        if (
            old_e.name,
            old_e.from_node_uuid,
            old_e.to_node_uuid,
            old_e.data,
            old_e.edge_type,
        ) != (
            new_e.name,
            new_e.from_node_uuid,
            new_e.to_node_uuid,
            new_e.data,
            new_e.edge_type,
        ):
            diff.edge_changes.append(EdgeChange(old=old_e, new=new_e))

    return diff


# ---------------------------------------------------------------------------
# Apply diff
# ---------------------------------------------------------------------------


def _apply_diff(
    conn,
    edm_obj,
    diff: TreeDiff,
    target_nodes: dict[UUID, NodeSnapshot],
    target_edges: dict[UUID, EdgeSnapshot],
) -> None:
    """Apply the diff to the database (create-only).

    1. Insert nodes (in DFS order so parent_uuid FKs always resolve).
    2. Insert edges (endpoints now exist).

    Series declarations are registered alongside their owners during the
    insert walk via :func:`create_node` / :func:`create_edge`.
    """
    walked: set[UUID] = set()
    edm_objects_by_uuid = _index_edm_objects(edm_obj)

    def _walk_and_apply_nodes(obj):
        if isinstance(obj, edm.Edge):
            return
        if obj.id in target_nodes:
            create_node(conn, obj, parent_uuid=target_nodes[obj.id].parent_uuid)
            walked.add(obj.id)
        for child in obj.children():
            _walk_and_apply_nodes(child)

    _walk_and_apply_nodes(edm_obj)

    for change in diff.edge_changes:
        edge_obj = edm_objects_by_uuid.get(change.uuid)
        if edge_obj is None:
            raise RuntimeError(f"Edge {change.uuid} in diff has no corresponding EDM object.")
        create_edge(conn, edge_obj, tree_root=edm_obj)


def _index_edm_objects(edm_obj) -> dict[UUID, Any]:
    """``{uuid: edm_object}`` for every node and edge reachable in the tree."""
    out: dict[UUID, Any] = {}

    def _walk(obj):
        out[obj.id] = obj
        for child in obj.children():
            _walk(child)

    _walk(edm_obj)
    return out


def _validate_no_inline_data(edm_obj) -> None:
    """Raise if any node/edge in the tree carries non-empty TimeSeries data.

    ``register_tree()`` is structure-only — every ``TimeSeries`` on
    ``element.timeseries`` must be metadata-only (``df=None``). Data is
    written separately via :meth:`client.write`.
    """

    def _check(obj):
        ts_list = getattr(obj, "timeseries", None) or []
        for ts in ts_list:
            if ts.df is not None and ts.df.height > 0:
                obj_name = getattr(obj, "name", "<unnamed>")
                raise ValueError(
                    f"register_tree() received {obj_name!r} with inline timeseries data "
                    f"(name={ts.name!r}, rows={ts.df.height}). register_tree() is "
                    f"structure-only — write data separately with client.write(df)."
                )
        for child in obj.children():
            _check(child)

    _check(edm_obj)


# ---------------------------------------------------------------------------
# Series registration
# ---------------------------------------------------------------------------


def register_node_descriptors(conn, node_uuid: UUID, edm_obj) -> None:
    """Walk ``edm_obj.timeseries`` and register every entry on this node."""
    ts_list = getattr(edm_obj, "timeseries", None)
    if not ts_list:
        return
    for ts in ts_list:
        _register_one(conn, node_uuid=node_uuid, edge_uuid=None, ts=ts)


def register_edge_descriptors(conn, edge_uuid: UUID, edm_obj) -> None:
    ts_list = getattr(edm_obj, "timeseries", None)
    if not ts_list:
        return
    for ts in ts_list:
        _register_one(conn, node_uuid=None, edge_uuid=edge_uuid, ts=ts)


def _register_one(
    conn,
    *,
    node_uuid: UUID | None,
    edge_uuid: UUID | None,
    ts: TimeSeries,
) -> int:
    """Register one series row using ``series_mod.register_series``."""
    name = ts.name
    canonical_unit = ts.unit
    data_type = str(ts.data_type).lower() if ts.data_type is not None else None
    ts_type = ts.timeseries_type
    timeseries_type = ts_type.value if isinstance(ts_type, TimeSeriesType) else (str(ts_type) if ts_type else None)

    if name is None:
        raise ValueError("ts.name is required")
    if data_type is None:
        raise ValueError(f"ts.data_type is required for {name!r}")
    if timeseries_type is None:
        raise ValueError(f"ts.timeseries_type is required for {name!r} (FLAT | OVERLAPPING)")

    return series_mod.register_series(
        conn,
        node_uuid=node_uuid,
        edge_uuid=edge_uuid,
        data_type=data_type,
        name=name,
        canonical_unit=canonical_unit,
        timeseries_type=timeseries_type,
        description=ts.description,
    )


# ---------------------------------------------------------------------------
# Manifest unit conversion (used by the write pipeline)
# ---------------------------------------------------------------------------


def apply_manifest_unit_conversion(resolved: pl.DataFrame) -> pl.DataFrame:
    """Multiply ``value`` by per-series ``unit → canonical_unit`` factor.

    Operates on the unique (unit, canonical_unit) pairs so factor lookup runs
    once per pair rather than per row.
    """
    from energydb.units import compute_unit_factor

    pairs = resolved.select(["unit", "canonical_unit"]).unique()
    factor_rows: list[dict[str, Any]] = []
    for row in pairs.iter_rows(named=True):
        u = row["unit"]
        cu = row["canonical_unit"]
        f = compute_unit_factor(u, cu) if u else None
        factor_rows.append({"unit": u, "canonical_unit": cu, "_factor": float(f) if f is not None else 1.0})

    factor_df = pl.DataFrame(
        factor_rows,
        schema={"unit": pl.Utf8, "canonical_unit": pl.Utf8, "_factor": pl.Float64},
    )
    return (
        resolved.join(factor_df, on=["unit", "canonical_unit"], how="left")
        .with_columns((pl.col("value") * pl.col("_factor")).alias("value"))
        .drop("_factor")
    )
