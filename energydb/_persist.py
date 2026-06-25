"""Write helpers — node/edge inserts, series registration, tree walks.

These helpers all take an open ``conn`` and do **not** commit. The caller
controls the transaction boundary, which is how ``client.register_tree``
gets the whole structure walk persisted atomically. ``register_tree_under``
is structure-only (no timeseries data); manifest data writes go through
``_io.write_manifest``.

Identity is the EDM ``Element.id`` (UUID7). ``create_node`` is create-only
(``register_tree`` pre-validates that the uuid does not exist). Renames,
moves, and property edits go through the scope mutators on
:class:`NodeScope`. ``create_edge`` keeps an ``ON CONFLICT`` upsert because
it's exposed as :meth:`Client.create_edge` and documented as idempotent.
Edge endpoints are written straight into the FK columns
``from_node_uuid`` / ``to_node_uuid`` — no path resolution at write time.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

import energydatamodel as edm
import polars as pl
from energydatamodel.reference import Reference
from psycopg.types.json import Jsonb
from timedatamodel import TimeSeries, TimeSeriesType
from uuid6 import uuid7

from energydb import series as series_mod
from energydb.diff import EdgeChange, EdgeSnapshot, NodeChange, NodeSnapshot, TreeDiff
from energydb.serialization import serialize_edge, serialize_node
from energydb.series import validate_name
from energydb.units import compute_unit_factor

# ---------------------------------------------------------------------------
# Node / edge persistence
# ---------------------------------------------------------------------------


def create_node(
    conn,
    edm_obj,
    *,
    parent_uuid: UUID | None,
) -> UUID:
    """Insert one node under ``parent_uuid`` (or as a root if ``None``).

    Caller (``_apply_diff``) is create-only and has already verified the
    uuid does not exist, so a plain ``INSERT`` is enough. A colliding
    ``(parent_uuid, name)`` pair surfaces as a uniqueness error from the
    DB — kept implicit because the diff path can't reasonably preempt it
    without a separate read round-trip.
    """
    row_data = serialize_node(edm_obj)
    uuid_val: UUID = row_data["uuid"]
    validate_name(row_data["name"], kind="node")

    # Materialize ``path`` from the parent. For ``register_tree``'s DFS walk
    # we could thread the parent's path through the recursion to skip this
    # SELECT; deferred — the extra round-trip is cheap and the call sites
    # would need a wider signature.
    if parent_uuid is None:
        path = row_data["name"]
    else:
        parent_row = conn.execute(
            "SELECT path FROM node WHERE uuid = %s",
            (parent_uuid,),
        ).fetchone()
        if parent_row is None:
            raise ValueError(f"parent_uuid={parent_uuid} does not exist")
        path = f"{parent_row[0]}/{row_data['name']}"

    conn.execute(
        "INSERT INTO node (uuid, node_type, name, parent_uuid, path, data) VALUES (%s, %s, %s, %s, %s, %s)",
        (uuid_val, row_data["node_type"], row_data["name"], parent_uuid, path, row_data["data"]),
    )

    _register_descriptors(conn, owner_col="node_uuid", owner_uuid=uuid_val, edm_obj=edm_obj)
    return uuid_val


def create_node_raw(
    conn,
    *,
    node_type: str,
    name: str,
    data: dict | None = None,
    parent_uuid: UUID | None,
    uuid: UUID | None = None,
) -> UUID:
    """Insert one node from a type slug + ``data`` dict — no EDM object.

    Generic, EDM-free counterpart to :func:`create_node`: ``node_type`` is
    stored verbatim and the caller's ``data`` becomes the JSONB blob. Mints a
    uuid7 when ``uuid`` is omitted, validates ``name``, and materializes
    ``path`` from the parent. Caller controls the transaction (no commit). A
    colliding ``(parent_uuid, name)`` pair surfaces as a DB uniqueness error.
    """
    uuid_val: UUID = uuid if uuid is not None else uuid7()
    validate_name(name, kind="node")

    if parent_uuid is None:
        path = name
    else:
        parent_row = conn.execute(
            "SELECT path FROM node WHERE uuid = %s",
            (parent_uuid,),
        ).fetchone()
        if parent_row is None:
            raise ValueError(f"parent_uuid={parent_uuid} does not exist")
        path = f"{parent_row[0]}/{name}"

    conn.execute(
        "INSERT INTO node (uuid, node_type, name, parent_uuid, path, data) VALUES (%s, %s, %s, %s, %s, %s)",
        (uuid_val, node_type, name, parent_uuid, path, Jsonb(data or {})),
    )
    return uuid_val


def create_edge(
    conn,
    edm_obj,
    *,
    tree_root: edm.Element | None = None,
) -> UUID:
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
    if row_data["name"] is not None:
        validate_name(row_data["name"], kind="edge")

    from_uuid = _endpoint_uuid(edm_obj, "from_element", tree_root)
    to_uuid = _endpoint_uuid(edm_obj, "to_element", tree_root)

    row = conn.execute(
        """
        INSERT INTO edge (uuid, edge_type, name, from_node_uuid, to_node_uuid, data)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (uuid) DO UPDATE
          SET name           = EXCLUDED.name,
              from_node_uuid = EXCLUDED.from_node_uuid,
              to_node_uuid   = EXCLUDED.to_node_uuid,
              data           = EXCLUDED.data,
              updated_at     = now()
          WHERE edge.edge_type = EXCLUDED.edge_type
        RETURNING uuid
        """,
        (uuid_val, row_data["edge_type"], row_data["name"], from_uuid, to_uuid, row_data["data"]),
    ).fetchone()

    if row is None:
        existing = conn.execute(
            "SELECT edge_type FROM edge WHERE uuid = %s",
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

    _register_descriptors(conn, owner_col="edge_uuid", owner_uuid=uuid_val, edm_obj=edm_obj)
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

    target_nodes, target_edges, node_objs, edge_objs, root_uuid = _collect_target_state(edm_obj, parent_uuid)
    existing_node_uuids = _existing_uuids(conn, "node", list(target_nodes.keys()))
    existing_edge_uuids = _existing_uuids(conn, "edge", list(target_edges.keys()))

    if existing_node_uuids or existing_edge_uuids:
        parts = []
        if existing_node_uuids:
            parts.append(f"node uuid(s): {', '.join(str(u) for u in existing_node_uuids)}")
        if existing_edge_uuids:
            parts.append(f"edge uuid(s): {', '.join(str(u) for u in existing_edge_uuids)}")
        raise ValueError(
            f"register_tree is create-only; the payload contains "
            f"{len(existing_node_uuids)} node(s) and {len(existing_edge_uuids)} "
            f"edge(s) whose UUIDs already exist ({'; '.join(parts)}). "
            f"To modify existing rows use scope mutators "
            f"(client.get_node(...).rename(), .update(), .delete(), .move_to()) "
            f"or batch them with client.transaction()."
        )

    # Create-only path: every target row is an insert. No need for an
    # update/delete branch — the existing-uuid pre-check above raises.
    diff = TreeDiff(
        node_changes=[NodeChange(old=None, new=s) for s in target_nodes.values()],
        edge_changes=[EdgeChange(old=None, new=s) for s in target_edges.values()],
    )

    if dry_run:
        return root_uuid, diff

    # node_objs is in DFS order (parent-before-child) — exactly what the
    # parent_uuid FK chain needs. Edges go second once their endpoints exist.
    # Series declarations attached to each owner are registered as a side
    # effect of create_node / create_edge.
    for uid, obj in node_objs.items():
        create_node(conn, obj, parent_uuid=target_nodes[uid].parent_uuid)
    for obj in edge_objs.values():
        create_edge(conn, obj, tree_root=edm_obj)
    return root_uuid, diff


# ---------------------------------------------------------------------------
# Collect target state from the EDM tree
# ---------------------------------------------------------------------------


def _collect_target_state(
    edm_obj,
    parent_uuid: UUID | None,
) -> tuple[
    dict[UUID, NodeSnapshot],
    dict[UUID, EdgeSnapshot],
    dict[UUID, Any],
    dict[UUID, Any],
    UUID,
]:
    """Walk the EDM tree once; collect node/edge snapshots and EDM-object refs.

    Returns ``(node_snaps, edge_snaps, node_objs, edge_objs, root_uuid)``.
    ``node_objs`` is in DFS order so iterating it satisfies parent-before-
    child for FK resolution. ``edge_objs`` is filled in a second linear
    pass over the queued edges, after every node has been seen — that's
    the only way to validate that edge endpoints aren't cross-tree.
    """
    node_snaps: dict[UUID, NodeSnapshot] = {}
    node_objs: dict[UUID, Any] = {}
    edge_queue: list[Any] = []

    def _visit(obj, parent_ref: UUID | None) -> None:
        if isinstance(obj, edm.Edge):
            edge_queue.append(obj)
            return
        if obj.id in node_snaps:
            raise ValueError(
                f"Duplicate UUID {obj.id} on two distinct nodes in the tree. Each Element must have a unique id."
            )
        row = serialize_node(obj)
        node_snaps[obj.id] = NodeSnapshot(
            uuid=obj.id,
            node_type=row["node_type"],
            name=row["name"],
            parent_uuid=parent_ref,
            data=row["data"].obj,
        )
        node_objs[obj.id] = obj
        for child in obj.children():
            _visit(child, obj.id)

    _visit(edm_obj, parent_uuid)

    edge_snaps: dict[UUID, EdgeSnapshot] = {}
    edge_objs: dict[UUID, Any] = {}
    for child in edge_queue:
        if child.id in edge_snaps:
            raise ValueError(f"Duplicate UUID {child.id} on two distinct edges in the tree.")
        row = serialize_edge(child)
        from_uuid = _endpoint_uuid(child, "from_element", edm_obj)
        to_uuid = _endpoint_uuid(child, "to_element", edm_obj)
        if from_uuid not in node_snaps:
            raise ValueError(f"Edge {child.id} from_element {from_uuid} is not in the tree.")
        if to_uuid not in node_snaps:
            raise ValueError(f"Edge {child.id} to_element {to_uuid} is not in the tree.")
        edge_snaps[child.id] = EdgeSnapshot(
            uuid=child.id,
            edge_type=row["edge_type"],
            name=row["name"],
            from_node_uuid=from_uuid,
            to_node_uuid=to_uuid,
            data=row["data"].obj,
        )
        edge_objs[child.id] = child

    return node_snaps, edge_snaps, node_objs, edge_objs, edm_obj.id


# ---------------------------------------------------------------------------
# Fetch current DB state
# ---------------------------------------------------------------------------


def _existing_uuids(conn, table: str, uuids: list[UUID]) -> list[UUID]:
    """Return the subset of ``uuids`` that exist in ``{table}``.

    Lighter than ``_fetch_nodes_by_uuids`` / ``_fetch_edges_by_uuids``
    when the caller only needs to know *whether* the rows exist (e.g. the
    create-only pre-check on ``register_tree``). ``table`` is interpolated
    into the SQL and must be one of the trusted internal values
    ``"node"`` / ``"edge"``.
    """
    if not uuids:
        return []
    rows = conn.execute(
        f"SELECT uuid FROM {table} WHERE uuid = ANY(%s)",
        (uuids,),
    ).fetchall()
    return [r[0] for r in rows]


def _fetch_nodes_by_uuids(conn, uuids: list[UUID]) -> dict[UUID, NodeSnapshot]:
    if not uuids:
        return {}
    rows = conn.execute(
        "SELECT uuid, node_type, name, parent_uuid, data FROM node WHERE uuid = ANY(%s)",
        (uuids,),
    ).fetchall()
    return {
        r[0]: NodeSnapshot(uuid=r[0], node_type=r[1], name=r[2], parent_uuid=r[3], data=dict(r[4] or {})) for r in rows
    }


def _fetch_edges_by_uuids(conn, uuids: list[UUID]) -> dict[UUID, EdgeSnapshot]:
    if not uuids:
        return {}
    rows = conn.execute(
        "SELECT uuid, edge_type, name, from_node_uuid, to_node_uuid, data FROM edge WHERE uuid = ANY(%s)",
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


def _register_descriptors(
    conn,
    *,
    owner_col: Literal["node_uuid", "edge_uuid"],
    owner_uuid: UUID,
    edm_obj,
) -> None:
    """Walk ``edm_obj.timeseries`` and register every entry on this owner."""
    for ts in getattr(edm_obj, "timeseries", None) or []:
        _register_one(conn, owner_col=owner_col, owner_uuid=owner_uuid, ts=ts)


def _register_one(
    conn,
    *,
    owner_col: Literal["node_uuid", "edge_uuid"],
    owner_uuid: UUID,
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
        owner_col=owner_col,
        owner_uuid=owner_uuid,
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
