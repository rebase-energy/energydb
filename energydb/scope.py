"""NodeScope and EdgeScope — fluent APIs for navigating and operating on
a single node or edge.

Scope is for **exploration** (navigation, listings) and **single-element
read/write** (one timeseries on this node, property updates, deletes).
Tree / structure mutation goes through ``client.register_tree`` directly.

A node is identified by its ``uuid`` (UUID7); the path form
``client.get_node("Europe", "Sweden")`` is sugar that resolves to a uuid
via one indexed recursive CTE on ``(parent_uuid, name)``. An edge is
identified by its ``uuid`` (or by the ``(from_path, to_path, edge_type)``
triple). ``.get_node()`` / ``.where()`` are lazy: they accumulate path
and filters without hitting the DB. Terminal operations (``.read()``,
``.write()``, ``.children()``, ``.get()``, ...) trigger one indexed
resolution query and execute.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

import pandas as pd
import polars as pl
from psycopg.types.json import Jsonb
from timedatamodel import TimeSeries, TimeSeriesType

from energydb import series as series_mod
from energydb._frames import OutputType, to_output, to_polars
from energydb._persist import _fetch_edges_by_uuids, _fetch_nodes_by_uuids, register_tree_under
from energydb.diff import EdgeChange, NodeChange, TreeDiff
from energydb.paths import (
    Path,
    resolve_edge_uuid,
    resolve_node_uuid,
    resolve_path,
    resolve_subtree_uuids,
)
from energydb.serialization import reconstruct_edge, reconstruct_node

if TYPE_CHECKING:
    from energydb._transaction import Transaction
    from energydb.client import Client


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _dry_run_unsupported_in_txn() -> None:
    raise ValueError("dry_run is not supported inside a transaction(); use txn.preview() instead.")


def _ts_io_unsupported_in_txn(op: str) -> None:
    """Reject time-series I/O on a txn-bound scope.

    ``scope.write`` / ``scope.read`` route through the connection pool and
    (for writes) ClickHouse, neither of which participates in the PG
    transaction. Allowing them silently would let a successful ``write``
    persist data that a later rollback couldn't undo. Call
    ``client.write``/``client.read`` directly outside the transaction.
    """
    raise RuntimeError(
        f"scope.{op}() is not supported inside a transaction(); time-series I/O does "
        f"not participate in the PG transaction. Call client.{op}() directly outside "
        f"the transaction block."
    )


def _coerce_path(args: tuple, kwarg: Path | list[str] | str | None = None) -> Path:
    """Accept variadic names, a single tuple/list, or a kwarg form.

    ``_coerce_path(("A", "B", "C"))``    → ``("A", "B", "C")``
    ``_coerce_path((("A", "B"),))``      → ``("A", "B")``
    ``_coerce_path(([..."A","B"],))``    → ``("A", "B")``
    """
    if kwarg is not None:
        if isinstance(kwarg, str):
            return (kwarg,)
        return tuple(kwarg)
    if len(args) == 1 and isinstance(args[0], (tuple, list)):
        return tuple(args[0])
    return tuple(args)


def _resolve_endpoint(conn, target: NodeScope | Path | list[str]) -> UUID:
    """Resolve a node endpoint reference to a UUID against ``conn``."""
    if isinstance(target, NodeScope):
        return target._resolve_node_uuid(conn)
    path = tuple(target)
    if not path:
        raise ValueError("Endpoint path cannot be empty.")
    return resolve_node_uuid(conn, path)


def _timeseries_type_from_ts(ts: TimeSeries) -> str | None:
    """Extract timeseries_type from a TimeSeries as 'FLAT' or 'OVERLAPPING'."""
    ts_type = ts.timeseries_type
    if ts_type is None:
        return None
    return ts_type.value if isinstance(ts_type, TimeSeriesType) else str(ts_type)


def _normalize_series_register_args(
    ts_or_name: TimeSeries | str | None,
    *,
    name: str | None,
    canonical_unit: str | None,
    data_type: str | None,
    timeseries_type: str | None,
    description: str | None,
) -> dict[str, Any]:
    """Normalize ``register_series`` inputs to the kwargs ``series_mod`` expects.

    Pulls metadata off a :class:`TimeSeries` when one is supplied, otherwise
    takes the explicit kwargs. Raises if any required field is still missing.
    """
    if isinstance(ts_or_name, TimeSeries):
        ts = ts_or_name
        name = name or ts.name
        canonical_unit = canonical_unit or ts.unit
        if data_type is None and ts.data_type is not None:
            data_type = str(ts.data_type).lower()
        if timeseries_type is None:
            timeseries_type = _timeseries_type_from_ts(ts)
        description = description or ts.description
    elif isinstance(ts_or_name, str):
        name = ts_or_name

    if name is None:
        raise ValueError("name is required")
    if data_type is None:
        raise ValueError("data_type is required")
    if canonical_unit is None:
        raise ValueError("canonical_unit is required")
    if timeseries_type is None:
        raise ValueError("timeseries_type is required (FLAT | OVERLAPPING)")

    return {
        "data_type": str(data_type).lower(),
        "name": name,
        "canonical_unit": canonical_unit,
        "timeseries_type": timeseries_type,
        "description": description,
    }


def _attach_routing(
    df: pl.DataFrame,
    *,
    owner_col: str,
    owner_val: UUID,
    data_type: str,
    name: str,
    unit: str | None,
) -> pl.DataFrame:
    """Attach the routing columns required by the manifest pipeline.

    ``owner_col`` is one of ``"node_uuid"`` / ``"edge_uuid"``. UUIDs are
    serialized as strings on the manifest so polars-side joins work cleanly.
    """
    cols = [
        pl.lit(str(owner_val), dtype=pl.Utf8).alias(owner_col),
        pl.lit(str(data_type).lower(), dtype=pl.Utf8).alias("data_type"),
        pl.lit(name, dtype=pl.Utf8).alias("name"),
    ]
    if unit is not None:
        cols.append(pl.lit(unit, dtype=pl.Utf8).alias("unit"))
    return df.with_columns(cols)


# ---------------------------------------------------------------------------
# _BaseScope — shared plumbing for NodeScope and EdgeScope.
# ---------------------------------------------------------------------------


class _BaseScope:
    """Shared connection / mutation plumbing.

    Subclasses plug in their identity by implementing the small set of
    abstract methods below; everything else (connection borrowing, txn
    routing, the 8-step mutator boilerplate) lives here.
    """

    _client: Client
    _txn: Transaction | None

    # -- shared properties / connection management ---------------------

    @property
    def _pool(self):
        return self._client._pool

    @property
    def _td(self):
        return self._client.td

    @contextmanager
    def _use_conn(self):
        """Yield a DB connection. Inside a txn, use the txn's connection
        (caller MUST NOT call ``.commit()`` / ``.rollback()``). Otherwise
        borrow from the pool; mutators are responsible for explicit
        ``commit()`` or ``rollback()``.
        """
        if self._txn is not None:
            yield self._txn._conn
        else:
            with self._pool.connection() as conn:
                yield conn

    # -- subclass contract (overridden in NodeScope / EdgeScope) -------

    _owner_col: str  # "node_uuid" or "edge_uuid" — see series_mod.register_series

    def _resolve_uuid(self, conn) -> UUID:
        raise NotImplementedError

    def _fetch_snapshot(self, conn, uuid_: UUID):
        raise NotImplementedError

    def _record_to_txn(self, before, after) -> None:
        raise NotImplementedError

    def _wrap_in_diff(self, before, after) -> TreeDiff:
        raise NotImplementedError

    def _not_found_msg(self, uuid_: UUID) -> str:
        raise NotImplementedError

    def _build_read_manifest(self, *, data_type: str | None, name: str | None) -> pl.DataFrame | None:
        """Subclass-specific: resolve scope's targets to a routing manifest."""
        raise NotImplementedError

    # -- shared mutation machinery -------------------------------------

    def _apply_mutation(
        self,
        exec_fn: Callable[[Any, UUID], None],
        *,
        dry_run: bool,
        fetch_after: bool = True,
    ) -> TreeDiff | None:
        """Run a single mutating statement against this scope's target.

        ``exec_fn(conn, uuid_)`` runs after the pre-mutation snapshot is
        captured; it may execute arbitrary additional queries (e.g. cycle
        checks, endpoint resolution) before the actual UPDATE/DELETE.
        ``fetch_after=False`` is for deletes — the post-state record is
        ``None``.

        Behavior matches the pre-refactor mutators exactly: txn-bound
        scopes record (before, after) on the txn and return ``None``;
        dry-run scopes roll back and return a :class:`TreeDiff`; plain
        scopes commit and return ``None``.
        """
        if dry_run and self._txn is not None:
            _dry_run_unsupported_in_txn()
        with self._use_conn() as conn:
            uuid_ = self._resolve_uuid(conn)
            before = self._fetch_snapshot(conn, uuid_)
            if before is None:
                raise ValueError(self._not_found_msg(uuid_))
            exec_fn(conn, uuid_)
            after = self._fetch_snapshot(conn, uuid_) if fetch_after else None
            if self._txn is not None:
                self._record_to_txn(before, after)
                return None
            if dry_run:
                conn.rollback()
                return self._wrap_in_diff(before, after)
            conn.commit()
        return None

    # -- shared series + timeseries I/O --------------------------------

    def register_series(
        self,
        ts_or_name: TimeSeries | str | None = None,
        *,
        name: str | None = None,
        canonical_unit: str | None = None,
        data_type: str | None = None,
        timeseries_type: str | None = None,
        retention: str | None = None,
        description: str | None = None,
    ) -> int:
        """Register a time series on this scope's owner (node or edge).

        Accepts a ``TimeSeries`` (metadata extracted) or explicit kwargs.
        When ``retention`` is omitted it is derived from
        ``timeseries_type``: ``FLAT`` (actuals) → ``'forever'``,
        ``OVERLAPPING`` (forecasts) → ``'medium'``.
        """
        args = _normalize_series_register_args(
            ts_or_name,
            name=name,
            canonical_unit=canonical_unit,
            data_type=data_type,
            timeseries_type=timeseries_type,
            description=description,
        )
        with self._use_conn() as conn:
            owner_uuid = self._resolve_uuid(conn)
            node_uuid = owner_uuid if self._owner_col == "node_uuid" else None
            edge_uuid = owner_uuid if self._owner_col == "edge_uuid" else None
            sid = series_mod.register_series(
                conn,
                node_uuid=node_uuid,
                edge_uuid=edge_uuid,
                retention=retention,
                **args,
            )
            if self._txn is None:
                conn.commit()
        return sid

    def write(
        self,
        df: pl.DataFrame | pd.DataFrame,
        *,
        data_type: str,
        name: str,
        unit: str | None = None,
        knowledge_time: datetime | None = None,
        run_id: int | None = None,
        workflow_id: str | None = None,
        model_name: str | None = None,
        run_start_time: datetime | None = None,
        run_finish_time: datetime | None = None,
        run_params: dict | None = None,
    ) -> int:
        """Write time-series data for a single series on this scope's owner.

        Builds a 1-route manifest (owner uuid, ``data_type``, ``name``,
        plus optional ``unit``) over ``df`` (pandas or polars) and
        delegates to :meth:`Client.write`. Returns the ``run_id`` used.
        """
        if self._txn is not None:
            _ts_io_unsupported_in_txn("write")
        with self._use_conn() as conn:
            owner_val = self._resolve_uuid(conn)
        manifest = _attach_routing(
            to_polars(df),
            owner_col=self._owner_col,
            owner_val=owner_val,
            data_type=data_type,
            name=name,
            unit=unit,
        )
        return self._client.write(
            manifest,
            knowledge_time=knowledge_time,
            run_id=run_id,
            workflow_id=workflow_id,
            model_name=model_name,
            run_start_time=run_start_time,
            run_finish_time=run_finish_time,
            run_params=run_params,
        )

    def read(
        self,
        *,
        data_type: str | None = None,
        name: str | None = None,
        unit: str | None = None,
        start_valid: datetime | None = None,
        end_valid: datetime | None = None,
        start_known: datetime | None = None,
        end_known: datetime | None = None,
        include_updates: bool = False,
        include_knowledge_time: bool = False,
        output: OutputType = "pandas",
    ) -> pl.DataFrame | pd.DataFrame:
        """Read time-series data for this scope.

        For :class:`NodeScope` the manifest spans the resolved subtree;
        for :class:`EdgeScope` it's the single edge. Returns pandas by
        default; pass ``output="polars"`` for polars.
        """
        if self._txn is not None:
            _ts_io_unsupported_in_txn("read")
        manifest = self._build_read_manifest(data_type=data_type, name=name)
        if manifest is None:
            return to_output(pl.DataFrame(), output)
        return self._client.read(
            manifest,
            unit=unit,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            include_updates=include_updates,
            include_knowledge_time=include_knowledge_time,
            output=output,
        )

    def read_relative(
        self,
        *,
        data_type: str,
        name: str,
        unit: str | None = None,
        output: OutputType = "pandas",
        **td_read_kwargs,
    ) -> pl.DataFrame | pd.DataFrame:
        if self._txn is not None:
            _ts_io_unsupported_in_txn("read_relative")
        manifest = self._build_read_manifest(data_type=data_type, name=name)
        if manifest is None:
            return to_output(pl.DataFrame(), output)
        return self._client.read_relative(manifest, unit=unit, output=output, **td_read_kwargs)


# ---------------------------------------------------------------------------
# NodeScope
# ---------------------------------------------------------------------------


class NodeScope(_BaseScope):
    """Accumulated scope for navigating and operating on a single node.

    Identity is the ``uuid``. ``_path`` and ``_node_uuid`` accumulate as
    the user calls ``.get_node(...)``; resolution happens on the next
    terminal call.
    """

    _owner_col = "node_uuid"

    def __init__(
        self,
        client: Client,
        *,
        node_uuid: UUID | None = None,
        path: Path = (),
        where_filters: dict[str, Any] | None = None,
        txn: Transaction | None = None,
    ):
        self._client = client
        self._node_uuid = node_uuid
        self._path: Path = tuple(path)
        self._where_filters = where_filters
        self._txn = txn

    def _with_txn(self, txn: Transaction) -> NodeScope:
        """Return a sibling scope bound to ``txn``."""
        return NodeScope(
            self._client,
            node_uuid=self._node_uuid,
            path=self._path,
            where_filters=self._where_filters,
            txn=txn,
        )

    # ------------------------------------------------------------------
    # Subclass contract for _BaseScope._apply_mutation
    # ------------------------------------------------------------------

    def _resolve_uuid(self, conn) -> UUID:
        return self._resolve_node_uuid(conn)

    def _fetch_snapshot(self, conn, uuid_: UUID):
        return _fetch_nodes_by_uuids(conn, [uuid_]).get(uuid_)

    def _record_to_txn(self, before, after) -> None:
        assert self._txn is not None
        self._txn._record_node(before, after)

    def _wrap_in_diff(self, before, after) -> TreeDiff:
        return TreeDiff(node_changes=[NodeChange(old=before, new=after)])

    def _not_found_msg(self, uuid_: UUID) -> str:
        return f"Node not found: uuid={uuid_}"

    # ------------------------------------------------------------------
    # Navigation (lazy)
    # ------------------------------------------------------------------

    def get_node(self, *names_or_path, uuid: UUID | None = None) -> NodeScope:
        """Lazy navigation. Accepts variadic names, a tuple/list, or ``uuid=``.

        ``scope.get_node("A", "B")``    — append two segments
        ``scope.get_node(("A", "B"))``  — same, tuple form
        ``scope.get_node(uuid=...)``    — replace scope with absolute uuid
        """
        if uuid is not None:
            if names_or_path:
                raise ValueError("Pass either uuid= or names, not both.")
            return NodeScope(self._client, node_uuid=uuid, txn=self._txn)
        if not names_or_path:
            raise ValueError("Must provide names or uuid.")
        extra = _coerce_path(names_or_path)
        return NodeScope(
            self._client,
            node_uuid=self._node_uuid,
            path=self._path + extra,
            txn=self._txn,
        )

    def where(
        self,
        *,
        type: str | None = None,
        name: str | None = None,
        **property_filters,
    ) -> NodeScope:
        """Lazy subtree filter — narrows the current scope to nodes matching
        the given type / name / data-property predicates. Composes with
        ``.node()`` and resolves at the next terminal call."""
        filters: dict[str, Any] = {}
        if type is not None:
            filters["node_type"] = type
        if name is not None:
            filters["name"] = name
        filters.update(property_filters)
        return NodeScope(
            self._client,
            node_uuid=self._node_uuid,
            path=self._path,
            where_filters=filters,
            txn=self._txn,
        )

    # ------------------------------------------------------------------
    # Internal: resolve scope → uuid(s)
    # ------------------------------------------------------------------

    def _resolve_node_uuid(self, conn) -> UUID:
        if self._path:
            return resolve_node_uuid(conn, self._path, start_uuid=self._node_uuid)
        if self._node_uuid is not None:
            return self._node_uuid
        raise ValueError("NodeScope has no path or uuid to resolve.")

    def _resolve_target_node_uuids(self, conn) -> list[UUID]:
        root_uuid = self._resolve_node_uuid(conn)
        subtree_uuids = resolve_subtree_uuids(conn, root_uuid)
        if not self._where_filters:
            return subtree_uuids

        conditions = ["uuid = ANY(%s)"]
        params: list[Any] = [subtree_uuids]
        if "node_type" in self._where_filters:
            conditions.append("node_type = %s")
            params.append(self._where_filters["node_type"])
        if "name" in self._where_filters:
            conditions.append("name = %s")
            params.append(self._where_filters["name"])
        for key, value in self._where_filters.items():
            if key in ("node_type", "name"):
                continue
            conditions.append("data->>%s = %s")
            params.append(key)
            params.append(str(value))

        where = " AND ".join(conditions)
        rows = conn.execute(f"SELECT uuid FROM energydb.node WHERE {where}", params).fetchall()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # Get / hierarchy queries
    # ------------------------------------------------------------------

    def get(self):
        with self._use_conn() as conn:
            node_uuid = self._resolve_node_uuid(conn)
            row = conn.execute(
                "SELECT uuid, node_type, name, data FROM energydb.node WHERE uuid = %s",
                (node_uuid,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Node not found: uuid={node_uuid}")
            return reconstruct_node({"uuid": row[0], "node_type": row[1], "name": row[2], "data": row[3]})

    def children(self, *, type: str | None = None) -> list[dict]:
        """Direct children of this node only (one level). Optional type filter."""
        with self._use_conn() as conn:
            node_uuid = self._resolve_node_uuid(conn)
            if type:
                rows = conn.execute(
                    "SELECT uuid, node_type, name, data "
                    "FROM energydb.node WHERE parent_uuid = %s AND node_type = %s "
                    "ORDER BY name",
                    (node_uuid, type),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT uuid, node_type, name, data FROM energydb.node WHERE parent_uuid = %s ORDER BY name",
                    (node_uuid,),
                ).fetchall()
            return [{"uuid": r[0], "node_type": r[1], "name": r[2], "data": r[3]} for r in rows]

    def descendants(self, *, type: str | None = None) -> list[dict]:
        """Every node in the subtree rooted at this node, excluding the node
        itself (recursive). Optional type filter."""
        with self._use_conn() as conn:
            node_uuid = self._resolve_node_uuid(conn)
            rows = conn.execute(
                """
                WITH RECURSIVE subtree AS (
                    SELECT uuid FROM energydb.node WHERE uuid = %s
                    UNION ALL
                    SELECT n.uuid FROM energydb.node n
                    JOIN subtree s ON n.parent_uuid = s.uuid
                ) CYCLE uuid SET _is_cycle USING _cycle_path
                SELECT n.uuid, n.node_type, n.name, n.data
                FROM energydb.node n
                JOIN subtree s ON n.uuid = s.uuid
                WHERE NOT s._is_cycle AND n.uuid != %s
                ORDER BY n.name
                """,
                (node_uuid, node_uuid),
            ).fetchall()
            if type:
                rows = [r for r in rows if r[1] == type]
            return [{"uuid": r[0], "node_type": r[1], "name": r[2], "data": r[3]} for r in rows]

    def path(self) -> Path:
        """Return the resolved path of the scope's node."""
        with self._use_conn() as conn:
            node_uuid = self._resolve_node_uuid(conn)
            return resolve_path(conn, node_uuid)

    # ------------------------------------------------------------------
    # Single-element mutations
    # ------------------------------------------------------------------

    def rename(self, new_name: str, *, dry_run: bool = False) -> TreeDiff | None:
        def _do(conn, node_uuid: UUID) -> None:
            conn.execute(
                "UPDATE energydb.node SET name = %s, updated_at = now() WHERE uuid = %s",
                (new_name, node_uuid),
            )

        return self._apply_mutation(_do, dry_run=dry_run)

    def update(self, data: dict, *, replace_data: bool = False, dry_run: bool = False) -> TreeDiff | None:
        """Patch the node's JSONB ``data`` column.

        Default is a shallow merge (Postgres ``data = data || %s``) — top-level
        keys in ``data`` overwrite existing keys; nested objects are replaced,
        not deep-merged. Pass ``replace_data=True`` to fully replace the row's
        ``data`` instead. Renames go through :meth:`rename`.
        """
        op = "data = %s" if replace_data else "data = data || %s"

        def _do(conn, node_uuid: UUID) -> None:
            conn.execute(
                f"UPDATE energydb.node SET {op}, updated_at = now() WHERE uuid = %s",
                (Jsonb(data), node_uuid),
            )

        return self._apply_mutation(_do, dry_run=dry_run)

    def delete(self, *, dry_run: bool = False) -> TreeDiff | None:
        def _do(conn, node_uuid: UUID) -> None:
            conn.execute("DELETE FROM energydb.node WHERE uuid = %s", (node_uuid,))

        return self._apply_mutation(_do, dry_run=dry_run, fetch_after=False)

    def move_to(self, target: NodeScope | Path | list[str], *, dry_run: bool = False) -> TreeDiff | None:
        """Re-parent this node to ``target``.

        ``target`` is a :class:`NodeScope` or a path tuple/list. The node's
        ``uuid`` (and its series) stays attached. The
        ``(parent_uuid, name)`` unique constraint surfaces destination-name
        collisions as a Postgres error.

        Rejects re-parenting into self or any descendant — that would create
        a cycle in the parent chain.
        """
        if isinstance(target, NodeScope):
            target_path = target._path
            target_node_uuid = target._node_uuid
        else:
            target_path = tuple(target)
            target_node_uuid = None

        def _do(conn, node_uuid: UUID) -> None:
            if target_path:
                new_parent_uuid = resolve_node_uuid(conn, target_path, start_uuid=target_node_uuid)
            elif target_node_uuid is not None:
                new_parent_uuid = target_node_uuid
            else:
                raise ValueError("move_to requires a non-root target.")

            if new_parent_uuid == node_uuid:
                raise ValueError("Cannot move a node into itself.")

            # Walk up from new_parent_uuid; if node_uuid is among the ancestors,
            # the move would create a cycle.
            ancestors = conn.execute(
                """
                WITH RECURSIVE chain AS (
                    SELECT uuid, parent_uuid
                    FROM energydb.node WHERE uuid = %s
                    UNION ALL
                    SELECT n.uuid, n.parent_uuid
                    FROM energydb.node n JOIN chain c ON n.uuid = c.parent_uuid
                ) CYCLE uuid SET _is_cycle USING _cycle_path
                SELECT uuid FROM chain WHERE NOT _is_cycle
                """,
                (new_parent_uuid,),
            ).fetchall()
            if any(r[0] == node_uuid for r in ancestors):
                raise ValueError("Cannot move a node into its own subtree (would create a cycle).")

            conn.execute(
                "UPDATE energydb.node SET parent_uuid = %s, updated_at = now() WHERE uuid = %s",
                (new_parent_uuid, node_uuid),
            )

        return self._apply_mutation(_do, dry_run=dry_run)

    def add(self, edm_obj, *, dry_run: bool = False) -> NodeScope | TreeDiff:
        """Add a new child node (or subtree) under this scope.

        Sugar for ``register_tree(edm_obj, under=<this scope>)``. Returns a
        :class:`NodeScope` pointing at the added root, or a :class:`TreeDiff`
        when ``dry_run=True``. Inherits create-only semantics from
        :meth:`Client.register_tree`: raises if any UUID in the payload
        already exists.

        Inside ``client.transaction()`` the insert participates in the
        transaction and shows up in ``txn.preview()``; ``dry_run=True`` is
        not supported inside a transaction.
        """
        if dry_run and self._txn is not None:
            _dry_run_unsupported_in_txn()
        with self._use_conn() as conn:
            parent_uuid = self._resolve_node_uuid(conn)
            root_uuid, diff = register_tree_under(
                conn,
                edm_obj,
                parent_uuid=parent_uuid,
                dry_run=dry_run,
            )
            if self._txn is not None:
                self._txn._node_changes.extend(diff.node_changes)
                self._txn._edge_changes.extend(diff.edge_changes)
                return NodeScope(self._client, node_uuid=root_uuid, txn=self._txn)
            if dry_run:
                conn.rollback()
                return diff
            conn.commit()
        return NodeScope(self._client, node_uuid=root_uuid)

    # ------------------------------------------------------------------
    # Manifest builder for the shared _BaseScope read/read_relative
    # ------------------------------------------------------------------

    def _build_read_manifest(
        self,
        *,
        data_type: str | None,
        name: str | None,
    ) -> pl.DataFrame | None:
        """Resolve the scope's subtree to a node-routed manifest.

        Returns ``None`` when the scope is empty or no series match.
        """
        with self._use_conn() as conn:
            target_uuids = self._resolve_target_node_uuids(conn)
            if not target_uuids:
                return None
            data_type_str = str(data_type).lower() if data_type else None
            meta = series_mod.resolve_for_read(
                conn,
                node_uuids=target_uuids,
                data_type=data_type_str,
                name=name,
            )
        if meta.is_empty():
            return None
        return meta.select(["node_uuid", "data_type", "name"]).unique()


# ---------------------------------------------------------------------------
# EdgeScope
# ---------------------------------------------------------------------------


class EdgeScope(_BaseScope):
    """Scope for operating on a single edge.

    Identified by ``uuid`` or by the ``(from_path, to_path, edge_type)``
    triple.
    """

    _owner_col = "edge_uuid"

    def __init__(
        self,
        client: Client,
        *,
        edge_uuid: UUID | None = None,
        from_path: Path | None = None,
        to_path: Path | None = None,
        edge_type: str | None = None,
        txn: Transaction | None = None,
    ):
        self._client = client
        self._edge_uuid = edge_uuid
        self._txn = txn
        self._from_path = tuple(from_path) if from_path is not None else None
        self._to_path = tuple(to_path) if to_path is not None else None
        self._edge_type = edge_type

    def _with_txn(self, txn: Transaction) -> EdgeScope:
        return EdgeScope(
            self._client,
            edge_uuid=self._edge_uuid,
            from_path=self._from_path,
            to_path=self._to_path,
            edge_type=self._edge_type,
            txn=txn,
        )

    # ------------------------------------------------------------------
    # Subclass contract for _BaseScope._apply_mutation
    # ------------------------------------------------------------------

    def _resolve_uuid(self, conn) -> UUID:
        return self._resolve_edge_uuid(conn)

    def _fetch_snapshot(self, conn, uuid_: UUID):
        return _fetch_edges_by_uuids(conn, [uuid_]).get(uuid_)

    def _record_to_txn(self, before, after) -> None:
        assert self._txn is not None
        self._txn._record_edge(before, after)

    def _wrap_in_diff(self, before, after) -> TreeDiff:
        return TreeDiff(edge_changes=[EdgeChange(old=before, new=after)])

    def _not_found_msg(self, uuid_: UUID) -> str:
        return f"Edge not found: uuid={uuid_}"

    # ------------------------------------------------------------------
    # Internal: identity resolution + endpoint helpers
    # ------------------------------------------------------------------

    def _resolve_edge_uuid(self, conn) -> UUID:
        if self._edge_uuid is not None:
            return self._edge_uuid
        if self._from_path is not None and self._to_path is not None and self._edge_type is not None:
            return resolve_edge_uuid(conn, self._from_path, self._to_path, self._edge_type)
        raise ValueError("EdgeScope has no uuid or (from_path, to_path, edge_type) triple to resolve.")

    def _endpoints(self, conn) -> tuple[UUID, UUID]:
        """Fetch ``(from_node_uuid, to_node_uuid)`` for this edge in one query."""
        edge_uuid = self._resolve_edge_uuid(conn)
        row = conn.execute(
            "SELECT from_node_uuid, to_node_uuid FROM energydb.edge WHERE uuid = %s",
            (edge_uuid,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Edge not found: uuid={edge_uuid}")
        return row[0], row[1]

    # ------------------------------------------------------------------
    # get / navigation
    # ------------------------------------------------------------------

    def get(self):
        with self._use_conn() as conn:
            edge_uuid = self._resolve_edge_uuid(conn)
            row = conn.execute(
                "SELECT uuid, edge_type, name, data, from_node_uuid, to_node_uuid FROM energydb.edge WHERE uuid = %s",
                (edge_uuid,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Edge not found: uuid={edge_uuid}")
            return reconstruct_edge(
                {
                    "uuid": row[0],
                    "edge_type": row[1],
                    "name": row[2],
                    "data": row[3],
                    "from_node_uuid": row[4],
                    "to_node_uuid": row[5],
                }
            )

    def from_node(self) -> NodeScope:
        with self._use_conn() as conn:
            from_uuid, _ = self._endpoints(conn)
        return NodeScope(self._client, node_uuid=from_uuid, txn=self._txn)

    def to_node(self) -> NodeScope:
        with self._use_conn() as conn:
            _, to_uuid = self._endpoints(conn)
        return NodeScope(self._client, node_uuid=to_uuid, txn=self._txn)

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def rename(self, new_name: str, *, dry_run: bool = False) -> TreeDiff | None:
        def _do(conn, edge_uuid: UUID) -> None:
            conn.execute(
                "UPDATE energydb.edge SET name = %s, updated_at = now() WHERE uuid = %s",
                (new_name, edge_uuid),
            )

        return self._apply_mutation(_do, dry_run=dry_run)

    def update(self, data: dict, *, replace_data: bool = False, dry_run: bool = False) -> TreeDiff | None:
        """Patch the edge's JSONB ``data`` column.

        Default is a shallow merge (Postgres ``data = data || %s``); pass
        ``replace_data=True`` to fully replace the row's ``data``. Renames
        go through :meth:`rename`; endpoint changes through :meth:`move_to`.
        """
        op = "data = %s" if replace_data else "data = data || %s"

        def _do(conn, edge_uuid: UUID) -> None:
            conn.execute(
                f"UPDATE energydb.edge SET {op}, updated_at = now() WHERE uuid = %s",
                (Jsonb(data), edge_uuid),
            )

        return self._apply_mutation(_do, dry_run=dry_run)

    def move_to(
        self,
        *,
        from_node: NodeScope | Path | list[str],
        to_node: NodeScope | Path | list[str],
        dry_run: bool = False,
    ) -> TreeDiff | None:
        """Re-point this edge to a new ``(from_node, to_node)`` pair.

        The edge's ``uuid`` (and its series) stays attached. The
        ``(edge_type, from_node_uuid, to_node_uuid)`` unique constraint
        surfaces collisions with an existing edge as a Postgres error.
        """

        def _do(conn, edge_uuid: UUID) -> None:
            new_from_uuid = _resolve_endpoint(conn, from_node)
            new_to_uuid = _resolve_endpoint(conn, to_node)
            if new_from_uuid == new_to_uuid:
                raise ValueError("Edge endpoints must be distinct nodes.")
            conn.execute(
                "UPDATE energydb.edge SET from_node_uuid = %s, to_node_uuid = %s, updated_at = now() WHERE uuid = %s",
                (new_from_uuid, new_to_uuid, edge_uuid),
            )

        return self._apply_mutation(_do, dry_run=dry_run)

    def delete(self, *, dry_run: bool = False) -> TreeDiff | None:
        def _do(conn, edge_uuid: UUID) -> None:
            conn.execute("DELETE FROM energydb.edge WHERE uuid = %s", (edge_uuid,))

        return self._apply_mutation(_do, dry_run=dry_run, fetch_after=False)

    # ------------------------------------------------------------------
    # Manifest builder for the shared _BaseScope read/read_relative
    # ------------------------------------------------------------------

    def _build_read_manifest(
        self,
        *,
        data_type: str | None,
        name: str | None,
    ) -> pl.DataFrame | None:
        """Resolve this edge to an edge-routed manifest. Returns ``None``
        when no series match the given filters."""
        with self._use_conn() as conn:
            edge_uuid = self._resolve_edge_uuid(conn)
            data_type_str = str(data_type).lower() if data_type else None
            meta = series_mod.resolve_for_read(
                conn,
                edge_uuids=[edge_uuid],
                data_type=data_type_str,
                name=name,
            )
        if meta.is_empty():
            return None
        return meta.select(["edge_uuid", "data_type", "name"]).unique()
