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

from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

import pandas as pd
import polars as pl
from psycopg.types.json import Jsonb
from timedatamodel import TimeSeries, TimeSeriesType
from timedb import UnchangedScope, profiling

from energydb import series as series_mod
from energydb._frames import Backend, Output, to_backend, to_polars
from energydb._io import WriteResult, read_relative_resolved, read_resolved
from energydb._join import EdgeSeriesKey, SeriesKey
from energydb._persist import _fetch_edges_by_uuids, _fetch_nodes_by_uuids, register_tree_under
from energydb.diff import EdgeChange, NodeChange, TreeDiff
from energydb.paths import (
    Path,
    _like_escape,
    build_filter_conditions,
    resolve_edge_uuid,
    resolve_node_uuid,
    resolve_path,
    resolve_subtree_uuids,
)
from energydb.serialization import reconstruct_edge, reconstruct_node

if TYPE_CHECKING:
    from energydb._transaction import Transaction
    from energydb.client import AsyncClient


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


def _split_path_string(s: str) -> Path:
    """Split a ``/``-separated path string into segments; validate non-empty.

    ``"P/Site/T01"`` → ``("P", "Site", "T01")``. Leading, trailing, or
    consecutive ``/`` (which would produce an empty segment) are rejected
    with a message naming the offending input. ``/`` itself is forbidden
    inside node/edge/series names by a PG CHECK constraint, so splitting
    is unambiguous.
    """
    if not s:
        raise ValueError("Path string must be non-empty; got ''.")
    segments = s.split("/")
    if any(seg == "" for seg in segments):
        raise ValueError(
            f"Path {s!r} has an empty segment (leading/trailing/double '/'). "
            f"Pass non-empty names separated by single '/'."
        )
    return tuple(segments)


def _flatten_segments(items) -> Path:
    """Flatten an iterable of segments, splitting any ``/``-containing strings.

    Non-string items raise. Used by :func:`_coerce_path` to handle both
    variadic forms (``("a", "b/c")``) and explicit tuple/list arguments
    (``("a", "b/c")`` as one positional). String elements are always split
    on ``/`` for consistency — if the user passed structured data with a
    string segment carrying a separator, that's still a path expression.
    """
    out: list[str] = []
    for it in items:
        if not isinstance(it, str):
            raise TypeError(f"Path segment must be str, got {type(it).__name__}")
        out.extend(_split_path_string(it))
    return tuple(out)


def _coerce_path(args: tuple, kwarg: Path | list[str] | str | None = None) -> Path:
    """Accept variadic names, a single tuple/list, a ``/``-joined string,
    or a kwarg form. Strings are always ``/``-split into segments.

    ``_coerce_path(("P/Site/T01",))``     → ``("P", "Site", "T01")``  *(canonical)*
    ``_coerce_path(("P", "Site", "T01"))``→ ``("P", "Site", "T01")``  *(variadic)*
    ``_coerce_path((("P","Site","T01"),))`` → ``("P", "Site", "T01")``
    ``_coerce_path(("P/Site", "T01"))``   → ``("P", "Site", "T01")``  *(mixed)*
    ``_coerce_path((), kwarg="P/Site")``  → ``("P", "Site")``
    ``_coerce_path((), kwarg=("P","Site"))`` → ``("P", "Site")``
    """
    if kwarg is not None:
        if isinstance(kwarg, str):
            return _split_path_string(kwarg)
        return _flatten_segments(kwarg)
    if len(args) == 1 and isinstance(args[0], (tuple, list)):
        return _flatten_segments(args[0])
    return _flatten_segments(args)


async def _resolve_endpoint(conn, target: NodeScope | Path | list[str] | str) -> UUID:
    """Resolve a node endpoint reference to a UUID against ``conn``.

    Accepts a :class:`NodeScope`, a ``/``-joined string (``"P/Site/T01"``),
    a tuple/list of segments, or a single name. Strings are split on ``/``;
    see :func:`_coerce_path` for full semantics.
    """
    if isinstance(target, NodeScope):
        return await target._resolve_node_uuid(conn)
    path = _coerce_path((), kwarg=target)
    if not path:
        raise ValueError("Endpoint path cannot be empty.")
    return await resolve_node_uuid(conn, path)


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


_SCOPE_IDENTITY_NODE = ("path", "data_type", "name")
_SCOPE_IDENTITY_EDGE = ("from_path", "to_path", "edge_type", "data_type", "name")


def _strip_scope_identity(result: pl.DataFrame, *, is_edge: bool) -> pl.DataFrame:
    """Drop identity columns the scope caller already knows.

    Applied when a scope read resolves to exactly one series — the caller
    is unambiguously asking for that series' data, so re-broadcasting the
    path / data_type / name on every row is pure noise. Multi-series
    scope reads keep the full shape because callers need the identity
    columns to disambiguate.
    """
    cols = _SCOPE_IDENTITY_EDGE if is_edge else _SCOPE_IDENTITY_NODE
    present = [c for c in cols if c in result.columns]
    return result.drop(present) if present else result


def _attach_routing(
    df: pl.DataFrame,
    *,
    owner_col: str,
    owner_val: UUID | str,
    data_type: str,
    name: str,
    unit: str | None,
) -> pl.DataFrame:
    """Attach the routing columns required by the manifest pipeline.

    ``owner_col`` is one of ``"node_uuid"`` / ``"edge_uuid"`` (a UUID owner) or
    ``"path"`` (a ``/``-joined materialized path string). Owner values are
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

    _client: AsyncClient
    _txn: Transaction | None

    # -- shared properties / connection management ---------------------

    @property
    def _pool(self):
        return self._client._pool

    @property
    def _td(self):
        return self._client.td

    @asynccontextmanager
    async def _use_conn(self):
        """Yield a DB connection. Inside a txn, use the txn's connection
        (caller MUST NOT call ``.commit()`` / ``.rollback()``). Otherwise
        borrow from the pool; mutators are responsible for explicit
        ``commit()`` or ``rollback()``.
        """
        if self._txn is not None:
            yield self._txn._conn
            return
        async with self._pool.connection() as conn:
            yield conn

    # -- subclass contract (overridden in NodeScope / EdgeScope) -------

    _owner_col: Literal["node_uuid", "edge_uuid"]

    async def _resolve_uuid(self, conn) -> UUID:
        raise NotImplementedError

    def _write_route(self) -> tuple[str, str] | None:
        """Routing for ``write()`` that needs no DB call, or ``None``.

        Default ``None`` → ``write()`` resolves the owner uuid (one PG
        round-trip). :class:`NodeScope` overrides this to route a path-addressed
        write by its materialized path, collapsing the resolve to one round-trip.
        """
        return None

    async def _fetch_snapshot(self, conn, uuid_: UUID):
        raise NotImplementedError

    def _record_to_txn(self, before, after) -> None:
        raise NotImplementedError

    def _wrap_in_diff(self, before, after) -> TreeDiff:
        raise NotImplementedError

    def _not_found_msg(self, uuid_: UUID) -> str:
        raise NotImplementedError

    async def _build_resolved_meta(self, *, data_type: str | None, name: str | None) -> pl.DataFrame | None:
        """Subclass-specific: resolve the scope to per-series meta in PG.

        Returns one row per series with columns ``(series_id, retention,
        canonical_unit, data_type, name)`` plus exactly one of
        ``node_uuid`` / ``edge_uuid`` — the input shape :func:`read_resolved`
        expects. Returns ``None`` when the scope is empty / nothing matches.
        """
        raise NotImplementedError

    # -- shared mutation machinery -------------------------------------

    async def _apply_mutation(
        self,
        exec_fn: Callable[[Any, UUID], Awaitable[None]],
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
        async with self._use_conn() as conn:
            uuid_ = await self._resolve_uuid(conn)
            before = await self._fetch_snapshot(conn, uuid_)
            if before is None:
                raise ValueError(self._not_found_msg(uuid_))
            (await exec_fn(conn, uuid_))
            after = (await self._fetch_snapshot(conn, uuid_)) if fetch_after else None
            if self._txn is not None:
                self._record_to_txn(before, after)
                return None
            if dry_run:
                await conn.rollback()
                return self._wrap_in_diff(before, after)
            await conn.commit()
        return None

    # -- shared series + timeseries I/O --------------------------------

    async def register_series(
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
        async with self._use_conn() as conn:
            sid = await series_mod.register_series(
                conn,
                owner_col=self._owner_col,
                owner_uuid=(await self._resolve_uuid(conn)),
                retention=retention,
                **args,
            )
            if self._txn is None:
                await conn.commit()
        return sid

    async def write(
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
        skip_unchanged: bool = False,
        unchanged_scope: UnchangedScope = "valid_time",
    ) -> WriteResult:
        """Write time-series data for a single series on this scope's owner.

        Builds a 1-route manifest (owner uuid, ``data_type``, ``name``,
        plus optional ``unit``) over ``df`` (pandas or polars) and
        delegates to :meth:`Client.write`. ``skip_unchanged`` /
        ``unchanged_scope`` are forwarded; see :func:`timedb.write`. Returns a
        :class:`WriteResult` — an ``int`` run_id carrying ``written`` /
        ``skipped`` counts.
        """
        if self._txn is not None:
            _ts_io_unsupported_in_txn("write")
        # A path-addressed NodeScope routes by its materialized path, so the
        # manifest resolve + runs upsert collapse to ONE PG round-trip
        # (resolve_manifest's path route) and the separate uuid resolve is skipped.
        route = self._write_route()
        if route is not None:
            owner_col, owner_val = route
        else:
            async with self._use_conn() as conn:
                owner_val = await self._resolve_uuid(conn)
            owner_col = self._owner_col
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            df_pl = to_polars(df)
        with profiling._phase(profiling.PHASE_EDB_MANIFEST_BUILD):
            manifest = _attach_routing(
                df_pl,
                owner_col=owner_col,
                owner_val=owner_val,
                data_type=data_type,
                name=name,
                unit=unit,
            )
        return await self._client.write(
            manifest,
            knowledge_time=knowledge_time,
            run_id=run_id,
            workflow_id=workflow_id,
            model_name=model_name,
            run_start_time=run_start_time,
            run_finish_time=run_finish_time,
            run_params=run_params,
            skip_unchanged=skip_unchanged,
            unchanged_scope=unchanged_scope,
        )

    async def read(
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
        output: Output = "frame",
        backend: Backend = "polars",
    ) -> (
        pl.DataFrame
        | pd.DataFrame
        | dict[SeriesKey, pl.DataFrame]
        | dict[SeriesKey, pd.DataFrame]
        | dict[EdgeSeriesKey, pl.DataFrame]
        | dict[EdgeSeriesKey, pd.DataFrame]
    ):
        """Read time-series data for this scope.

        For :class:`NodeScope` the manifest spans the resolved subtree;
        for :class:`EdgeScope` it's the single edge. See :meth:`Client.read`
        for the ``output`` / ``backend`` contract.
        """
        if self._txn is not None:
            _ts_io_unsupported_in_txn("read")
        meta = await self._build_resolved_meta(data_type=data_type, name=name)
        if meta is None:
            empty: pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame] = (
                {} if output == "by_path" else pl.DataFrame()
            )
            return to_backend(empty, backend)
        # Take the polars result first so the auto-strip is a single dtype op;
        # convert to the requested backend at the boundary.
        result = await read_resolved(
            self._pool,
            self._td,
            meta,
            unit=unit,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            include_updates=include_updates,
            include_knowledge_time=include_knowledge_time,
            output=output,
        )
        if output == "frame" and meta.height == 1 and isinstance(result, pl.DataFrame):
            result = _strip_scope_identity(result, is_edge=(self._owner_col == "edge_uuid"))
        return to_backend(result, backend)

    async def read_relative(
        self,
        *,
        data_type: str,
        name: str,
        unit: str | None = None,
        output: Output = "frame",
        backend: Backend = "polars",
        **td_read_kwargs,
    ) -> (
        pl.DataFrame
        | pd.DataFrame
        | dict[SeriesKey, pl.DataFrame]
        | dict[SeriesKey, pd.DataFrame]
        | dict[EdgeSeriesKey, pl.DataFrame]
        | dict[EdgeSeriesKey, pd.DataFrame]
    ):
        """Relative-window read for this scope.

        ``**td_read_kwargs`` are forwarded to
        :meth:`timedb.TimeDBClient.read_relative`; see that signature for
        accepted window-selector arguments.
        """
        if self._txn is not None:
            _ts_io_unsupported_in_txn("read_relative")
        meta = await self._build_resolved_meta(data_type=data_type, name=name)
        if meta is None:
            empty: pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame] = (
                {} if output == "by_path" else pl.DataFrame()
            )
            return to_backend(empty, backend)
        result = await read_relative_resolved(
            self._pool,
            self._td,
            meta,
            unit=unit,
            output=output,
            **td_read_kwargs,
        )
        if output == "frame" and meta.height == 1 and isinstance(result, pl.DataFrame):
            result = _strip_scope_identity(result, is_edge=(self._owner_col == "edge_uuid"))
        return to_backend(result, backend)


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

    def _write_route(self) -> tuple[str, str] | None:
        # Path-addressed → route by materialized path (one-round-trip folded
        # resolve + runs upsert). uuid-addressed scopes fall back to the uuid resolve.
        return ("path", "/".join(self._path)) if self._path else None

    def __init__(
        self,
        client: AsyncClient,
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

    def __repr__(self) -> str:
        """Plain-text repr — no I/O. Shows accumulated path, uuid, filters, txn binding."""
        parts: list[str] = []
        if self._path:
            parts.append(f"path={'/'.join(self._path)!r}")
        if self._node_uuid is not None:
            parts.append(f"uuid={self._node_uuid}")
        if self._where_filters:
            parts.append(f"where={self._where_filters!r}")
        if self._txn is not None:
            parts.append("txn=True")
        return f"NodeScope({', '.join(parts) or '<unresolved>'})"

    def _repr_html_(self) -> str:
        """Rich Jupyter repr — no I/O. Renders the scope's accumulated state."""
        addr = "/".join(self._path) if self._path else (str(self._node_uuid) if self._node_uuid else "<unresolved>")
        filters_html = f"<br/><small>where: <code>{self._where_filters!r}</code></small>" if self._where_filters else ""
        txn_html = "<br/><small style='color:#888'>(bound to transaction)</small>" if self._txn else ""
        uuid_html = (
            f"<br/><small style='color:#888'>uuid: <code>{self._node_uuid}</code></small>"
            if self._path and self._node_uuid is not None
            else ""
        )
        return (
            "<div style='border:1px solid #ddd;padding:8px;border-radius:4px;font-family:monospace'>"
            "<b>NodeScope</b><br/>"
            f"<code>{addr}</code>"
            f"{uuid_html}{filters_html}{txn_html}"
            "</div>"
        )

    # ------------------------------------------------------------------
    # Subclass contract for _BaseScope._apply_mutation
    # ------------------------------------------------------------------

    async def _resolve_uuid(self, conn) -> UUID:
        return await self._resolve_node_uuid(conn)

    async def _fetch_snapshot(self, conn, uuid_: UUID):
        return (await _fetch_nodes_by_uuids(conn, [uuid_])).get(uuid_)

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
        """Lazy navigation. Accepts a ``/``-joined string, variadic names,
        a tuple/list, or ``uuid=``.

        ``scope.get_node("Site/T01")``     — canonical ``/``-joined string
        ``scope.get_node("Site", "T01")``  — variadic — equivalent
        ``scope.get_node(("Site","T01"))`` — tuple form
        ``scope.get_node(uuid=...)``       — replace scope with absolute uuid
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

    async def _resolve_node_uuid(self, conn) -> UUID:
        if self._path:
            return await resolve_node_uuid(conn, self._path, start_uuid=self._node_uuid)
        if self._node_uuid is not None:
            return self._node_uuid
        raise ValueError("NodeScope has no path or uuid to resolve.")

    async def _resolve_target_node_uuids(self, conn) -> list[UUID]:
        with profiling._phase(profiling.PHASE_EDB_RESOLVE_SUBTREE):
            root_uuid = await self._resolve_node_uuid(conn)
            if not self._where_filters:
                return await resolve_subtree_uuids(conn, root_uuid)

            # Two-step: fetch root path, then LIKE with escaped prefix as
            # bind param so PG can Index Scan via ``ix_node_path_prefix``.
            # Drop the ``n`` alias since the JOIN is gone — the filter
            # predicates now run directly on ``node``.
            filter_conds, filter_params = build_filter_conditions(self._where_filters, type_col="node_type")
            extra = (" AND " + " AND ".join(filter_conds)) if filter_conds else ""
            root_path_row = await (
                await conn.execute(
                    "SELECT path FROM node WHERE uuid = %s",
                    (root_uuid,),
                )
            ).fetchone()
            if root_path_row is None:
                return []
            root_path = root_path_row[0]
            sql = rf"""
                SELECT uuid FROM node
                WHERE (path = %s OR path LIKE %s || '/%%' ESCAPE '\')
                  {extra}
            """
            rows = await (await conn.execute(sql, (root_path, _like_escape(root_path), *filter_params))).fetchall()
            return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # Get / hierarchy queries
    # ------------------------------------------------------------------

    async def get(self):
        async with self._use_conn() as conn:
            node_uuid = await self._resolve_node_uuid(conn)
            row = await (
                await conn.execute(
                    "SELECT uuid, node_type, name, data FROM node WHERE uuid = %s",
                    (node_uuid,),
                )
            ).fetchone()
            if row is None:
                raise ValueError(f"Node not found: uuid={node_uuid}")
            return reconstruct_node({"uuid": row[0], "node_type": row[1], "name": row[2], "data": row[3]})

    async def get_raw(self) -> dict | None:
        """Fetch this node as a raw dict, without EDM reconstruction.

        Returns ``{uuid, node_type, name, data, parent_uuid, path}`` or ``None``
        if the node does not exist. Use for generic node types (any ``node_type``
        string), where :meth:`get` would raise on an unregistered EDM type.
        """
        async with self._use_conn() as conn:
            node_uuid = await self._resolve_node_uuid(conn)
            row = await (
                await conn.execute(
                    "SELECT uuid, node_type, name, data, parent_uuid, path FROM node WHERE uuid = %s",
                    (node_uuid,),
                )
            ).fetchone()
        if row is None:
            return None
        return {
            "uuid": row[0],
            "node_type": row[1],
            "name": row[2],
            "data": row[3],
            "parent_uuid": row[4],
            "path": row[5],
        }

    async def children(self, *, type: str | None = None) -> list[dict]:
        """Direct children of this node only (one level). Optional type filter."""
        async with self._use_conn() as conn:
            node_uuid = await self._resolve_node_uuid(conn)
            if type:
                rows = await (
                    await conn.execute(
                        "SELECT uuid, node_type, name, data, parent_uuid "
                        "FROM node WHERE parent_uuid = %s AND node_type = %s "
                        "ORDER BY name",
                        (node_uuid, type),
                    )
                ).fetchall()
            else:
                rows = await (
                    await conn.execute(
                        "SELECT uuid, node_type, name, data, parent_uuid "
                        "FROM node WHERE parent_uuid = %s ORDER BY name",
                        (node_uuid,),
                    )
                ).fetchall()
            return [{"uuid": r[0], "node_type": r[1], "name": r[2], "data": r[3], "parent_uuid": r[4]} for r in rows]

    async def descendants(self, *, type: str | None = None) -> list[dict]:
        """Every node in the subtree rooted at this node, excluding the node
        itself (recursive). Optional type filter.

        Materialized-path prefix scan against ``ix_node_path_prefix`` —
        one indexed lookup, no recursive CTE.
        """
        async with self._use_conn() as conn:
            node_uuid = await self._resolve_node_uuid(conn)
            # Two-step: fetch root path, then LIKE with escaped prefix as
            # bind param so PG Index Scans on ``ix_node_path_prefix``.
            root_path_row = await (
                await conn.execute(
                    "SELECT path FROM node WHERE uuid = %s",
                    (node_uuid,),
                )
            ).fetchone()
            if root_path_row is None:
                return []
            root_path = root_path_row[0]
            rows = await (
                await conn.execute(
                    r"""
                SELECT uuid, node_type, name, data, parent_uuid FROM node
                WHERE path LIKE %s || '/%%' ESCAPE '\'
                  AND (%s::text IS NULL OR node_type = %s::text)
                ORDER BY name
                """,
                    (_like_escape(root_path), type, type),
                )
            ).fetchall()
            return [{"uuid": r[0], "node_type": r[1], "name": r[2], "data": r[3], "parent_uuid": r[4]} for r in rows]

    async def path(self) -> Path:
        """Return the resolved path of the scope's node."""
        async with self._use_conn() as conn:
            node_uuid = await self._resolve_node_uuid(conn)
            return await resolve_path(conn, node_uuid)

    # ------------------------------------------------------------------
    # Single-element mutations
    # ------------------------------------------------------------------

    async def rename(self, new_name: str, *, dry_run: bool = False) -> TreeDiff | None:
        async def _do(conn, node_uuid: UUID) -> None:
            # One SELECT to grab the node's current path and its parent's path,
            # then a single UPDATE rewrites ``path`` for self + every descendant
            # via the ``ix_node_path_prefix`` index. ``name`` is only changed on
            # the renamed row itself.
            row = await (
                await conn.execute(
                    """
                SELECT n.path AS old_path, p.path AS parent_path
                FROM node n
                LEFT JOIN node p ON p.uuid = n.parent_uuid
                WHERE n.uuid = %s
                """,
                    (node_uuid,),
                )
            ).fetchone()
            if row is None:
                raise ValueError(f"Node not found: uuid={node_uuid}")
            old_path, parent_path = row
            new_path = f"{parent_path}/{new_name}" if parent_path else new_name

            await conn.execute(
                r"""
                UPDATE node
                SET path = %s || substring(path FROM length(%s) + 1),
                    name = CASE WHEN path = %s THEN %s ELSE name END,
                    updated_at = now()
                WHERE path = %s OR path LIKE %s || '/%%' ESCAPE '\'
                """,
                (new_path, old_path, old_path, new_name, old_path, _like_escape(old_path)),
            )

        return await self._apply_mutation(_do, dry_run=dry_run)

    async def update(self, data: dict, *, replace_data: bool = False, dry_run: bool = False) -> TreeDiff | None:
        """Patch the node's JSONB ``data`` column.

        Default is a shallow merge (Postgres ``data = data || %s``) — top-level
        keys in ``data`` overwrite existing keys; nested objects are replaced,
        not deep-merged. Pass ``replace_data=True`` to fully replace the row's
        ``data`` instead. Renames go through :meth:`rename`.
        """
        op = "data = %s" if replace_data else "data = data || %s"

        async def _do(conn, node_uuid: UUID) -> None:
            await conn.execute(
                f"UPDATE node SET {op}, updated_at = now() WHERE uuid = %s",
                (Jsonb(data), node_uuid),
            )

        return await self._apply_mutation(_do, dry_run=dry_run)

    async def delete(self, *, dry_run: bool = False) -> TreeDiff | None:
        async def _do(conn, node_uuid: UUID) -> None:
            await conn.execute("DELETE FROM node WHERE uuid = %s", (node_uuid,))

        return await self._apply_mutation(_do, dry_run=dry_run, fetch_after=False)

    async def move_to(self, target: NodeScope | Path | list[str] | str, *, dry_run: bool = False) -> TreeDiff | None:
        """Re-parent this node to ``target``.

        ``target`` is a :class:`NodeScope`, a ``/``-joined string
        (``"P/Site"``), or a tuple/list of segments. The node's ``uuid``
        (and its series) stays attached. The ``(parent_uuid, name)``
        unique constraint surfaces destination-name collisions as a
        Postgres error.

        Rejects re-parenting into self or any descendant — that would create
        a cycle in the parent chain.
        """
        if isinstance(target, NodeScope):
            target_path = target._path
            target_node_uuid = target._node_uuid
        else:
            target_path = _coerce_path((), kwarg=target)
            target_node_uuid = None

        async def _do(conn, node_uuid: UUID) -> None:
            if target_path:
                new_parent_uuid = await resolve_node_uuid(conn, target_path, start_uuid=target_node_uuid)
            elif target_node_uuid is not None:
                new_parent_uuid = target_node_uuid
            else:
                raise ValueError("move_to requires a non-root target.")

            if new_parent_uuid == node_uuid:
                raise ValueError("Cannot move a node into itself.")

            # Cycle iff the prospective new parent is at or under the moving
            # node's own path. Fetch the moving node's path to Python and use
            # the bind-param LIKE-escape (the SQL ``_like_esc`` helper is gone).
            subj_row = await (
                await conn.execute(
                    "SELECT path FROM node WHERE uuid = %s",
                    (node_uuid,),
                )
            ).fetchone()
            if subj_row is None:
                raise ValueError(f"Node not found: uuid={node_uuid}")
            subj_path = subj_row[0]
            cycle_row = await (
                await conn.execute(
                    r"""
                SELECT EXISTS (
                    SELECT 1 FROM node cand
                    WHERE cand.uuid = %s
                      AND (cand.path = %s OR cand.path LIKE %s || '/%%' ESCAPE '\')
                )
                """,
                    (new_parent_uuid, subj_path, _like_escape(subj_path)),
                )
            ).fetchone()
            if cycle_row and cycle_row[0]:
                raise ValueError("Cannot move a node into its own subtree (would create a cycle).")

            # Fetch old path, the new parent's path, and the moving node's own
            # name. ``LEFT JOIN`` against the new parent so a move-to-root
            # (``new_parent_uuid IS NULL``) returns ``new_parent_path = None``.
            row = await (
                await conn.execute(
                    """
                SELECT n.path AS old_path,
                       parent.path AS new_parent_path,
                       n.name AS own_name
                FROM node n
                LEFT JOIN node parent ON parent.uuid = %s
                WHERE n.uuid = %s
                """,
                    (new_parent_uuid, node_uuid),
                )
            ).fetchone()
            if row is None:
                raise ValueError(f"Node not found: uuid={node_uuid}")
            old_path, new_parent_path, own_name = row
            new_path = f"{new_parent_path}/{own_name}" if new_parent_path else own_name

            await conn.execute(
                r"""
                UPDATE node
                SET parent_uuid = CASE WHEN uuid = %s THEN %s ELSE parent_uuid END,
                    path = %s || substring(path FROM length(%s) + 1),
                    updated_at = now()
                WHERE path = %s OR path LIKE %s || '/%%' ESCAPE '\'
                """,
                (node_uuid, new_parent_uuid, new_path, old_path, old_path, _like_escape(old_path)),
            )

        return await self._apply_mutation(_do, dry_run=dry_run)

    async def add(self, edm_obj, *, dry_run: bool = False) -> NodeScope | TreeDiff:
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
        async with self._use_conn() as conn:
            parent_uuid = await self._resolve_node_uuid(conn)
            root_uuid, diff = await register_tree_under(
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
                await conn.rollback()
                return diff
            await conn.commit()
        return NodeScope(self._client, node_uuid=root_uuid)

    # ------------------------------------------------------------------
    # Manifest builder for the shared _BaseScope read/read_relative
    # ------------------------------------------------------------------

    async def _build_resolved_meta(
        self,
        *,
        data_type: str | None,
        name: str | None,
    ) -> pl.DataFrame | None:
        """Resolve the scope's subtree to per-series read meta in one PG round-trip.

        Returns the per-series ``(series_id, retention, canonical_unit,
        data_type, name, node_uuid)`` frame :func:`read_resolved` consumes
        directly — no second hash-and-join pass through
        :func:`resolve_manifest`. ``None`` when the subtree is empty or no
        series match the optional ``data_type`` / ``name`` filters.
        """
        data_type_str = str(data_type).lower() if data_type else None
        if self._where_filters:
            where_conds, where_params = build_filter_conditions(
                self._where_filters, type_col="node_type", table_alias="n"
            )
        else:
            where_conds, where_params = [], []
        async with self._use_conn() as conn:
            with profiling._phase(profiling.PHASE_EDB_RESOLVE):
                if self._path and self._node_uuid is None:
                    meta = await series_mod.resolve_subtree_series_for_read(
                        conn,
                        root_path="/".join(self._path),
                        where_conds=where_conds,
                        where_params=where_params,
                        data_type=data_type_str,
                        name=name,
                    )
                elif self._node_uuid is not None:
                    meta = await series_mod.resolve_subtree_series_for_read(
                        conn,
                        start_uuid=self._node_uuid,
                        rel_path="/".join(self._path) if self._path else None,
                        where_conds=where_conds,
                        where_params=where_params,
                        data_type=data_type_str,
                        name=name,
                    )
                else:
                    raise ValueError("NodeScope has no path or uuid to resolve.")
        return None if meta.is_empty() else meta


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
        client: AsyncClient,
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

    def __repr__(self) -> str:
        """Plain-text repr — no I/O."""
        if self._edge_uuid is not None and self._from_path is None:
            base = f"EdgeScope(uuid={self._edge_uuid}"
        else:
            base = (
                f"EdgeScope(from={'/'.join(self._from_path or ())!r}, "
                f"to={'/'.join(self._to_path or ())!r}, "
                f"type={self._edge_type!r}"
            )
        if self._txn is not None:
            base += ", txn=True"
        return base + ")"

    # ------------------------------------------------------------------
    # Subclass contract for _BaseScope._apply_mutation
    # ------------------------------------------------------------------

    async def _resolve_uuid(self, conn) -> UUID:
        return await self._resolve_edge_uuid(conn)

    async def _fetch_snapshot(self, conn, uuid_: UUID):
        return (await _fetch_edges_by_uuids(conn, [uuid_])).get(uuid_)

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

    async def _resolve_edge_uuid(self, conn) -> UUID:
        if self._edge_uuid is not None:
            return self._edge_uuid
        if self._from_path is not None and self._to_path is not None and self._edge_type is not None:
            return await resolve_edge_uuid(conn, self._from_path, self._to_path, self._edge_type)
        raise ValueError("EdgeScope has no uuid or (from_path, to_path, edge_type) triple to resolve.")

    async def _endpoints(self, conn) -> tuple[UUID, UUID]:
        """Fetch ``(from_node_uuid, to_node_uuid)`` for this edge in one query."""
        edge_uuid = await self._resolve_edge_uuid(conn)
        row = await (
            await conn.execute(
                "SELECT from_node_uuid, to_node_uuid FROM edge WHERE uuid = %s",
                (edge_uuid,),
            )
        ).fetchone()
        if row is None:
            raise ValueError(f"Edge not found: uuid={edge_uuid}")
        return row[0], row[1]

    # ------------------------------------------------------------------
    # get / navigation
    # ------------------------------------------------------------------

    async def get(self):
        async with self._use_conn() as conn:
            edge_uuid = await self._resolve_edge_uuid(conn)
            row = await (
                await conn.execute(
                    "SELECT uuid, edge_type, name, data, from_node_uuid, to_node_uuid FROM edge WHERE uuid = %s",
                    (edge_uuid,),
                )
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

    async def from_node(self) -> NodeScope:
        async with self._use_conn() as conn:
            from_uuid, _ = await self._endpoints(conn)
        return NodeScope(self._client, node_uuid=from_uuid, txn=self._txn)

    async def to_node(self) -> NodeScope:
        async with self._use_conn() as conn:
            _, to_uuid = await self._endpoints(conn)
        return NodeScope(self._client, node_uuid=to_uuid, txn=self._txn)

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    async def rename(self, new_name: str, *, dry_run: bool = False) -> TreeDiff | None:
        async def _do(conn, edge_uuid: UUID) -> None:
            await conn.execute(
                "UPDATE edge SET name = %s, updated_at = now() WHERE uuid = %s",
                (new_name, edge_uuid),
            )

        return await self._apply_mutation(_do, dry_run=dry_run)

    async def update(self, data: dict, *, replace_data: bool = False, dry_run: bool = False) -> TreeDiff | None:
        """Patch the edge's JSONB ``data`` column.

        Default is a shallow merge (Postgres ``data = data || %s``); pass
        ``replace_data=True`` to fully replace the row's ``data``. Renames
        go through :meth:`rename`; endpoint changes through :meth:`move_to`.
        """
        op = "data = %s" if replace_data else "data = data || %s"

        async def _do(conn, edge_uuid: UUID) -> None:
            await conn.execute(
                f"UPDATE edge SET {op}, updated_at = now() WHERE uuid = %s",
                (Jsonb(data), edge_uuid),
            )

        return await self._apply_mutation(_do, dry_run=dry_run)

    async def move_to(
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

        async def _do(conn, edge_uuid: UUID) -> None:
            new_from_uuid = await _resolve_endpoint(conn, from_node)
            new_to_uuid = await _resolve_endpoint(conn, to_node)
            if new_from_uuid == new_to_uuid:
                raise ValueError("Edge endpoints must be distinct nodes.")
            await conn.execute(
                "UPDATE edge SET from_node_uuid = %s, to_node_uuid = %s, updated_at = now() WHERE uuid = %s",
                (new_from_uuid, new_to_uuid, edge_uuid),
            )

        return await self._apply_mutation(_do, dry_run=dry_run)

    async def delete(self, *, dry_run: bool = False) -> TreeDiff | None:
        async def _do(conn, edge_uuid: UUID) -> None:
            await conn.execute("DELETE FROM edge WHERE uuid = %s", (edge_uuid,))

        return await self._apply_mutation(_do, dry_run=dry_run, fetch_after=False)

    # ------------------------------------------------------------------
    # Manifest builder for the shared _BaseScope read/read_relative
    # ------------------------------------------------------------------

    async def _build_resolved_meta(
        self,
        *,
        data_type: str | None,
        name: str | None,
    ) -> pl.DataFrame | None:
        """Resolve this edge to per-series read meta. ``None`` if no series match."""
        async with self._use_conn() as conn:
            edge_uuid = await self._resolve_edge_uuid(conn)
            data_type_str = str(data_type).lower() if data_type else None
            with profiling._phase(profiling.PHASE_EDB_RESOLVE):
                meta = await series_mod.resolve_for_read(
                    conn,
                    owner_col="edge_uuid",
                    owner_uuids=[edge_uuid],
                    data_type=data_type_str,
                    name=name,
                )
        if meta.is_empty():
            return None
        with profiling._phase(profiling.PHASE_EDB_MANIFEST_BUILD):
            return meta.drop("node_uuid")
