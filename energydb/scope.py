"""NodeScope and EdgeScope: fluent APIs for navigating and operating on
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
from timedb import PgEngineMeta, UnchangedScope, profiling

from energydb import series as series_mod
from energydb._ch_meta_engine import CH_ENGINE_TABLE
from energydb._frames import Backend, Output, to_backend, to_polars
from energydb._io import WriteResult, annotate_undefined_table, execute_read
from energydb._join import EdgeSeriesKey, SeriesKey
from energydb._persist import _fetch_edges_by_uuids, _fetch_nodes_by_uuids, map_edge_conflict, register_tree_under
from energydb.diff import EdgeChange, NodeChange, TreeDiff
from energydb.errors import EdgeNotFoundError, NodeNotFoundError, NotFoundError, ValidationError
from energydb.models import SQL_SCHEMA_PREFIX as P
from energydb.paths import (
    Path,
    _like_escape,
    ambiguous_edge_error,
    build_filter_conditions,
    derived_prefix_like,
    edge_address_repr,
    resolve_edge_uuid,
    resolve_node_uuid,
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
    raise ValidationError("dry_run is not supported inside a transaction(); use txn.preview() instead.")


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
        raise ValidationError("Path string must be non-empty; got ''.")
    segments = s.split("/")
    if any(seg == "" for seg in segments):
        raise ValidationError(
            f"Path {s!r} has an empty segment (leading/trailing/double '/'). "
            f"Pass non-empty names separated by single '/'."
        )
    return tuple(segments)


def _flatten_segments(items) -> Path:
    """Flatten an iterable of segments, splitting any ``/``-containing strings.

    Non-string items raise. Used by :func:`_coerce_path` to handle both
    variadic forms (``("a", "b/c")``) and explicit tuple/list arguments
    (``("a", "b/c")`` as one positional). String elements are always split
    on ``/`` for consistency: a string segment carrying a separator is still a
    path expression, even inside structured data.
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
        raise ValidationError("Endpoint path cannot be empty.")
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
        raise ValidationError("name is required")
    if data_type is None:
        raise ValidationError("data_type is required")
    if canonical_unit is None:
        raise ValidationError("canonical_unit is required")
    if timeseries_type is None:
        raise ValidationError("timeseries_type is required (FLAT | OVERLAPPING)")

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

    Applied when a scope read resolves to exactly one series: the caller is
    unambiguously asking for that series' data, so re-broadcasting the
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
# _BaseScope: shared plumbing for NodeScope and EdgeScope
# ---------------------------------------------------------------------------


class _BaseScope:
    """Shared connection / mutation plumbing.

    Subclasses plug in their identity by implementing the small set of
    abstract methods below; everything else (connection borrowing, txn
    routing, the 8-step mutator boilerplate) lives here.
    """

    _client: AsyncClient
    _txn: Transaction | None

    # -----------------------------------------------------------------------
    # shared properties / connection management
    # -----------------------------------------------------------------------

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

        Wrapped in :func:`annotate_undefined_table` on both branches, so every
        scope operation gets the schema-misconfiguration note.
        """
        if self._txn is not None:
            async with annotate_undefined_table():
                yield self._txn._conn
            return
        # Client checkout point: binds the namespace GUC on namespaced views.
        async with self._client._conn() as conn:
            yield conn

    @asynccontextmanager
    async def _use_read_conn(self):
        """Yield a connection for a pure read. Inside a txn, the txn's
        connection, since reads must see the transaction's uncommitted
        mutations.
        Otherwise a pooled autocommit connection: SELECTs under READ
        COMMITTED see the same data either way, and autocommit skips
        psycopg's implicit-BEGIN round-trip and the pool's rollback-on-return.
        """
        if self._txn is not None:
            async with annotate_undefined_table():
                yield self._txn._conn
            return
        # Client checkout point (autocommit): binds the namespace GUC at
        # session level on namespaced views. Annotates already.
        async with self._client._read_conn() as conn:
            yield conn

    # -----------------------------------------------------------------------
    # subclass contract (overridden in NodeScope / EdgeScope)
    # -----------------------------------------------------------------------

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

    def _not_found_error(self, uuid_: UUID) -> NotFoundError:
        """The typed not-found error for this scope's target, addressed by uuid."""
        raise NotImplementedError

    async def _build_resolved_meta(self, *, data_type: str | None, name: str | None) -> pl.DataFrame | None:
        """Subclass-specific: resolve the scope to per-series meta in PG.

        Returns one row per series with columns ``(series_id, retention,
        canonical_unit, data_type, name)`` plus exactly one of
        ``node_uuid`` / ``edge_uuid``, which is the input shape
        :func:`execute_read` expects. Returns ``None`` when the scope is empty / nothing matches.
        """
        raise NotImplementedError

    def _engine_meta(self, *, data_type: str | None, name: str | None) -> PgEngineMeta | None:
        """The engine-table predicate for this scope's read, or ``None`` when the scope
        can't be expressed server-side (the read then runs sequentially).

        Called lazily: :func:`execute_read` receives this as a thunk and invokes
        it only once the session's engine is known to be available, so an
        implementation never runs on an engine-disabled session.
        """
        raise NotImplementedError

    def _finalize_result(self, result, *, n_series: int, output: str, backend: Backend):
        """Shared tail of every scope read: single-series identity strip + backend convert.

        When a frame-shaped read resolved to exactly one series, the caller
        unambiguously asked for that series' data, so the broadcast identity
        columns are dropped (see :func:`_strip_scope_identity`). The polars
        result converts to the requested backend at this boundary.
        """
        if output == "frame" and n_series == 1 and isinstance(result, pl.DataFrame):
            result = _strip_scope_identity(result, is_edge=(self._owner_col == "edge_uuid"))
        return to_backend(result, backend)

    # -----------------------------------------------------------------------
    # shared mutation machinery
    # -----------------------------------------------------------------------

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
        ``fetch_after=False`` is for deletes: the post-state record is
        ``None``.

        Behavior by scope kind: txn-bound scopes record (before, after) on
        the txn and return ``None``; dry-run scopes roll back and return a
        :class:`TreeDiff`; plain scopes commit and return ``None``.
        """
        if dry_run and self._txn is not None:
            _dry_run_unsupported_in_txn()
        async with self._use_conn() as conn:
            uuid_ = await self._resolve_uuid(conn)
            before = await self._fetch_snapshot(conn, uuid_)
            if before is None:
                raise self._not_found_error(uuid_)
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

    # -----------------------------------------------------------------------
    # shared series + timeseries I/O
    # -----------------------------------------------------------------------

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
        unchanged_scope: UnchangedScope = "auto",
    ) -> WriteResult:
        """Write time-series data for a single series on this scope's owner.

        Builds a 1-route manifest (owner uuid, ``data_type``, ``name``,
        plus optional ``unit``) over ``df`` (pandas or polars) and
        delegates to :meth:`Client.write`. ``skip_unchanged`` /
        ``unchanged_scope`` are forwarded; the default ``"auto"`` picks the
        comparison key from this series' registered type, so an OVERLAPPING
        series keeps its republications; see :meth:`Client.write`. Returns a
        :class:`WriteResult`, an ``int`` run_id carrying ``written`` /
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
        for the ``output`` / ``backend`` contract. When the scope is
        engine-expressible (see :meth:`_engine_meta`) the PG resolve runs in
        parallel with the CH value read; otherwise (``.where()`` filters,
        uuid-addressed subtrees, or an unavailable engine) it runs sequentially.
        Results are identical either way.
        """
        if self._txn is not None:
            _ts_io_unsupported_in_txn("read")
        # Scope reads route by subtree, not by a manifest: "nothing matched"
        # already returns an empty result rather than raising, so there is no
        # on_missing to expose and the third element is always empty.
        result, n_series, _missing = await execute_read(
            self._pool,
            self._td,
            self._client,
            resolve=lambda: self._build_resolved_meta(data_type=data_type, name=name),
            engine_meta=lambda: self._engine_meta(data_type=data_type, name=name),
            unit=unit,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            include_updates=include_updates,
            include_knowledge_time=include_knowledge_time,
            output=output,
        )
        return self._finalize_result(result, n_series=n_series, output=output, backend=backend)

    async def resolve(
        self,
        *,
        data_type: str | None = None,
        name: str | None = None,
    ) -> pl.DataFrame | None:
        """Resolve this scope to per-series read metadata in one PG round-trip,
        *without* reading any timeseries data.

        Returns the frame :meth:`read_from_meta` consumes, one row per series
        with ``series_id``, ``canonical_unit``, ``timeseries_type``,
        ``data_type``, ``name`` and (for node scopes) the materialized ``path``,
        or ``None`` if nothing matches. Splitting resolve from the read lets a
        caller authorize or inspect (e.g. by ``path``) before paying for the
        ClickHouse read; ``resolve()`` then :meth:`read_from_meta` is exactly
        what :meth:`read` does in one call (sequential path).
        """
        if self._txn is not None:
            _ts_io_unsupported_in_txn("resolve")
        return await self._build_resolved_meta(data_type=data_type, name=name)

    async def read_from_meta(
        self,
        meta: pl.DataFrame,
        *,
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
        """Read timeseries data for a ``meta`` frame from :meth:`resolve`:
        the ClickHouse leg only, with no further PG round-trip. ``output`` /
        ``backend`` follow the :meth:`read` contract.

        Implemented over :func:`execute_read` with an instant resolve and no
        engine predicate (the meta is already exact), so it shares the one
        read pipeline with everything else.
        """
        if self._txn is not None:
            _ts_io_unsupported_in_txn("read_from_meta")

        async def _instant_resolve() -> pl.DataFrame | None:
            return meta

        result, n_series, _missing = await execute_read(
            self._pool,
            self._td,
            self._client,
            resolve=_instant_resolve,
            engine_meta=None,
            unit=unit,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            include_updates=include_updates,
            include_knowledge_time=include_knowledge_time,
            output=output,
        )
        return self._finalize_result(result, n_series=n_series, output=output, backend=backend)

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
        result, n_series, _missing = await execute_read(
            self._pool,
            self._td,
            self._client,
            resolve=lambda: self._build_resolved_meta(data_type=data_type, name=name),
            engine_meta=lambda: self._engine_meta(data_type=data_type, name=name),
            relative=True,
            unit=unit,
            output=output,
            td_kwargs=td_read_kwargs,
        )
        return self._finalize_result(result, n_series=n_series, output=output, backend=backend)


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
        # Path-addressed: route by materialized path (one-round-trip folded
        # resolve + runs upsert). uuid-addressed scopes fall back to the uuid
        # resolve.
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
        """Plain-text repr: no I/O. Shows accumulated path, uuid, filters, txn binding."""
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
        """Rich Jupyter repr: no I/O. Renders the scope's accumulated state."""
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

    # -----------------------------------------------------------------------
    # Subclass contract for _BaseScope._apply_mutation
    # -----------------------------------------------------------------------

    async def _resolve_uuid(self, conn) -> UUID:
        return await self._resolve_node_uuid(conn)

    async def _fetch_snapshot(self, conn, uuid_: UUID):
        return (await _fetch_nodes_by_uuids(conn, [uuid_])).get(uuid_)

    def _record_to_txn(self, before, after) -> None:
        assert self._txn is not None
        self._txn._record_node(before, after)

    def _wrap_in_diff(self, before, after) -> TreeDiff:
        return TreeDiff(node_changes=[NodeChange(old=before, new=after)])

    def _not_found_error(self, uuid_: UUID) -> NotFoundError:
        return NodeNotFoundError(f"Node not found: uuid={uuid_}", uuid=uuid_)

    # -----------------------------------------------------------------------
    # Navigation (lazy)
    # -----------------------------------------------------------------------

    def get_node(self, *names_or_path, uuid: UUID | None = None) -> NodeScope:
        """Lazy navigation. Accepts a ``/``-joined string, variadic names,
        a tuple/list, or ``uuid=``.

        ``scope.get_node("Site/T01")``: canonical ``/``-joined string
        ``scope.get_node("Site", "T01")``: variadic, equivalent
        ``scope.get_node(("Site","T01"))``: tuple form
        ``scope.get_node(uuid=...)``: replace scope with absolute uuid
        """
        if uuid is not None:
            if names_or_path:
                raise ValidationError("Pass either uuid= or names, not both.")
            return NodeScope(self._client, node_uuid=uuid, txn=self._txn)
        if not names_or_path:
            raise ValidationError("Must provide names or uuid.")
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
        """Lazy subtree filter: narrows the current scope to nodes matching
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

    # -----------------------------------------------------------------------
    # Internal: resolve scope → uuid(s)
    # -----------------------------------------------------------------------

    async def _resolve_node_uuid(self, conn) -> UUID:
        if self._path:
            return await resolve_node_uuid(conn, self._path, start_uuid=self._node_uuid)
        if self._node_uuid is not None:
            return self._node_uuid
        raise ValidationError("NodeScope has no path or uuid to resolve.")

    def _node_match(self) -> tuple[str, list[Any], str, list[Any]]:
        """SQL pieces matching exactly this scope's node as alias ``n``.

        Returns ``(from_sql, from_params, where_sql, where_params)`` for the
        three addressings (absolute path / uuid / uuid + relative path), so
        the single-round-trip reads inline the resolve instead of paying a
        separate ``resolve_node_uuid`` query. Params are split per segment
        because psycopg fills placeholders in textual order.
        """
        if self._path:
            joined = "/".join(self._path)
            if self._node_uuid is None:
                return f"{P}node n", [], "n.path = %s", [joined]
            return (
                f"{P}node n JOIN {P}node s ON n.path = s.path || '/' || %s",
                [joined],
                "s.uuid = %s",
                [self._node_uuid],
            )
        if self._node_uuid is not None:
            return f"{P}node n", [], "n.uuid = %s", [self._node_uuid]
        raise ValidationError("NodeScope has no path or uuid to resolve.")

    def _missing_error(self) -> NodeNotFoundError:
        """The not-found error ``resolve_node_uuid`` / ``get`` would raise."""
        if self._path:
            joined = "/".join(self._path)
            if self._node_uuid is not None:
                return NodeNotFoundError(
                    f"Node not found: {joined} (relative to {self._node_uuid})",
                    path=joined,
                )
            return NodeNotFoundError(f"Node not found: {joined}", path=joined)
        return NodeNotFoundError(f"Node not found: uuid={self._node_uuid}", uuid=self._node_uuid)

    async def _resolve_target_node_uuids(self, conn) -> list[UUID]:
        with profiling._phase(profiling.PHASE_EDB_RESOLVE_SUBTREE):
            root_uuid = await self._resolve_node_uuid(conn)
            if not self._where_filters:
                return await resolve_subtree_uuids(conn, root_uuid)

            # Two-step: fetch root path, then LIKE with escaped prefix as
            # bind param so PG can Index Scan via ix_node_path_prefix.
            # Drop the n alias since the JOIN is gone; the filter predicates
            # now run directly on node.
            filter_conds, filter_params = build_filter_conditions(self._where_filters, type_col="node_type")
            extra = (" AND " + " AND ".join(filter_conds)) if filter_conds else ""
            root_path_row = await (
                await conn.execute(
                    f"SELECT path FROM {P}node WHERE uuid = %s",
                    (root_uuid,),
                )
            ).fetchone()
            if root_path_row is None:
                return []
            root_path = root_path_row[0]
            sql = rf"""
                SELECT uuid FROM {P}node
                WHERE (path = %s OR path LIKE %s || '/%%' ESCAPE '\')
                  {extra}
            """
            rows = await (await conn.execute(sql, (root_path, _like_escape(root_path), *filter_params))).fetchall()
            return [r[0] for r in rows]

    # -----------------------------------------------------------------------
    # Get / hierarchy queries
    # -----------------------------------------------------------------------

    async def get(self):
        """Reconstruct this node as an EnergyDataModel object.

        The returned ``Element`` keeps the stored UUID, so it round-trips.
        Series are not attached: use
        :meth:`Client.get_tree(include_series=True) <energydb.AsyncClient.get_tree>`
        for that, or :meth:`get_raw` for the plain row.

        Raises :class:`~energydb.errors.NodeNotFoundError` if the path or
        uuid resolves to nothing.
        """
        frm, frm_params, where, where_params = self._node_match()
        async with self._use_read_conn() as conn:
            row = await (
                await conn.execute(
                    f"SELECT n.uuid, n.node_type, n.name, n.data FROM {frm} WHERE {where}",
                    [*frm_params, *where_params],
                )
            ).fetchone()
        if row is None:
            raise self._missing_error()
        return reconstruct_node({"uuid": row[0], "node_type": row[1], "name": row[2], "data": row[3]})

    async def get_raw(self) -> dict | None:
        """Fetch this node as a raw dict, without EDM reconstruction.

        Returns ``{uuid, node_type, name, data, parent_uuid, path}`` or ``None``
        if the uuid-addressed node does not exist (a path-addressed miss raises,
        matching the resolve contract). Use for generic node types (any
        ``node_type`` string), where :meth:`get` would raise on an
        unregistered EDM type.
        """
        frm, frm_params, where, where_params = self._node_match()
        async with self._use_read_conn() as conn:
            row = await (
                await conn.execute(
                    f"SELECT n.uuid, n.node_type, n.name, n.data, n.parent_uuid, n.path FROM {frm} WHERE {where}",
                    [*frm_params, *where_params],
                )
            ).fetchone()
        if row is None:
            if self._path:
                raise self._missing_error()
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
        """Direct children of this node only (one level). Optional type filter.

        One round-trip: the scope resolve rides the same statement, and the
        LEFT JOIN keeps the root row so a missing node (raise / empty per
        addressing) is distinguishable from a childless one (empty).
        """
        frm, frm_params, where, where_params = self._node_match()
        type_cond = " AND c.node_type = %s" if type else ""
        type_params = [type] if type else []
        async with self._use_read_conn() as conn:
            rows = await (
                await conn.execute(
                    f"SELECT c.uuid, c.node_type, c.name, c.data, c.parent_uuid "
                    f"FROM {frm} LEFT JOIN {P}node c ON c.parent_uuid = n.uuid{type_cond} "
                    f"WHERE {where} ORDER BY c.name",
                    [*frm_params, *type_params, *where_params],
                )
            ).fetchall()
        if not rows and self._path:
            raise self._missing_error()
        return [
            {"uuid": r[0], "node_type": r[1], "name": r[2], "data": r[3], "parent_uuid": r[4]}
            for r in rows
            if r[0] is not None  # the LEFT-JOIN row of a childless root
        ]

    async def descendants(self, *, type: str | None = None) -> list[dict]:
        """Every node in the subtree rooted at this node, excluding the node
        itself (recursive). Optional type filter.

        One round-trip; the LEFT JOIN keeps the root so a missing node is
        distinguishable from a childless one. An absolute-path scope knows
        the prefix client-side, so it goes in as an escaped bind param and PG
        extracts the literal prefix at plan time (Index Scan on
        ``ix_node_path_prefix``); uuid-addressed scopes derive the prefix
        from the root row inside the statement (catalog-wide scan).
        """
        frm, frm_params, where, where_params = self._node_match()
        if self._path and self._node_uuid is None:
            prefix, prefix_params = "%s || '/%%'", ["/".join(_like_escape(p) for p in self._path)]
        else:
            prefix, prefix_params = derived_prefix_like("n.path"), []
        async with self._use_read_conn() as conn:
            rows = await (
                await conn.execute(
                    rf"""
                SELECT c.uuid, c.node_type, c.name, c.data, c.parent_uuid
                FROM {frm} LEFT JOIN {P}node c
                  ON c.path LIKE {prefix} ESCAPE '\'
                 AND (%s::text IS NULL OR c.node_type = %s::text)
                WHERE {where}
                ORDER BY c.name
                """,
                    [*frm_params, *prefix_params, type, type, *where_params],
                )
            ).fetchall()
        if not rows and self._path:
            raise self._missing_error()
        return [
            {"uuid": r[0], "node_type": r[1], "name": r[2], "data": r[3], "parent_uuid": r[4]}
            for r in rows
            if r[0] is not None  # the LEFT-JOIN row of a leaf root
        ]

    async def path(self) -> Path:
        """Return the resolved path of the scope's node."""
        frm, frm_params, where, where_params = self._node_match()
        async with self._use_read_conn() as conn:
            row = await (
                await conn.execute(f"SELECT n.path FROM {frm} WHERE {where}", [*frm_params, *where_params])
            ).fetchone()
        if row is None:
            raise self._missing_error()
        return tuple(row[0].split("/"))

    # -----------------------------------------------------------------------
    # Single-element mutations
    # -----------------------------------------------------------------------

    async def rename(self, new_name: str, *, dry_run: bool = False) -> TreeDiff | None:
        """Rename this node in place: same uuid, one ``UPDATE``.

        The node's ``path`` and every descendant's ``path`` are rewritten in
        the same statement. With ``dry_run=True`` nothing is written and a
        :class:`~energydb.TreeDiff` of the pending change is returned.
        """

        async def _do(conn, node_uuid: UUID) -> None:
            # One SELECT to grab the node's current path and its parent's path,
            # then a single UPDATE rewrites path for self + every descendant
            # via the ix_node_path_prefix index. name is only changed on
            # the renamed row itself.
            row = await (
                await conn.execute(
                    f"""
                SELECT n.path AS old_path, p.path AS parent_path
                FROM {P}node n
                LEFT JOIN {P}node p ON p.uuid = n.parent_uuid
                WHERE n.uuid = %s
                """,
                    (node_uuid,),
                )
            ).fetchone()
            if row is None:
                raise NodeNotFoundError(f"Node not found: uuid={node_uuid}", uuid=node_uuid)
            old_path, parent_path = row
            new_path = f"{parent_path}/{new_name}" if parent_path else new_name

            await conn.execute(
                rf"""
                UPDATE {P}node
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

        Default is a shallow merge (Postgres ``data = data || %s``): top-level
        keys in ``data`` overwrite existing keys; nested objects are replaced,
        not deep-merged. Pass ``replace_data=True`` to fully replace the row's
        ``data`` instead. Renames go through :meth:`rename`.
        """
        op = "data = %s" if replace_data else "data = data || %s"

        async def _do(conn, node_uuid: UUID) -> None:
            await conn.execute(
                f"UPDATE {P}node SET {op}, updated_at = now() WHERE uuid = %s",
                (Jsonb(data), node_uuid),
            )

        return await self._apply_mutation(_do, dry_run=dry_run)

    async def delete(self, *, dry_run: bool = False) -> TreeDiff | None:
        """Delete this node.

        Descendants, attached edges, and series declarations go with it via
        ``ON DELETE CASCADE``; the time-series values already written to
        ClickHouse are **not** removed. With ``dry_run=True`` nothing is
        written and a :class:`~energydb.TreeDiff` is returned.
        """

        async def _do(conn, node_uuid: UUID) -> None:
            await conn.execute(f"DELETE FROM {P}node WHERE uuid = %s", (node_uuid,))

        return await self._apply_mutation(_do, dry_run=dry_run, fetch_after=False)

    async def move_to(self, target: NodeScope | Path | list[str] | str, *, dry_run: bool = False) -> TreeDiff | None:
        """Re-parent this node to ``target``.

        ``target`` is a :class:`NodeScope`, a ``/``-joined string
        (``"P/Site"``), or a tuple/list of segments. The node's ``uuid``
        (and its series) stays attached. The ``(parent_uuid, name)``
        unique constraint surfaces destination-name collisions as a
        Postgres error.

        Rejects re-parenting into self or any descendant; that would create
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
                raise ValidationError("move_to requires a non-root target.")

            if new_parent_uuid == node_uuid:
                raise ValidationError("Cannot move a node into itself.")

            # Cycle iff the prospective new parent is at or under the moving
            # node's own path. Fetch the moving node's path to Python and escape
            # it as a bind param rather than in SQL.
            subj_row = await (
                await conn.execute(
                    f"SELECT path FROM {P}node WHERE uuid = %s",
                    (node_uuid,),
                )
            ).fetchone()
            if subj_row is None:
                raise NodeNotFoundError(f"Node not found: uuid={node_uuid}", uuid=node_uuid)
            subj_path = subj_row[0]
            cycle_row = await (
                await conn.execute(
                    rf"""
                SELECT EXISTS (
                    SELECT 1 FROM {P}node cand
                    WHERE cand.uuid = %s
                      AND (cand.path = %s OR cand.path LIKE %s || '/%%' ESCAPE '\')
                )
                """,
                    (new_parent_uuid, subj_path, _like_escape(subj_path)),
                )
            ).fetchone()
            if cycle_row and cycle_row[0]:
                raise ValidationError("Cannot move a node into its own subtree (would create a cycle).")

            # Fetch old path, the new parent's path, and the moving node's own
            # name. LEFT JOIN against the new parent so a move-to-root
            # (new_parent_uuid IS NULL) returns new_parent_path = None.
            row = await (
                await conn.execute(
                    f"""
                SELECT n.path AS old_path,
                       parent.path AS new_parent_path,
                       n.name AS own_name
                FROM {P}node n
                LEFT JOIN {P}node parent ON parent.uuid = %s
                WHERE n.uuid = %s
                """,
                    (new_parent_uuid, node_uuid),
                )
            ).fetchone()
            if row is None:
                raise NodeNotFoundError(f"Node not found: uuid={node_uuid}", uuid=node_uuid)
            old_path, new_parent_path, own_name = row
            new_path = f"{new_parent_path}/{own_name}" if new_parent_path else own_name

            await conn.execute(
                rf"""
                UPDATE {P}node
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

    # -----------------------------------------------------------------------
    # Manifest builder for the shared _BaseScope read/read_relative
    # -----------------------------------------------------------------------

    def _engine_meta(self, *, data_type: str | None, name: str | None) -> PgEngineMeta | None:
        """Engine predicate for a node subtree: the path-prefix match.

        ``None`` (sequential read) when the scope can't be expressed server-side:
        JSONB/node-column ``.where()`` filters don't push through the engine view,
        and a uuid-addressed subtree's path prefix is unknown without the very
        round-trip the engine read avoids.
        """
        if self._where_filters or self._node_uuid is not None or not self._path:
            return None
        return PgEngineMeta(
            table=CH_ENGINE_TABLE,
            root_path="/".join(self._path),
            data_type=(str(data_type).lower() if data_type else None),
            name=name,
        )

    async def _build_resolved_meta(
        self,
        *,
        data_type: str | None,
        name: str | None,
    ) -> pl.DataFrame | None:
        """Resolve the scope's subtree to per-series read meta in one PG round-trip.

        Returns the per-series ``(series_id, retention, canonical_unit,
        data_type, name, node_uuid)`` frame :func:`execute_read` consumes
        directly, with no second hash-and-join pass through
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
        # Through the client's read checkout: binds the namespace GUC on
        # namespaced views (a raw pool checkout would leave it unset and RLS
        # would hide every series); autocommit still skips the implicit-BEGIN
        # round-trip. Reads are guarded against txn-bound scopes upstream.
        async with self._use_read_conn() as conn:
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
                    raise ValidationError("NodeScope has no path or uuid to resolve.")
        return None if meta.is_empty() else meta


# ---------------------------------------------------------------------------
# EdgeScope
# ---------------------------------------------------------------------------


class EdgeScope(_BaseScope):
    """Scope for operating on a single edge.

    Identified by ``uuid`` or by the ``(from_path, to_path, edge_type)``
    triple, optionally narrowed by ``edge_name``, which is what tells
    parallel edges of a multigraph apart. A triple matching several edges
    raises :class:`~energydb.errors.AmbiguousEdgeError` on resolution.
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
        edge_name: str | None = None,
        txn: Transaction | None = None,
    ):
        self._client = client
        self._edge_uuid = edge_uuid
        self._txn = txn
        self._from_path = tuple(from_path) if from_path is not None else None
        self._to_path = tuple(to_path) if to_path is not None else None
        self._edge_type = edge_type
        self._edge_name = edge_name

    def _with_txn(self, txn: Transaction) -> EdgeScope:
        return EdgeScope(
            self._client,
            edge_uuid=self._edge_uuid,
            from_path=self._from_path,
            to_path=self._to_path,
            edge_type=self._edge_type,
            edge_name=self._edge_name,
            txn=txn,
        )

    def __repr__(self) -> str:
        """Plain-text repr: no I/O."""
        if self._edge_uuid is not None and self._from_path is None:
            base = f"EdgeScope(uuid={self._edge_uuid}"
        else:
            base = (
                f"EdgeScope(from={'/'.join(self._from_path or ())!r}, "
                f"to={'/'.join(self._to_path or ())!r}, "
                f"type={self._edge_type!r}"
            )
            if self._edge_name is not None:
                base += f", name={self._edge_name!r}"
        if self._txn is not None:
            base += ", txn=True"
        return base + ")"

    # -----------------------------------------------------------------------
    # Subclass contract for _BaseScope._apply_mutation
    # -----------------------------------------------------------------------

    async def _resolve_uuid(self, conn) -> UUID:
        return await self._resolve_edge_uuid(conn)

    async def _fetch_snapshot(self, conn, uuid_: UUID):
        return (await _fetch_edges_by_uuids(conn, [uuid_])).get(uuid_)

    def _record_to_txn(self, before, after) -> None:
        assert self._txn is not None
        self._txn._record_edge(before, after)

    def _wrap_in_diff(self, before, after) -> TreeDiff:
        return TreeDiff(edge_changes=[EdgeChange(old=before, new=after)])

    def _not_found_error(self, uuid_: UUID) -> NotFoundError:
        return EdgeNotFoundError(f"Edge not found: uuid={uuid_}", uuid=uuid_)

    # -----------------------------------------------------------------------
    # Internal: identity resolution + endpoint helpers
    # -----------------------------------------------------------------------

    async def _resolve_edge_uuid(self, conn) -> UUID:
        if self._edge_uuid is not None:
            return self._edge_uuid
        if self._from_path is not None and self._to_path is not None and self._edge_type is not None:
            return await resolve_edge_uuid(conn, self._from_path, self._to_path, self._edge_type, name=self._edge_name)
        raise ValidationError("EdgeScope has no uuid or (from_path, to_path, edge_type) triple to resolve.")

    async def _fetch_edge_row(self, conn):
        """Fetch this edge's full row in ONE statement, or ``None``.

        The triple addressing joins the endpoint nodes by path inline, with
        no separate ``resolve_edge_uuid`` round-trip. Columns:
        ``(uuid, edge_type, name, data, from_node_uuid, to_node_uuid)``.

        A triple that matches several parallel edges raises
        :class:`~energydb.errors.AmbiguousEdgeError` rather than picking one.
        """
        if self._edge_uuid is not None:
            sql = f"SELECT uuid, edge_type, name, data, from_node_uuid, to_node_uuid FROM {P}edge WHERE uuid = %s"
            params: list[Any] = [self._edge_uuid]
        elif self._from_path is not None and self._to_path is not None and self._edge_type is not None:
            sql = (
                "SELECT e.uuid, e.edge_type, e.name, e.data, e.from_node_uuid, e.to_node_uuid "
                f"FROM {P}edge e "
                f"JOIN {P}node fn ON fn.uuid = e.from_node_uuid "
                f"JOIN {P}node tn ON tn.uuid = e.to_node_uuid "
                "WHERE fn.path = %s AND tn.path = %s AND e.edge_type = %s"
            )
            params = ["/".join(self._from_path), "/".join(self._to_path), self._edge_type]
            if self._edge_name is not None:
                sql += " AND e.name = %s"
                params.append(self._edge_name)
        else:
            raise ValidationError("EdgeScope has no uuid or (from_path, to_path, edge_type) triple to resolve.")
        rows = await (await conn.execute(sql, params)).fetchall()
        if len(rows) > 1:
            # uuid- and name-narrowed addressing are unique by construction, so
            # this is always a bare triple over parallel edges.
            assert self._from_path is not None and self._to_path is not None and self._edge_type is not None
            raise ambiguous_edge_error(
                from_path="/".join(self._from_path),
                to_path="/".join(self._to_path),
                edge_type=self._edge_type,
                matches=[(r[0], r[2]) for r in rows],
            )
        return rows[0] if rows else None

    async def _edge_not_found(self, conn):
        """Raise the not-found error specific to this addressing.

        Error path only: the triple form re-runs ``resolve_edge_uuid`` so a
        missing endpoint path keeps its own message, distinct from a missing
        edge.
        """
        if self._edge_uuid is not None:
            raise EdgeNotFoundError(f"Edge not found: uuid={self._edge_uuid}", uuid=self._edge_uuid)
        assert self._from_path is not None and self._to_path is not None and self._edge_type is not None
        joined_from, joined_to = "/".join(self._from_path), "/".join(self._to_path)
        await resolve_edge_uuid(conn, self._from_path, self._to_path, self._edge_type, name=self._edge_name)
        raise EdgeNotFoundError(
            f"Edge not found: {edge_address_repr(joined_from, joined_to, self._edge_type, self._edge_name)}",
            from_path=joined_from,
            to_path=joined_to,
            edge_type=self._edge_type,
            name=self._edge_name,
        )

    async def _endpoints(self, conn) -> tuple[UUID, UUID]:
        """Fetch ``(from_node_uuid, to_node_uuid)`` for this edge in one query."""
        row = await self._fetch_edge_row(conn)
        if row is None:
            await self._edge_not_found(conn)
        return row[4], row[5]

    # -----------------------------------------------------------------------
    # get / navigation
    # -----------------------------------------------------------------------

    async def get(self):
        """Reconstruct this edge as an EnergyDataModel object, endpoints
        included.

        Raises :class:`~energydb.errors.EdgeNotFoundError` when the uuid or
        the ``(from_path, to_path, type)`` triple matches no edge.
        """
        async with self._use_read_conn() as conn:
            row = await self._fetch_edge_row(conn)
            if row is None:
                await self._edge_not_found(conn)
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

    async def get_raw(self) -> dict | None:
        """Fetch this edge as a raw dict, without EDM reconstruction.

        Returns ``{uuid, edge_type, name, data, from_node_uuid, to_node_uuid}``
        or ``None`` if the uuid-addressed edge does not exist (a triple-addressed
        miss raises, matching the resolve contract). The light way to fetch an
        edge's uuid, mirroring :meth:`NodeScope.get_raw`: no EDM reconstruction,
        so it works for any ``edge_type`` string where :meth:`get` would raise on
        an unregistered EDM type.
        """
        async with self._use_read_conn() as conn:
            row = await self._fetch_edge_row(conn)
            if row is None:
                if self._edge_uuid is None:
                    await self._edge_not_found(conn)
                return None
        return {
            "uuid": row[0],
            "edge_type": row[1],
            "name": row[2],
            "data": row[3],
            "from_node_uuid": row[4],
            "to_node_uuid": row[5],
        }

    async def from_node(self) -> NodeScope:
        """Return a :class:`NodeScope` on this edge's source endpoint."""
        async with self._use_read_conn() as conn:
            from_uuid, _ = await self._endpoints(conn)
        return NodeScope(self._client, node_uuid=from_uuid, txn=self._txn)

    async def to_node(self) -> NodeScope:
        """Return a :class:`NodeScope` on this edge's target endpoint."""
        async with self._use_read_conn() as conn:
            _, to_uuid = await self._endpoints(conn)
        return NodeScope(self._client, node_uuid=to_uuid, txn=self._txn)

    # -----------------------------------------------------------------------
    # CRUD
    # -----------------------------------------------------------------------

    async def rename(self, new_name: str, *, dry_run: bool = False) -> TreeDiff | None:
        """Rename this edge in place: same uuid, one ``UPDATE``.

        The name is part of the edge's unique key, so renaming onto a
        ``(edge_type, from, to, name)`` quadruple that a parallel edge already
        occupies raises :class:`~energydb.errors.AlreadyExistsError`.

        With ``dry_run=True`` nothing is written and a
        :class:`~energydb.TreeDiff` of the pending change is returned.
        """

        async def _do(conn, edge_uuid: UUID) -> None:
            async with map_edge_conflict():
                await conn.execute(
                    f"UPDATE {P}edge SET name = %s, updated_at = now() WHERE uuid = %s",
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
                f"UPDATE {P}edge SET {op}, updated_at = now() WHERE uuid = %s",
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

        The edge's ``uuid`` (and its series) stays attached. Landing on a
        ``(edge_type, from_node_uuid, to_node_uuid, name)`` quadruple that is
        already taken raises :class:`~energydb.errors.AlreadyExistsError`;
        give the edge a distinct ``name`` (see :meth:`rename`) to park two
        parallel edges on the same endpoint pair.
        """

        async def _do(conn, edge_uuid: UUID) -> None:
            new_from_uuid = await _resolve_endpoint(conn, from_node)
            new_to_uuid = await _resolve_endpoint(conn, to_node)
            if new_from_uuid == new_to_uuid:
                raise ValidationError("Edge endpoints must be distinct nodes.")
            async with map_edge_conflict():
                await conn.execute(
                    f"UPDATE {P}edge SET from_node_uuid = %s, to_node_uuid = %s, updated_at = now() WHERE uuid = %s",
                    (new_from_uuid, new_to_uuid, edge_uuid),
                )

        return await self._apply_mutation(_do, dry_run=dry_run)

    async def delete(self, *, dry_run: bool = False) -> TreeDiff | None:
        """Delete this edge and its series declarations.

        The endpoint nodes are untouched, and values already in ClickHouse
        are not removed. With ``dry_run=True`` nothing is written and a
        :class:`~energydb.TreeDiff` is returned.
        """

        async def _do(conn, edge_uuid: UUID) -> None:
            await conn.execute(f"DELETE FROM {P}edge WHERE uuid = %s", (edge_uuid,))

        return await self._apply_mutation(_do, dry_run=dry_run, fetch_after=False)

    # -----------------------------------------------------------------------
    # Manifest builder for the shared _BaseScope read/read_relative
    # -----------------------------------------------------------------------

    def _engine_meta(self, *, data_type: str | None, name: str | None) -> PgEngineMeta | None:
        """Engine predicate for an edge read: owner-uuid match or the exact triple.

        Both edge addressings are expressible server-side (the view carries
        ``edge_uuid`` / ``edge_type`` / ``from_path`` / ``to_path``), so edge
        reads run the resolve and the value read in parallel too.

        The triple predicate stays triple-only for a name-narrowed scope: it
        resolves the superset of parallel edges, which the caller trims against
        the exactly-resolved meta, same contract as the set-valued manifest
        predicates. An ambiguous (nameless) triple never gets that far; the
        parallel PG resolve raises before the values are used.
        """
        dt = str(data_type).lower() if data_type else None
        if self._edge_uuid is not None:
            return PgEngineMeta(table=CH_ENGINE_TABLE, edge_uuids=(str(self._edge_uuid),), data_type=dt, name=name)
        if self._from_path is not None and self._to_path is not None and self._edge_type is not None:
            return PgEngineMeta(
                table=CH_ENGINE_TABLE,
                edge_triple=("/".join(self._from_path), "/".join(self._to_path), self._edge_type),
                data_type=dt,
                name=name,
            )
        return None

    async def _build_resolved_meta(
        self,
        *,
        data_type: str | None,
        name: str | None,
    ) -> pl.DataFrame | None:
        """Resolve this edge to per-series read meta in ONE PG round-trip.

        Both addressings go through :func:`series.resolve_edge_series_for_read`
        directly; the triple form collapses the paths → edge → series chain
        (3 round-trips) into a single query. ``None`` if no series match.
        """
        data_type_str = str(data_type).lower() if data_type else None
        # Client read checkout (namespace GUC) — see NodeScope._build_resolved_meta.
        async with self._use_read_conn() as conn:
            with profiling._phase(profiling.PHASE_EDB_RESOLVE):
                if self._edge_uuid is not None:
                    meta = await series_mod.resolve_edge_series_for_read(
                        conn,
                        edge_uuid=self._edge_uuid,
                        data_type=data_type_str,
                        name=name,
                    )
                elif self._from_path is not None and self._to_path is not None and self._edge_type is not None:
                    meta = await series_mod.resolve_edge_series_for_read(
                        conn,
                        from_path="/".join(self._from_path),
                        to_path="/".join(self._to_path),
                        edge_type=self._edge_type,
                        edge_name=self._edge_name,
                        data_type=data_type_str,
                        name=name,
                    )
                else:
                    raise ValidationError("EdgeScope has no uuid or (from_path, to_path, edge_type) triple to resolve.")
        return None if meta.is_empty() else meta
