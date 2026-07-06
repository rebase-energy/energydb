"""Client — owns the psycopg pool and constructs TimeDBClient.

UUID identity model:

* A node is uniquely identified by its ``uuid`` (UUID7, set on the EDM
  Element at construction).
* An edge is uniquely identified by its ``uuid`` (also UUID7).
* Path-based addressing (``client.get_node("Europe", "Sweden")``) is
  preserved as a user-friendly fluent CLI; resolution walks
  ``(parent_uuid, name)`` via one indexed recursive CTE.
* Edge endpoints in storage are ``from_node_uuid`` / ``to_node_uuid`` —
  no path resolution at write or read time.

API split:

* ``register_tree`` — structure (nodes, edges, series declarations). Create-only;
  raises if any UUID in the payload already exists, or on inline timeseries data.
* ``write`` / ``read`` — bulk timeseries data via manifest DataFrames.
* ``get_node`` / ``get_edge`` — fluent scope entry points. Reads like
  English: ``client.get_node("p").where(type="WindTurbine").read()``.
  Terminate with ``.get()`` to fetch the EDM object eagerly.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    from energydb._transaction import Transaction

import pandas as pd
import polars as pl
from psycopg_pool import AsyncConnectionPool
from sqlalchemy import create_engine
from timedatamodel import DataType, TimeSeries, TimeSeriesType
from timedb import TimeDBClient, UnchangedScope, profiling

from energydb import runs as runs_mod
from energydb._fast_read import engine_table_ddl
from energydb._frames import Backend, Output, to_backend, to_polars
from energydb._io import WriteResult, read_manifest, read_relative_manifest, write_manifest
from energydb._join import EdgeSeriesKey, SeriesKey
from energydb._persist import create_edge, create_node_raw, register_tree_under
from energydb.diff import TreeDiff
from energydb.models import CREATE_SERIES_META_VIEW, SCHEMA, Base
from energydb.paths import (
    Path,
    _like_escape,
    build_filter_conditions,
    resolve_node_uuid,
    resolve_subtree_uuids,
)
from energydb.scope import EdgeScope, NodeScope, _coerce_path
from energydb.serialization import reconstruct_edge, reconstruct_node

_SEARCH_PATH = f"SET search_path TO {SCHEMA}, public" if SCHEMA else "SET search_path TO public"


class AsyncClient:
    """Async-native client for energy assets, hierarchy, and time series.

    Owns the psycopg ``AsyncConnectionPool`` (used for all PG ops) and
    constructs a :class:`TimeDBClient` for ClickHouse I/O. Every PG
    round-trip is awaited; the ClickHouse leg (sync ``clickhouse-connect``)
    is offloaded to a worker thread. Synchronous callers use
    :class:`energydb.Client`, a thin blocking facade over this class.
    """

    def __init__(
        self,
        *,
        pg_conninfo: str | None = None,
        ch_url: str | None = None,
        strategy: str = "today",
    ):
        """Construct a client.

        ``strategy`` is the default read strategy (``"today"`` | ``"concurrent"``);
        a per-read ``strategy=`` overrides it. ``concurrent`` requires
        :meth:`setup_ch_fast_read` to have provisioned the ClickHouse engine table,
        and falls back to ``"today"`` on any gate-miss or error.
        """
        conninfo = pg_conninfo or os.environ.get("TIMEDB_PG_DSN") or os.environ.get("DATABASE_URL")
        if not conninfo:
            raise ValueError("PostgreSQL connection not configured. Pass pg_conninfo or set TIMEDB_PG_DSN.")
        if "://" not in conninfo:
            raise ValueError(
                "pg_conninfo must be a URI (e.g. postgresql://user:pass@host/db); "
                "key=value DSNs are not supported here because the schema-create path "
                "needs a SQLAlchemy URL."
            )
        self._dsn = conninfo

        async def _configure(conn):
            await conn.execute(_SEARCH_PATH)
            # ``prepare_threshold=1`` makes psycopg cache a server-side
            # prepared statement after the first execution of each SQL text.
            # Saves ~4-8ms on the repeated 6000-uuid resolve query at scale=200
            # (PG parse+plan stage skipped on subsequent calls).
            conn.prepare_threshold = 1
            await conn.commit()

        self._pool = AsyncConnectionPool(
            conninfo=conninfo,
            min_size=1,
            max_size=10,
            open=False,
            configure=_configure,
        )
        self.td = TimeDBClient(ch_url=ch_url)
        self._strategy = strategy

    async def open(self) -> None:
        """Open the async connection pool. Await once before first use."""
        await self._pool.open()

    def _safe_dsn(self) -> str:
        """The DSN with the userinfo segment (user:pass) replaced by ``***``.

        Shows scheme + host(:port) + db. Pure formatting — no I/O. Shared by
        :meth:`__repr__` and the sync :class:`energydb.Client` facade.
        """
        dsn = self._dsn
        if "://" in dsn:
            scheme, rest = dsn.split("://", 1)
            if "@" in rest:
                _userinfo, hostpart = rest.split("@", 1)
                return f"{scheme}://***@{hostpart}"
        return dsn

    def __repr__(self) -> str:
        return f"AsyncClient(pg={self._safe_dsn()!r})"

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    async def create(self) -> None:
        """Create PG schema + CH tables.

        Schema is defined by the SQLAlchemy models in :mod:`energydb.models`.
        SQLAlchemy's ``create_all`` and TimeDB's create are synchronous, so they
        run in a worker thread to keep the event loop free.
        """
        await asyncio.to_thread(self._create_blocking)

    def _create_blocking(self) -> None:
        engine = create_engine(self._sqlalchemy_url())
        try:
            Base.metadata.create_all(engine, checkfirst=True)
        finally:
            engine.dispose()
        self.td.create()

    async def delete(self) -> None:
        """Drop EnergyDB's tables and CH tables.

        With a named schema, drops the whole schema (CASCADE). With the
        default ``public`` schema (``SCHEMA is None``), drops only EnergyDB's
        own four tables — never the shared ``public`` schema, which would take
        the host application's tables with it.
        """
        async with self._pool.connection() as conn:
            if SCHEMA is None:
                await conn.execute("DROP TABLE IF EXISTS series, runs, edge, node CASCADE")
            else:
                await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
            await conn.commit()
        await asyncio.to_thread(self.td.delete)

    async def setup_ch_fast_read(self) -> None:
        """Provision the ClickHouse scaffolding for the ``concurrent`` read strategy.

        Idempotent. (Re)creates the PG ``series_meta`` view and the ClickHouse ``PostgreSQL()``
        engine table over it (PG creds taken inline from the client DSN -- works with an
        unprivileged CH role). Call once per deployment before enabling ``concurrent``;
        safe to call repeatedly.
        """
        async with self._pool.connection() as conn:
            await conn.execute(CREATE_SERIES_META_VIEW)
            await conn.commit()
        schema = os.environ.get("ENERGYDB_SCHEMA", "public")
        await asyncio.to_thread(self.td._ch.command, engine_table_ddl(self._dsn, schema))

    async def close(self) -> None:
        await self._pool.close()

    # ------------------------------------------------------------------
    # Fluent entry — scopes for navigation & single-element ops
    # ------------------------------------------------------------------

    def get_node(self, *names_or_path, uuid: UUID | None = None) -> NodeScope:
        """Return a :class:`NodeScope` for a node or subtree.

        ``client.get_node("P/Site/T01")``            — canonical ``/``-joined string
        ``client.get_node("P", "Site", "T01")``      — variadic — equivalent
        ``client.get_node(("P", "Site", "T01"))``    — tuple/list path
        ``client.get_node(uuid=...)``                — absolute by uuid

        ``/`` is reserved as the path separator; names containing ``/``
        are rejected at registration time. Empty segments (leading,
        trailing, or doubled ``/``) raise ``ValueError``.

        Terminate the chain with ``.get()`` to fetch the EDM object,
        ``.read()`` for time-series data, ``.where(...)`` to filter a
        subtree, etc.
        """
        if uuid is not None:
            if names_or_path:
                raise ValueError("Pass either uuid= or names, not both.")
            return NodeScope(self, node_uuid=uuid)
        if not names_or_path:
            raise ValueError("Provide a path or uuid=.")
        return NodeScope(self, path=_coerce_path(names_or_path))

    def get_edge(
        self,
        from_path: Path | list[str] | str | None = None,
        to_path: Path | list[str] | str | None = None,
        *,
        type: str | None = None,
        uuid: UUID | None = None,
    ) -> EdgeScope:
        """Return an :class:`EdgeScope` by uuid or by ``(from_path, to_path, type)``.

        ``from_path`` / ``to_path`` accept the canonical ``/``-joined string
        form (``"P/Site/T01"``) or a tuple/list of segments. Terminate with
        ``.get()`` to fetch the EDM edge eagerly.
        """
        if uuid is not None:
            if from_path is not None or to_path is not None or type is not None:
                raise ValueError("Pass uuid= alone, or (from_path, to_path, type=) — not both.")
            return EdgeScope(self, edge_uuid=uuid)
        if from_path is None or to_path is None or type is None:
            raise ValueError("Provide uuid= or (from_path, to_path, type=).")
        return EdgeScope(
            self,
            from_path=_coerce_path((), kwarg=from_path),
            to_path=_coerce_path((), kwarg=to_path),
            edge_type=type,
        )

    # ------------------------------------------------------------------
    # Transactions
    # ------------------------------------------------------------------

    def transaction(self) -> Transaction:
        """Open an atomic batch of scope mutations.

        Returns a :class:`Transaction` context manager. Mutations executed
        through ``txn.get_node(...)`` / ``txn.get_edge(...)`` /
        ``txn.register_tree(...)`` apply immediately to the open
        transaction's connection but are not committed until
        :meth:`Transaction.commit` is called explicitly. Exit without
        commit raises and rolls back.

        Time-series I/O (``scope.write(df, ...)`` / ``scope.read(...)``)
        inside a transaction does **not** participate in atomicity — it
        executes immediately against the pool / ClickHouse.
        """
        from energydb._transaction import Transaction

        return Transaction(self)

    # ------------------------------------------------------------------
    # Structure — register_tree, edges, queries
    # ------------------------------------------------------------------

    async def register_tree(
        self,
        edm_obj,
        *,
        under: Path | list[str] | str | None = None,
        dry_run: bool = False,
    ) -> UUID | TreeDiff:
        """Persist an EDM tree's structure: nodes, edges, series declarations.

        Create-only. Raises :class:`ValueError` if any UUID in the payload
        already exists in the DB; modify existing rows via scope mutators
        (:meth:`NodeScope.rename`, ``.update``, ``.delete``, ``.move_to``)
        or batch them with :meth:`transaction`.

        ``dry_run=True`` returns the computed :class:`TreeDiff` without
        committing — the transaction is rolled back so no DB state changes.

        Inline ``TimeSeries.df`` data is rejected: write data separately
        via :meth:`write` against a manifest. ``under`` selects the parent
        under which the tree's root is grafted; ``None`` means create at
        root. Raises if ``under`` points at a non-existent parent.

        Series declarations attached to nodes/edges on the tree **are**
        registered alongside their owners but are not represented in the
        returned :class:`TreeDiff`. Adding a series to a node that
        already exists in the DB is not supported here (the create-only
        pre-check rejects the whole payload); use
        :meth:`NodeScope.register_series` /
        :meth:`EdgeScope.register_series` instead.

        Returns the ``uuid`` of the tree's root, except when
        ``dry_run=True`` (which returns the :class:`TreeDiff`).
        """
        async with self._pool.connection() as conn:
            parent_uuid = await resolve_node_uuid(conn, _coerce_path((), kwarg=under)) if under is not None else None
            root_uuid, diff = await register_tree_under(
                conn,
                edm_obj,
                parent_uuid=parent_uuid,
                dry_run=dry_run,
            )
            if dry_run:
                await conn.rollback()
            else:
                await conn.commit()
        if dry_run:
            return diff
        return root_uuid

    async def query_nodes(
        self,
        *,
        type: str | None = None,
        within: Path | list[str] | str | UUID | None = None,
        **property_filters,
    ) -> list:
        """Return matching nodes as a flat list of EDM objects.

        ``within`` accepts a ``/``-joined string (``"P/Site"``), a path
        tuple/list of segments, or a :class:`UUID`.
        """
        where_filters: dict[str, Any] = dict(property_filters)
        if type is not None:
            where_filters["node_type"] = type

        async with self._pool.connection() as conn:
            filter_conds, filter_params = build_filter_conditions(where_filters, type_col="node_type")
            conditions: list[str] = list(filter_conds)
            params: list[Any] = list(filter_params)

            if within is not None:
                within_uuid = (
                    within
                    if isinstance(within, UUID)
                    else await resolve_node_uuid(conn, _coerce_path((), kwarg=within))
                )
                conditions.append("uuid = ANY(%s)")
                params.append(await resolve_subtree_uuids(conn, within_uuid))

            where = " AND ".join(conditions) if conditions else "TRUE"
            rows = await (
                await conn.execute(
                    f"SELECT uuid, node_type, name, data FROM node WHERE {where} ORDER BY name",
                    params,
                )
            ).fetchall()

        return [reconstruct_node({"uuid": r[0], "node_type": r[1], "name": r[2], "data": r[3]}) for r in rows]

    async def create_edge(self, edm_obj) -> UUID:
        """Upsert an edge between two existing nodes. Idempotent.

        The edge's :class:`Reference` endpoints (``from_element`` /
        ``to_element``) carry the endpoint UUIDs directly — no path
        resolution. The endpoints must already exist as nodes; the FK
        constraint will fail otherwise.

        For edges that are part of a tree, prefer :meth:`register_tree` —
        it walks the structure and validates endpoints against the tree's
        index in one pass.
        """
        async with self._pool.connection() as conn:
            edge_uuid = await create_edge(conn, edm_obj, tree_root=None)
            await conn.commit()
        return edge_uuid

    # ------------------------------------------------------------------
    # Generic raw node API — store/read by (node_type, data), no EDM class
    # ------------------------------------------------------------------

    async def create_node(
        self,
        *,
        node_type: str,
        name: str,
        data: dict | None = None,
        parent: UUID | Path | list[str] | str | None = None,
        uuid: UUID | None = None,
    ) -> UUID:
        """Create a single node from a type slug + JSONB ``data`` — no EDM class.

        Generic counterpart to :meth:`register_tree`: ``node_type`` is stored
        as a free-form string and ``data`` verbatim, bypassing EnergyDataModel
        (de)serialization. ``parent`` selects the parent node (UUID or path);
        ``None`` creates a root. ``uuid`` is minted (uuid7) when omitted. Read
        these nodes back with :meth:`get_node_raw` / :meth:`get_subtree_raw` or
        ``NodeScope.children()`` — not the EDM readers, which require a
        registered type.
        """
        async with self._pool.connection() as conn:
            if parent is None:
                parent_uuid = None
            elif isinstance(parent, UUID):
                parent_uuid = parent
            else:
                parent_uuid = await resolve_node_uuid(conn, _coerce_path((), kwarg=parent))
            new_uuid = await create_node_raw(
                conn,
                node_type=node_type,
                name=name,
                data=data,
                parent_uuid=parent_uuid,
                uuid=uuid,
            )
            await conn.commit()
        return new_uuid

    async def get_node_raw(self, node_uuid: UUID) -> dict | None:
        """Fetch one node as a raw dict, without EDM reconstruction.

        Returns ``{uuid, node_type, name, data, parent_uuid}`` or ``None`` if
        the node does not exist. Safe for any ``node_type`` string, unlike
        :meth:`get_node` / :meth:`get_tree`.
        """
        async with self._pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT uuid, node_type, name, data, parent_uuid, path, created_at, updated_at "
                    "FROM node WHERE uuid = %s",
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
            "created_at": row[6],
            "updated_at": row[7],
        }

    async def get_subtree_raw(self, root_uuid: UUID) -> list[dict]:
        """Return the node + every descendant as raw dicts (no EDM reconstruction).

        Indexed materialized-path prefix scan (``ix_node_path_prefix``). Each
        dict is ``{uuid, node_type, name, data, parent_uuid, path}``. Includes
        the root itself; empty list if the root does not exist.
        """
        async with self._pool.connection() as conn:
            root = await (
                await conn.execute(
                    "SELECT path FROM node WHERE uuid = %s",
                    (root_uuid,),
                )
            ).fetchone()
            if root is None:
                return []
            root_path = root[0]
            rows = await (
                await conn.execute(
                    r"""
                SELECT uuid, node_type, name, data, parent_uuid, path, created_at, updated_at
                FROM node
                WHERE path = %s OR path LIKE %s || '/%%' ESCAPE '\'
                ORDER BY path
                """,
                    (root_path, _like_escape(root_path)),
                )
            ).fetchall()
        return [
            {
                "uuid": r[0],
                "node_type": r[1],
                "name": r[2],
                "data": r[3],
                "parent_uuid": r[4],
                "path": r[5],
                "created_at": r[6],
                "updated_at": r[7],
            }
            for r in rows
        ]

    async def list_series(self, owner_uuid: UUID, *, owner_col: str = "node_uuid") -> list[dict]:
        """List the series catalog owned by a node (or edge).

        Returns ``{name, data_type, canonical_unit, timeseries_type,
        description}`` per series. ``owner_col`` is ``"node_uuid"`` (default)
        or ``"edge_uuid"``.
        """
        if owner_col not in ("node_uuid", "edge_uuid"):
            raise ValueError("owner_col must be 'node_uuid' or 'edge_uuid'")
        async with self._pool.connection() as conn:
            rows = await (
                await conn.execute(
                    f"SELECT name, data_type, canonical_unit, timeseries_type, description "
                    f"FROM series WHERE {owner_col} = %s ORDER BY data_type, name",
                    (owner_uuid,),
                )
            ).fetchall()
        return [
            {
                "name": r[0],
                "data_type": r[1],
                "canonical_unit": r[2],
                "timeseries_type": r[3],
                "description": r[4],
            }
            for r in rows
        ]

    async def query_edges(
        self,
        *,
        type: str | None = None,
        within: Path | list[str] | str | UUID | None = None,
        **property_filters,
    ) -> list:
        """Return matching edges as a flat list of EDM objects.

        ``within`` (``/``-joined string ``"P/Site"``, path tuple/list of
        segments, or a :class:`UUID`) restricts to edges where either
        endpoint is in that subtree.
        """
        where_filters: dict[str, Any] = dict(property_filters)
        if type is not None:
            where_filters["edge_type"] = type

        async with self._pool.connection() as conn:
            filter_conds, filter_params = build_filter_conditions(where_filters, type_col="edge_type")
            conditions: list[str] = list(filter_conds)
            params: list[Any] = list(filter_params)

            if within is not None:
                within_uuid = (
                    within
                    if isinstance(within, UUID)
                    else await resolve_node_uuid(conn, _coerce_path((), kwarg=within))
                )
                subtree = await resolve_subtree_uuids(conn, within_uuid)
                conditions.append("(from_node_uuid = ANY(%s) OR to_node_uuid = ANY(%s))")
                params.append(subtree)
                params.append(subtree)

            where = " AND ".join(conditions) if conditions else "TRUE"
            rows = await (
                await conn.execute(
                    f"SELECT uuid, edge_type, name, data, from_node_uuid, to_node_uuid "
                    f"FROM edge WHERE {where} ORDER BY name NULLS LAST",
                    params,
                )
            ).fetchall()
            if not rows:
                return []

        return [
            reconstruct_edge(
                {
                    "uuid": r[0],
                    "edge_type": r[1],
                    "name": r[2],
                    "data": r[3],
                    "from_node_uuid": r[4],
                    "to_node_uuid": r[5],
                }
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Tree reconstruction
    # ------------------------------------------------------------------

    async def get_tree(
        self,
        *names_or_path,
        uuid: UUID | None = None,
        include_series: bool = False,
    ):
        """Reconstruct the full EDM subtree rooted at the given node.

        With ``include_series=True``, every reconstructed node has its
        registered series attached as metadata-only :class:`TimeSeries`
        entries (``df=None``) on ``timeseries``.

        **Edges are intentionally not attached to the returned tree.**
        The result is a node-only subtree walked via ``parent_uuid``.
        Edges (and their series) live alongside nodes in the schema but
        outside the tree shape — query them separately with
        :meth:`get_edge` or :meth:`query_edges`.
        """
        async with self._pool.connection() as conn:
            if uuid is not None:
                root_uuid = uuid
            elif names_or_path:
                root_uuid = await resolve_node_uuid(conn, _coerce_path(names_or_path))
            else:
                raise ValueError("Provide a path or uuid=.")

            # Two-step: fetch the root's path, then LIKE with the escaped
            # prefix as a bind param so PG picks Index Scan on
            # ``ix_node_path_prefix``. A column-source LIKE would Seq Scan.
            root_path_row = await (
                await conn.execute(
                    "SELECT path FROM node WHERE uuid = %s",
                    (root_uuid,),
                )
            ).fetchone()
            if root_path_row is None:
                raise ValueError(f"Node not found: uuid={root_uuid}")
            root_path = root_path_row[0]
            rows = await (
                await conn.execute(
                    r"""
                SELECT uuid, node_type, name, data, parent_uuid
                FROM node
                WHERE path = %s OR path LIKE %s || '/%%' ESCAPE '\'
                """,
                    (root_path, _like_escape(root_path)),
                )
            ).fetchall()

            nodes: dict[UUID, Any] = {}
            parent_map: dict[UUID, UUID | None] = {}
            for r in rows:
                node_uuid = r[0]
                parent_map[node_uuid] = r[4]
                nodes[node_uuid] = reconstruct_node({"uuid": r[0], "node_type": r[1], "name": r[2], "data": r[3]})

            if include_series:
                series_rows = await (
                    await conn.execute(
                        "SELECT node_uuid, data_type, name, canonical_unit, timeseries_type, description "
                        "FROM series WHERE node_uuid = ANY(%s)",
                        (list(nodes.keys()),),
                    )
                ).fetchall()
                for nid, dt, sname, unit, tstype, desc in series_rows:
                    node_obj = nodes.get(nid)
                    if node_obj is None:
                        continue
                    series = TimeSeries(
                        df=None,
                        name=sname,
                        unit=unit or "dimensionless",
                        data_type=DataType(dt.upper()) if dt else None,
                        timeseries_type=TimeSeriesType(tstype) if tstype else TimeSeriesType.FLAT,
                        description=desc,
                    )
                    if node_obj.timeseries is None:
                        node_obj.timeseries = []
                    node_obj.timeseries.append(series)

        # Attach children to their parents (flat pass — uuid-based, order-agnostic).
        for node_uuid, parent_uuid in parent_map.items():
            if parent_uuid is not None and parent_uuid in nodes:
                nodes[parent_uuid].add_child(nodes[node_uuid])

        return nodes[root_uuid]

    # ------------------------------------------------------------------
    # Bulk timeseries I/O — manifest DataFrames only
    # ------------------------------------------------------------------

    async def write(
        self,
        df: pl.DataFrame | pd.DataFrame,
        *,
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
        """Bulk-write timeseries data via a routing manifest.

        ``df`` is a pandas or polars DataFrame carrying one routing column
        (``node_uuid``, ``edge_uuid``, or ``path`` as ``Utf8`` joined with
        ``/``, e.g. ``"my-portfolio/Offshore-1/T01"``), plus ``data_type``,
        ``name``, and the timedb data columns (``valid_time``, ``value``,
        optional ``knowledge_time``). Optional ``unit`` column triggers
        per-row unit conversion to each series's canonical unit.

        ``skip_unchanged`` (with ``unchanged_scope``) drops rows whose latest
        stored value is unchanged before the insert; see :func:`timedb.write`.

        Series must already be registered (typically via
        :meth:`register_tree`). Returns a :class:`WriteResult` — an ``int``
        run_id carrying ``written`` / ``skipped`` counts.
        """
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            df_pl = to_polars(df)
        return await write_manifest(
            self._pool,
            self.td,
            df_pl,
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
        df: pl.DataFrame | pd.DataFrame,
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
        """Bulk read via manifest. Detects edge vs node routing automatically.

        Accepts pandas or polars on input. Output shape:

        * ``output="frame"`` (default): a single DataFrame with columns
          ``(path, data_type, name, valid_time, value, …)`` for node-routed
          reads, or ``(from_path, to_path, edge_type, data_type, name,
          valid_time, value, …)`` for edge-routed reads. ``path`` /
          ``from_path`` / ``to_path`` are ``Utf8`` joined with ``/``.
          Optional columns appear when ``include_knowledge_time`` /
          ``include_updates`` are set.
        * ``output="by_path"``: a ``dict`` keyed by
          :class:`SeriesKey` (node-routed: ``path``, ``data_type``, ``name``)
          or :class:`EdgeSeriesKey` (edge-routed: ``from_path``, ``to_path``,
          ``edge_type``, ``data_type``, ``name``) with per-series DataFrames
          carrying only the data columns (``valid_time``, ``value``, plus
          opt-in time/audit columns). Keys are NamedTuples — positional
          access (``result[(path, dt, name)]``) and attribute access
          (``key.path``) both work. Each sub-frame is sorted by
          ``valid_time`` ascending; secondary sort keys are
          ``knowledge_time`` and/or ``change_time`` when requested.

        ``backend="polars"`` (default) returns polars frames;
        ``backend="pandas"`` converts at the boundary. Internal
        identifiers (``series_id``, ``node_uuid``, ``edge_uuid``) are
        never exposed on the result.
        """
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            manifest = to_polars(df)
        result = await read_manifest(
            self._pool,
            self.td,
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
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            return to_backend(result, backend)

    async def read_relative(
        self,
        df: pl.DataFrame | pd.DataFrame,
        *,
        unit: str | None = None,
        output: Output = "frame",
        backend: Backend = "polars",
        **td_kwargs,
    ) -> (
        pl.DataFrame
        | pd.DataFrame
        | dict[SeriesKey, pl.DataFrame]
        | dict[SeriesKey, pd.DataFrame]
        | dict[EdgeSeriesKey, pl.DataFrame]
        | dict[EdgeSeriesKey, pd.DataFrame]
    ):
        """Bulk relative read via manifest.

        See :meth:`read` for the ``output`` / ``backend`` contract.
        ``**td_kwargs`` are forwarded to :meth:`timedb.TimeDBClient.read_relative`;
        see that signature for accepted arguments (window selectors, etc.).
        """
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            manifest = to_polars(df)
        result = await read_relative_manifest(
            self._pool,
            self.td,
            manifest,
            unit=unit,
            output=output,
            **td_kwargs,
        )
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            return to_backend(result, backend)

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    async def read_runs_for_series(self, *, series_id: int) -> list[dict[str, Any]]:
        """Return runs that wrote data for a given series_id, latest first."""
        run_ids = await asyncio.to_thread(self.td.read_run_series, series_id=series_id)
        if not run_ids:
            return []
        async with self._pool.connection() as conn:
            return await runs_mod.get_runs(conn, run_ids)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _sqlalchemy_url(self) -> str:
        return f"postgresql+psycopg://{self._dsn.split('://', 1)[-1]}"
