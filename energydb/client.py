"""Client: owns the psycopg pool and constructs TimeDBClient.

UUID identity model:

* A node is uniquely identified by its ``uuid`` (UUID7, set on the EDM
  Element at construction).
* An edge is uniquely identified by its ``uuid`` (also UUID7).
* Path-based addressing (``client.get_node("Europe", "Sweden")``) resolves
  ``(parent_uuid, name)`` via one indexed recursive CTE.
* Edge endpoints in storage are ``from_node_uuid`` / ``to_node_uuid``, so
  there is no path resolution at write or read time.

API split:

* ``register_tree``: structure (nodes, edges, series declarations). Create-only;
  raises if any UUID in the payload already exists, or on inline timeseries data.
* ``write`` / ``read``: bulk timeseries data via manifest DataFrames.
* ``get_node`` / ``get_edge``: fluent scope entry points, e.g.
  ``client.get_node("p").where(type="WindTurbine").read()``. Terminate with
  ``.get()`` to fetch the EDM object eagerly.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

if TYPE_CHECKING:
    import psycopg

    from energydb._transaction import Transaction

import pandas as pd
import polars as pl
from psycopg_pool import AsyncConnectionPool
from sqlalchemy import create_engine
from timedatamodel import DataType, TimeSeries, TimeSeriesType
from timedb import TimeDBClient, UnchangedScope, profiling

from energydb import runs as runs_mod
from energydb._ch_meta_engine import (
    CH_ENGINE_TABLE,
    DROP_ENGINE_TABLE,
    DROP_LEGACY_ENGINE_TABLE,
    engine_pg_host,
    engine_table_ddl,
    inlines_pg_password,
)
from energydb._frames import Backend, Output, to_backend, to_polars
from energydb._io import (
    ReadResult,
    WriteResult,
    annotate_undefined_table,
    autocommit_read_conn,
    engine_meta_for_manifest,
    execute_read,
    write_manifest,
)
from energydb._join import EdgeSeriesKey, SeriesKey
from energydb._persist import create_edge, create_node_raw, register_tree_under
from energydb.diff import TreeDiff
from energydb.errors import ConfigurationError, NodeNotFoundError, ValidationError
from energydb.models import CREATE_SERIES_META_VIEW, SCHEMA, Base
from energydb.models import SQL_SCHEMA_PREFIX as P
from energydb.paths import (
    OnMissing,
    Path,
    _like_escape,
    build_filter_conditions,
    derived_prefix_like,
    resolve_node_uuid,
)
from energydb.scope import EdgeScope, NodeScope, _coerce_path
from energydb.serialization import reconstruct_edge, reconstruct_node

logger = logging.getLogger(__name__)


class AsyncClient:
    """Async-native client for energy assets, hierarchy, and time series.

    Owns the psycopg ``AsyncConnectionPool`` (used for all PG ops) and
    constructs a :class:`TimeDBClient` for ClickHouse I/O. Every PG
    round-trip is awaited; the ClickHouse leg (sync ``clickhouse-connect``)
    is offloaded to a worker thread. Synchronous callers use
    :class:`energydb.Client`, a thin blocking facade over this class.
    """

    # Class-level defaults so instances built without __init__ behave like root
    # clients. None means no GUC is set on checkouts. Views created via
    # namespace shadow both per-instance and never own the pool's lifecycle.
    _namespace: str | None = None
    _owns_pool: bool = True

    def __init__(
        self,
        *,
        pg_conninfo: str | None = None,
        ch_url: str | None = None,
    ):
        """Construct a client.

        Reads run the PG meta-resolve and the CH value read **in parallel**
        whenever the read is expressible over the ClickHouse engine table
        (provisioned by :meth:`create` for fresh DBs, or explicitly by
        :meth:`setup_ch_meta_engine`); anything else, and any engine failure,
        uses the sequential path, with identical results. Set
        ``ENERGYDB_DISABLE_ENGINE=1`` to force sequential reads for the whole
        session (ops kill-switch; also what benchmarks use for before/after).
        """
        conninfo = pg_conninfo or os.environ.get("TIMEDB_PG_DSN") or os.environ.get("DATABASE_URL")
        if not conninfo:
            raise ConfigurationError("PostgreSQL connection not configured. Pass pg_conninfo or set TIMEDB_PG_DSN.")
        if "://" not in conninfo:
            raise ConfigurationError(
                "pg_conninfo must be a URI (e.g. postgresql://user:pass@host/db); "
                "key=value DSNs are not supported here because the schema-create path "
                "needs a SQLAlchemy URL."
            )
        self._dsn = conninfo

        async def _configure(conn):
            # Caches a server-side prepared statement per SQL text, skipping
            # PG's parse and plan on repeat calls. Client-side attribute only,
            # so no round-trip and no transaction. Nothing else belongs here:
            # energydb's SQL is schema-qualified and needs no session state.
            conn.prepare_threshold = 1

        self._pool = AsyncConnectionPool(
            conninfo=conninfo,
            min_size=1,
            max_size=10,
            open=False,
            configure=_configure,
        )
        self.td = TimeDBClient(ch_url=ch_url)
        # Once set, the session uses the sequential resolve without retrying the
        # engine. setup_ch_meta_engine() resets it.
        self._engine_unavailable = os.environ.get("ENERGYDB_DISABLE_ENGINE") == "1"

    async def open(self) -> None:
        """Open the async connection pool. Await once before first use."""
        self._require_root("open")
        await self._pool.open()

    def _safe_dsn(self) -> str:
        """The DSN with the userinfo segment (user:pass) replaced by ``***``.

        Shows scheme + host(:port) + db. Pure formatting, no I/O. Shared by
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
        ns = f", namespace={self._namespace!r}" if self._namespace is not None else ""
        return f"AsyncClient(pg={self._safe_dsn()!r}{ns})"

    def namespace(self, ns: str) -> AsyncClient:
        """Return a view of this client bound to one namespace.

        The view shares the parent's connection pool and ClickHouse client and
        is a cheap, disposable dict-copy: create one per request. Every PG
        round-trip through the view binds the ``energydb.namespace`` GUC (see
        :meth:`_conn` / :meth:`_read_conn`), which row-level security policies
        use to filter every table to that namespace and the columns' server
        defaults use to stamp writes. Lifecycle and schema operations
        (:meth:`open`, :meth:`close`, :meth:`create`, :meth:`delete`,
        :meth:`setup_ch_meta_engine`) stay with the root client and raise on a
        view.

        Engine-parallel reads are disabled on views: the ClickHouse meta engine
        table reads PG with its own RLS-bypassing credentials, so views always
        take the sequential resolve until its predicate carries the namespace.
        Results are identical and namespace-enforced.
        """
        if not ns:
            raise ValidationError("namespace must be a non-empty string")
        clone = object.__new__(type(self))
        clone.__dict__ = {
            **self.__dict__,
            "_namespace": ns,
            "_owns_pool": False,
            "_engine_unavailable": True,
        }
        return clone

    def _require_root(self, op: str) -> None:
        """Raise unless called on a root client (namespaced views share the
        pool and must never manage its lifecycle or touch schema DDL)."""
        if not self._owns_pool:
            raise ValidationError(f"{op}() is not available on a namespaced view; call it on the root client")

    @asynccontextmanager
    async def _conn(self) -> AsyncIterator[psycopg.AsyncConnection[Any]]:
        """Borrow a pooled connection for transactional work: the single
        checkout point for every mutating PG round-trip (here and in
        ``scope`` / ``_transaction``).

        On a namespaced view, the transaction-local ``energydb.namespace``
        GUC is bound first. ``set_config(..., is_local := true)`` is
        ``SET LOCAL`` in function form, parameterizable and server-side
        preparable, so the value dies with the transaction and nothing
        leaks when the connection returns to the shared pool.
        """
        async with annotate_undefined_table(), self._pool.connection() as conn:
            if self._namespace is not None:
                await conn.execute(
                    "SELECT set_config('energydb.namespace', %s, true)",
                    (self._namespace,),
                )
            yield conn

    @asynccontextmanager
    async def _read_conn(self) -> AsyncIterator[psycopg.AsyncConnection[Any]]:
        """Borrow an autocommit connection for pure reads (see
        :func:`energydb._io.autocommit_read_conn` for why autocommit).

        Autocommit has no transaction for ``SET LOCAL`` to live in, so on a
        namespaced view the GUC is bound at **session** level and cleared
        again before the connection returns to the shared pool. If the clear
        itself fails the connection is broken and the pool discards it, so a
        stale binding cannot leak to another checkout.
        """
        async with autocommit_read_conn(self._pool) as conn:
            if self._namespace is None:
                yield conn
                return
            await conn.execute(
                "SELECT set_config('energydb.namespace', %s, false)",
                (self._namespace,),
            )
            try:
                yield conn
            finally:
                await conn.execute("SELECT set_config('energydb.namespace', '', false)")

    async def create(self) -> None:
        """Create PG schema + CH tables, and provision the CH meta engine table.

        Schema is defined by the SQLAlchemy models in :mod:`energydb.models`
        (the ``series_meta`` view rides on the DDL events), created in a worker
        thread because ``create_all`` and TimeDB's create are synchronous.

        The engine table is best-effort: a CH role that cannot create
        ``PostgreSQL()`` engine tables gets a logged warning and reads fall
        back to the sequential path. Same fallback, quietly, if the PG DSN has
        no TCP host as seen from ClickHouse (a Unix-socket-only DSN, e.g. from
        ``postgresql:///db?host=/run/postgresql``): the fast, engine-backed
        read path needs PostgreSQL reachable over TCP from ClickHouse, so set
        ``ENERGYDB_CH_PG_HOST`` if the DSN's own host is socket-only or not
        resolvable from ClickHouse's network. :meth:`setup_ch_meta_engine` is
        the explicit, raising alternative.

        For production, set ``ENERGYDB_CH_PG_COLLECTION`` to a ClickHouse named
        collection holding the PostgreSQL connection; otherwise the credentials
        are inlined into the engine table's DDL (and a warning says so).

        Raises :class:`~energydb.errors.ConfigurationError` on PostgreSQL
        older than 15: the ``edge_uniq`` multigraph key needs
        ``UNIQUE NULLS NOT DISTINCT``.
        """
        self._require_root("create")
        await self._check_server_version()
        await asyncio.to_thread(self._create_blocking)
        try:
            await asyncio.to_thread(self._provision_engine_table_blocking)
        except Exception:  # noqa: BLE001  (best-effort; engine reads degrade to sequential)
            logger.warning(
                "could not provision the ClickHouse meta engine table; reads will use the sequential path",
                exc_info=True,
            )

    # UNIQUE NULLS NOT DISTINCT on edge_uniq is PostgreSQL 15+ syntax; checking
    # up front turns a cryptic mid-DDL syntax error into a clear one.
    _MIN_SERVER_VERSION_NUM = 150000
    _MIN_SERVER_VERSION = "15"

    async def _check_server_version(self) -> None:
        """Raise :class:`ConfigurationError` if the server predates PostgreSQL 15."""
        async with self._pool.connection() as conn:
            row = await (await conn.execute("SHOW server_version_num")).fetchone()
        version_num = int(row[0]) if row is not None else 0
        if version_num < self._MIN_SERVER_VERSION_NUM:
            raise ConfigurationError(
                f"energydb requires PostgreSQL {self._MIN_SERVER_VERSION}+ "
                f"(server reports server_version_num={version_num}). The edge "
                f"table's unique key uses UNIQUE NULLS NOT DISTINCT, which "
                f"PostgreSQL 14 and older do not support."
            )

    def _provision_engine_table_blocking(self, *, strict: bool = False) -> None:
        # Named collections carry their own PG host, so the DSN's host is
        # irrelevant to them; only the inlined-credential path needs a TCP host.
        if not os.environ.get("ENERGYDB_CH_PG_COLLECTION") and engine_pg_host(self._dsn) is None:
            message = (
                "CH<->PG engine needs a TCP host in the PG DSN; "
                "reads will use the sequential path (set ENERGYDB_CH_PG_HOST to override)"
            )
            if strict:
                raise ConfigurationError(message)
            logger.info(message)
            return
        # Warn from here, not engine_table_ddl(), so DDL construction stays
        # side-effect-free and the warning fires as the credential is written.
        if inlines_pg_password(self._dsn):
            logger.warning(
                "energydb: provisioning the ClickHouse meta-engine table %r with inline "
                "PostgreSQL credentials: the password will be visible via SHOW CREATE TABLE "
                "to any ClickHouse user with read access. For production, create a ClickHouse "
                "named collection holding the PostgreSQL connection and set "
                "ENERGYDB_CH_PG_COLLECTION to its name.",
                CH_ENGINE_TABLE,
            )
        # The engine table is stateless, so recreating it picks up view and
        # column upgrades on existing deployments.
        self.td._ch.command(DROP_ENGINE_TABLE)
        if DROP_LEGACY_ENGINE_TABLE:
            self.td._ch.command(DROP_LEGACY_ENGINE_TABLE)
        self.td._ch.command(engine_table_ddl(self._dsn, SCHEMA or "public"))

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
        own four tables, never the shared ``public`` schema, which would take
        the host application's tables with it.
        """
        self._require_root("delete")
        async with annotate_undefined_table(), self._pool.connection() as conn:
            if SCHEMA is None:
                await conn.execute("DROP TABLE IF EXISTS series, runs, edge, node CASCADE")
            else:
                await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
            await conn.commit()
        await asyncio.to_thread(self.td.delete)
        await asyncio.to_thread(self.td._ch.command, DROP_ENGINE_TABLE)

    async def setup_ch_meta_engine(self) -> None:
        """Provision the ClickHouse ↔ PG metadata bridge for ``concurrent`` reads.

        Idempotent. (Re)creates the PG ``series_meta`` view and the ClickHouse
        ``PostgreSQL()`` engine table over it (see :mod:`energydb._ch_meta_engine`
        for the credential/vantage resolution). Unlike :meth:`create`'s best-effort
        provisioning this raises on failure, and it clears the session's
        engine-unavailable degrade flag; call it to re-enable ``concurrent``
        after fixing engine infrastructure.

        Set ``ENERGYDB_CH_PG_COLLECTION`` to a ClickHouse named collection for
        production deployments; without it the PostgreSQL password is inlined
        into the DDL and readable via ``SHOW CREATE TABLE`` (warned about at
        provisioning time).

        Raises :class:`~energydb.errors.ConfigurationError` if the PG DSN has
        no TCP host as seen from ClickHouse (see :meth:`create`); set
        ``ENERGYDB_CH_PG_HOST`` to fix it.
        """
        self._require_root("setup_ch_meta_engine")
        async with annotate_undefined_table(), self._pool.connection() as conn:
            await conn.execute(CREATE_SERIES_META_VIEW)
            await conn.commit()
        await asyncio.to_thread(self._provision_engine_table_blocking, strict=True)
        self._engine_unavailable = False

    async def close(self) -> None:
        """Close the PostgreSQL connection pool.

        Root-client only: calling it on a
        :meth:`namespace` view raises
        :class:`~energydb.errors.ValidationError`, since the view shares the
        root's pool.
        """
        self._require_root("close")
        await self._pool.close()

    def get_node(self, *names_or_path, uuid: UUID | None = None) -> NodeScope:
        """Return a :class:`NodeScope` for a node or subtree.

        ``client.get_node("P/Site/T01")``: canonical ``/``-joined string
        ``client.get_node("P", "Site", "T01")``: variadic, equivalent
        ``client.get_node(("P", "Site", "T01"))``: tuple/list path
        ``client.get_node(uuid=...)``: absolute by uuid

        ``/`` is reserved as the path separator; names containing ``/``
        are rejected at registration time. Empty segments (leading,
        trailing, or doubled ``/``) raise ``ValueError``.

        Terminate the chain with ``.get()`` to fetch the EDM object,
        ``.read()`` for time-series data, ``.where(...)`` to filter a
        subtree, etc.
        """
        if uuid is not None:
            if names_or_path:
                raise ValidationError("Pass either uuid= or names, not both.")
            return NodeScope(self, node_uuid=uuid)
        if not names_or_path:
            raise ValidationError("Provide a path or uuid=.")
        return NodeScope(self, path=_coerce_path(names_or_path))

    def get_edge(
        self,
        from_path: Path | list[str] | str | None = None,
        to_path: Path | list[str] | str | None = None,
        *,
        type: str | None = None,
        name: str | None = None,
        uuid: UUID | None = None,
    ) -> EdgeScope:
        """Return an :class:`EdgeScope` by uuid or by ``(from_path, to_path, type[, name])``.

        ``from_path`` / ``to_path`` accept the canonical ``/``-joined string
        form (``"P/Site/T01"``) or a tuple/list of segments. Terminate with
        ``.get()`` to fetch the EDM edge eagerly.

        ``name`` picks one of several *parallel* edges sharing the triple
        (the six circuits of a double-circuit corridor, say). Without it a
        triple that matches exactly one edge resolves, and one that matches
        several raises
        :class:`~energydb.errors.AmbiguousEdgeError` listing the candidates
        rather than picking one.
        """
        if uuid is not None:
            if from_path is not None or to_path is not None or type is not None or name is not None:
                raise ValidationError("Pass uuid= alone, or (from_path, to_path, type=[, name=]), not both.")
            return EdgeScope(self, edge_uuid=uuid)
        if from_path is None or to_path is None or type is None:
            raise ValidationError("Provide uuid= or (from_path, to_path, type=).")
        return EdgeScope(
            self,
            from_path=_coerce_path((), kwarg=from_path),
            to_path=_coerce_path((), kwarg=to_path),
            edge_type=type,
            edge_name=name,
        )

    def transaction(self) -> Transaction:
        """Open an atomic batch of scope mutations.

        Returns a :class:`Transaction` context manager. Mutations executed
        through ``txn.get_node(...)`` / ``txn.get_edge(...)`` /
        ``txn.register_tree(...)`` apply immediately to the open
        transaction's connection but are not committed until
        :meth:`Transaction.commit` is called explicitly. Exit without
        commit raises and rolls back.

        Time-series I/O (``scope.write(df, ...)`` / ``scope.read(...)``)
        inside a transaction does **not** participate in atomicity; it
        executes immediately against the pool / ClickHouse.
        """
        from energydb._transaction import Transaction

        return Transaction(self)

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
        committing; the transaction is rolled back so no DB state changes.

        Inline ``TimeSeries.df`` data is rejected: write data separately
        via :meth:`write` against a manifest. ``under`` selects the parent
        under which the tree's root is grafted; ``None`` means create at
        root. Raises if ``under`` points at a non-existent parent.

        Series declarations on the tree **are** registered alongside their
        owners but do not appear in the returned :class:`TreeDiff`. Adding a
        series to a node that already exists in the DB is not supported here,
        since the create-only pre-check rejects the whole payload; use
        :meth:`NodeScope.register_series` / :meth:`EdgeScope.register_series`.

        Returns the ``uuid`` of the tree's root, except when
        ``dry_run=True`` (which returns the :class:`TreeDiff`).
        """
        async with self._conn() as conn:
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

    @staticmethod
    def _within_match(within) -> tuple[str, Any, str | None]:
        """``(addr_sql, param, joined_path|None)`` for a ``within=`` root row ``r``.

        ``joined_path`` is ``None`` for the UUID form. A missing UUID root
        yields an empty result while a missing path raises, so callers need to
        know which form they got.
        """
        if isinstance(within, UUID):
            return "r.uuid = %s", within, None
        joined = "/".join(_coerce_path((), kwarg=within))
        return "r.path = %s", joined, joined

    @staticmethod
    def _subtree_on(alias: str, joined: str | None) -> tuple[str, list[Any]]:
        """ON-clause fragment matching ``alias`` rows in root ``r``'s subtree (incl. ``r``).

        With ``joined`` (path-addressed root) the escaped prefix is a bind
        param, so PG extracts the literal prefix at plan time and Index Scans
        ``ix_node_path_prefix``. The uuid form derives the prefix from the
        root row inside the statement (a catalog-wide scan), kept only where
        the root path is unknown client-side.
        """
        if joined is not None:
            return rf"({alias}.path = r.path OR {alias}.path LIKE %s || '/%%' ESCAPE '\')", [_like_escape(joined)]
        return rf"({alias}.path = r.path OR {alias}.path LIKE {derived_prefix_like('r.path')} ESCAPE '\')", []

    async def query_nodes(
        self,
        *,
        type: str | None = None,
        within: Path | list[str] | str | UUID | None = None,
        **property_filters,
    ) -> list:
        """Return matching nodes as a flat list of EDM objects.

        ``within`` accepts a ``/``-joined string (``"P/Site"``), a path
        tuple/list of segments, or a :class:`UUID`. One round-trip either
        way: the ``within`` subtree is matched by path prefix inside the
        statement (filters ride on the join), not resolved separately.
        """
        where_filters: dict[str, Any] = dict(property_filters)
        if type is not None:
            where_filters["node_type"] = type

        async with self._read_conn() as conn:
            if within is None:
                filter_conds, filter_params = build_filter_conditions(where_filters, type_col="node_type")
                where = " AND ".join(filter_conds) if filter_conds else "TRUE"
                rows = await (
                    await conn.execute(
                        f"SELECT uuid, node_type, name, data FROM {P}node WHERE {where} ORDER BY name",  # ty: ignore[invalid-argument-type]
                        list(filter_params),
                    )
                ).fetchall()
            else:
                filter_conds, filter_params = build_filter_conditions(
                    where_filters, type_col="node_type", table_alias="n"
                )
                extra = ("".join(f" AND {c}" for c in filter_conds)) if filter_conds else ""
                addr, addr_param, joined = self._within_match(within)
                subtree_on, prefix_params = self._subtree_on("n", joined)
                sql = f"""
                    SELECT n.uuid, n.node_type, n.name, n.data
                    FROM {P}node r LEFT JOIN {P}node n
                      ON {subtree_on}{extra}
                    WHERE {addr}
                    ORDER BY n.name
                """
                rows = await (
                    await conn.execute(sql, [*prefix_params, *filter_params, addr_param])  # ty: ignore[invalid-argument-type]
                ).fetchall()
                if not rows and joined is not None:
                    raise NodeNotFoundError(f"Node not found: {joined}", path=joined)
                rows = [r for r in rows if r[0] is not None]  # LEFT-JOIN row when nothing matches

        return [reconstruct_node({"uuid": r[0], "node_type": r[1], "name": r[2], "data": r[3]}) for r in rows]

    async def create_edge(self, edm_obj) -> UUID:
        """Upsert an edge between two existing nodes. Idempotent.

        The edge's :class:`Reference` endpoints (``from_element`` /
        ``to_element``) carry the endpoint UUIDs directly, with no path
        resolution. The endpoints must already exist as nodes; the FK
        constraint will fail otherwise.

        For edges that are part of a tree, prefer :meth:`register_tree`: it
        walks the structure and validates endpoints against the tree's index
        in one pass.
        """
        async with self._conn() as conn:
            edge_uuid = await create_edge(conn, edm_obj, tree_root=None)
            await conn.commit()
        return edge_uuid

    async def create_node(
        self,
        *,
        node_type: str,
        name: str,
        data: dict | None = None,
        parent: UUID | Path | list[str] | str | None = None,
        uuid: UUID | None = None,
    ) -> UUID:
        """Create a single node from a type slug + JSONB ``data``, with no EDM class.

        Generic counterpart to :meth:`register_tree`: ``node_type`` is stored
        as a free-form string and ``data`` verbatim, bypassing EnergyDataModel
        (de)serialization. ``parent`` selects the parent node (UUID or path);
        ``None`` creates a root. ``uuid`` is minted (uuid7) when omitted. Read
        these nodes back with :meth:`get_node_raw` / :meth:`get_subtree_raw` or
        ``NodeScope.children()``, not the EDM readers, which require a
        registered type.
        """
        async with self._conn() as conn:
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
        async with self._read_conn() as conn:
            row = await (
                await conn.execute(
                    "SELECT uuid, node_type, name, data, parent_uuid, path, created_at, updated_at "  # ty: ignore[invalid-argument-type]
                    f"FROM {P}node WHERE uuid = %s",
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

        One round-trip: materialized-path prefix scan with the prefix derived
        from the root row inside the statement. Each dict is ``{uuid,
        node_type, name, data, parent_uuid, path}``. Includes the root
        itself; empty list if the root does not exist.
        """
        async with self._read_conn() as conn:
            sql = rf"""
                SELECT c.uuid, c.node_type, c.name, c.data, c.parent_uuid, c.path, c.created_at, c.updated_at
                FROM {P}node r JOIN {P}node c
                  ON (c.path = r.path OR c.path LIKE {derived_prefix_like("r.path")} ESCAPE '\')
                WHERE r.uuid = %s
                ORDER BY c.path
                """
            rows = await (
                await conn.execute(sql, (root_uuid,))  # ty: ignore[invalid-argument-type]
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

    async def list_nodes_raw(
        self,
        *,
        node_type: str | list[str] | None = None,
        parents: list[UUID] | None = None,
        after: tuple[str, UUID] | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """List raw node rows with SQL-side filtering and keyset pagination.

        Filters compose with AND: ``node_type`` (one string or a list),
        ``parents`` (direct children of any of the given nodes). On a
        namespaced view the rows are additionally constrained to the view's
        namespace explicitly, independent of whether RLS policies are
        installed. ``after`` is a ``(name, uuid)`` keyset cursor matching
        the ``ORDER BY name, uuid::text`` ordering; ``limit`` caps the page.
        Row shape matches :meth:`get_node_raw` / :meth:`get_subtree_raw`.
        """
        conditions: list[str] = []
        params: list[Any] = []
        if self._namespace is not None:
            conditions.append("namespace = %s")
            params.append(self._namespace)
        if node_type is not None:
            conditions.append("node_type = ANY(%s)")
            params.append([node_type] if isinstance(node_type, str) else list(node_type))
        if parents is not None:
            conditions.append("parent_uuid = ANY(%s)")
            params.append(list(parents))
        if after is not None:
            conditions.append("(name, uuid::text) > (%s, %s)")
            params.extend([after[0], str(after[1])])
        where = " AND ".join(conditions) if conditions else "TRUE"
        sql = (
            "SELECT uuid, node_type, name, data, parent_uuid, path, created_at, updated_at "
            f"FROM {P}node WHERE {where} ORDER BY name, uuid::text"
        )
        if limit is not None:
            sql += " LIMIT %s"
            params.append(limit)
        async with self._read_conn() as conn:
            rows = await (await conn.execute(sql, params)).fetchall()  # ty: ignore[invalid-argument-type]
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

        Returns ``{series_id, name, data_type, canonical_unit, timeseries_type,
        description}`` per series. ``owner_col`` is ``"node_uuid"`` (default)
        or ``"edge_uuid"``.

        ``series_id`` is the timedb-internal handle (the same value
        :meth:`NodeScope.register_series` returns) and makes this the reverse
        lookup from ``(owner, data_type, name)``. It is an *input* to lower-level
        timedb APIs, not a secret; read **results** still never carry it.
        """
        if owner_col not in ("node_uuid", "edge_uuid"):
            raise ValidationError("owner_col must be 'node_uuid' or 'edge_uuid'")
        async with self._read_conn() as conn:
            rows = await (
                await conn.execute(
                    f"SELECT series_id, name, data_type, canonical_unit, timeseries_type, description "  # ty: ignore[invalid-argument-type]
                    f"FROM {P}series WHERE {owner_col} = %s ORDER BY data_type, name",
                    (owner_uuid,),
                )
            ).fetchall()
        return [
            {
                "series_id": r[0],
                "name": r[1],
                "data_type": r[2],
                "canonical_unit": r[3],
                "timeseries_type": r[4],
                "description": r[5],
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
        endpoint is in that subtree. One round-trip either way: the subtree
        is matched by path prefix inside the statement (DISTINCT collapses
        edges reached via both endpoints).
        """
        where_filters: dict[str, Any] = dict(property_filters)
        if type is not None:
            where_filters["edge_type"] = type

        async with self._read_conn() as conn:
            if within is None:
                filter_conds, filter_params = build_filter_conditions(where_filters, type_col="edge_type")
                where = " AND ".join(filter_conds) if filter_conds else "TRUE"
                rows = await (
                    await conn.execute(
                        f"SELECT uuid, edge_type, name, data, from_node_uuid, to_node_uuid "  # ty: ignore[invalid-argument-type]
                        f"FROM {P}edge WHERE {where} ORDER BY name NULLS LAST",
                        list(filter_params),
                    )
                ).fetchall()
            else:
                filter_conds, filter_params = build_filter_conditions(
                    where_filters, type_col="edge_type", table_alias="e"
                )
                extra = ("".join(f" AND {c}" for c in filter_conds)) if filter_conds else ""
                addr, addr_param, joined = self._within_match(within)
                subtree_on, prefix_params = self._subtree_on("m", joined)
                sql = f"""
                    SELECT DISTINCT e.uuid, e.edge_type, e.name, e.data, e.from_node_uuid, e.to_node_uuid
                    FROM {P}node r
                    LEFT JOIN {P}node m
                      ON {subtree_on}
                    LEFT JOIN {P}edge e
                      ON (e.from_node_uuid = m.uuid OR e.to_node_uuid = m.uuid){extra}
                    WHERE {addr}
                    ORDER BY e.name NULLS LAST
                    """
                rows = await (
                    await conn.execute(sql, [*prefix_params, *filter_params, addr_param])  # ty: ignore[invalid-argument-type]
                ).fetchall()
                if not rows and joined is not None:
                    raise NodeNotFoundError(f"Node not found: {joined}", path=joined)
                rows = [r for r in rows if r[0] is not None]  # LEFT-JOIN rows when nothing matches
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
        outside the tree shape; query them separately with
        :meth:`get_edge` or :meth:`query_edges`.
        """
        if uuid is not None:
            addr, addr_param, joined = "r.uuid = %s", uuid, None
            missing_err = NodeNotFoundError(f"Node not found: uuid={uuid}", uuid=uuid)
        elif names_or_path:
            joined = "/".join(_coerce_path(names_or_path))
            addr, addr_param = "r.path = %s", joined
            missing_err = NodeNotFoundError(f"Node not found: {joined}", path=joined)
        else:
            raise ValidationError("Provide a path or uuid=.")

        # The root resolve and prefix scan are inlined into one statement; with
        # include_series a second statement rides the same pipeline flush.
        subtree_on, prefix_params = self._subtree_on("n", joined)
        subtree_from = f"FROM {P}node r JOIN {P}node n ON {subtree_on}"
        params = [*prefix_params, addr_param]
        nodes_sql = f"SELECT n.uuid, n.node_type, n.name, n.data, n.parent_uuid, r.uuid {subtree_from} WHERE {addr}"
        series_sql = (
            f"SELECT s.node_uuid, s.data_type, s.name, s.canonical_unit, s.timeseries_type, s.description "
            f"{subtree_from} JOIN {P}series s ON s.node_uuid = n.uuid WHERE {addr}"
        )
        async with self._read_conn() as conn:
            if include_series:
                async with conn.pipeline():
                    nodes_cur = await conn.execute(nodes_sql, params)  # ty: ignore[invalid-argument-type]
                    series_cur = await conn.execute(series_sql, params)  # ty: ignore[invalid-argument-type]
                rows = await nodes_cur.fetchall()
                series_rows = await series_cur.fetchall()
            else:
                rows = await (await conn.execute(nodes_sql, params)).fetchall()  # ty: ignore[invalid-argument-type]
                series_rows = []

        if not rows:
            raise missing_err
        root_uuid = rows[0][5]  # r.uuid rides along on every subtree row

        nodes: dict[UUID, Any] = {}
        parent_map: dict[UUID, UUID | None] = {}
        for r in rows:
            node_uuid = r[0]
            parent_map[node_uuid] = r[4]
            nodes[node_uuid] = reconstruct_node({"uuid": r[0], "node_type": r[1], "name": r[2], "data": r[3]})

        if include_series:
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

        for node_uuid, parent_uuid in parent_map.items():
            if parent_uuid is not None and parent_uuid in nodes:
                nodes[parent_uuid].add_child(nodes[node_uuid])

        return nodes[root_uuid]

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
        unchanged_scope: UnchangedScope = "auto",
    ) -> WriteResult:
        """Bulk-write timeseries data via a routing manifest.

        ``df`` is a pandas or polars DataFrame carrying one routing column
        (``node_uuid``, ``edge_uuid``, or ``path`` as ``Utf8`` joined with
        ``/``, e.g. ``"my-portfolio/Offshore-1/T01"``), plus ``data_type``,
        ``name``, and the timedb data columns (``valid_time``, ``value``,
        optional ``knowledge_time``). Optional ``unit`` column triggers
        per-row unit conversion to each series's canonical unit.

        ``skip_unchanged`` drops rows whose latest stored value is unchanged
        before the insert. ``unchanged_scope`` picks the comparison key:

        * ``"auto"`` (default): per series, by its registered type. FLAT
          compares per ``valid_time``, OVERLAPPING per
          ``(valid_time, knowledge_time)``, so one call handles a mixed
          manifest. Identical to ``"valid_time"`` for a FLAT-only manifest.
        * ``"knowledge_time"``: that key uniformly.
        * ``"valid_time"``: that key uniformly. Raises
          :class:`~energydb.errors.UnchangedScopeError` if the manifest
          contains OVERLAPPING series, since it would drop their
          republications.

        Series must already be registered (typically via
        :meth:`register_tree`). Returns a :class:`WriteResult`, an ``int``
        run_id carrying ``written`` / ``skipped`` counts.
        """
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            df_pl = to_polars(df)
        return await write_manifest(
            self,
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
        on_missing: OnMissing = "raise",
    ) -> (
        pl.DataFrame
        | pd.DataFrame
        | dict[SeriesKey, pl.DataFrame]
        | dict[SeriesKey, pd.DataFrame]
        | dict[EdgeSeriesKey, pl.DataFrame]
        | dict[EdgeSeriesKey, pd.DataFrame]
        | ReadResult
    ):
        """Bulk read via manifest. Detects edge vs node routing automatically.

        Routing is chosen from the columns present (exactly one route):

        * ``path``: node series by materialized path (``Utf8`` joined with ``/``).
        * ``node_uuid`` / ``edge_uuid``: series by owner uuid.
        * ``from_path`` + ``to_path`` + ``edge_type``: edge series by their
          endpoint paths and type (all three required together), resolved
          server-side the same way node ``path`` is. Matches the edge output
          columns, so an edge read's output can be fed back as a manifest
          without a UUID-resolution round-trip.
        * ``edge_name``: optional fourth column on that route, picking one of
          several *parallel* edges sharing a triple (null = the unnamed edge).
          A triple matching more than one edge without it raises
          :class:`~energydb.errors.AmbiguousEdgeError`.

        Accepts pandas or polars on input. Output shape:

        * ``output="frame"`` (default): a single DataFrame with columns
          ``(path, data_type, name, valid_time, value, …)`` for node-routed
          reads, or ``(from_path, to_path, edge_type, edge_name, data_type,
          name, valid_time, value, …)`` for edge-routed reads. ``path`` /
          ``from_path`` / ``to_path`` are ``Utf8`` joined with ``/``;
          ``edge_name`` is the edge's own name, always present and null for
          unnamed edges. Optional columns appear when
          ``include_knowledge_time`` / ``include_updates`` are set.
        * ``output="by_path"``: a ``dict`` keyed by :class:`SeriesKey`
          (node-routed: ``path``, ``data_type``, ``name``) or
          :class:`EdgeSeriesKey` (edge-routed: ``from_path``, ``to_path``,
          ``edge_type``, ``edge_name``, ``data_type``, ``name``), valued by
          per-series DataFrames carrying only the data columns
          (``valid_time``, ``value``, plus opt-in time/audit columns). Keys are
          NamedTuples, so positional (``result[(path, dt, name)]``) and
          attribute (``key.path``) access both work. Sub-frames are sorted by
          ``valid_time`` ascending, then ``knowledge_time`` / ``change_time``
          when requested.

        ``backend="polars"`` (default) returns polars frames; ``"pandas"``
        converts at the boundary. Internal identifiers (``series_id``,
        ``node_uuid``, ``edge_uuid``) are never exposed on the result.

        **``on_missing`` changes the return type.** With the default
        ``"raise"``, an unregistered ``(owner, data_type, name)`` triple fails
        the whole call with :class:`~energydb.errors.SeriesNotFoundError`,
        naming every unresolved triple, and the return value is as described
        above. With ``"skip"``, those triples are dropped and the call returns a
        :class:`ReadResult` of ``(data, missing)``, reporting them there. Only
        unregistered series are affected: a structurally invalid manifest
        (missing or ambiguous routing column, wrong dtype, null routing value)
        raises either way.
        """
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            manifest = to_polars(df)
        result, _n_series, missing = await execute_read(
            self._pool,
            self.td,
            self,
            manifest=manifest,
            engine_meta=lambda: engine_meta_for_manifest(manifest),
            unit=unit,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            include_updates=include_updates,
            include_knowledge_time=include_knowledge_time,
            on_missing=on_missing,
            output=output,
        )
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            return self._with_missing(result, missing, backend=backend, on_missing=on_missing)

    async def read_relative(
        self,
        df: pl.DataFrame | pd.DataFrame,
        *,
        unit: str | None = None,
        output: Output = "frame",
        backend: Backend = "polars",
        on_missing: OnMissing = "raise",
        **td_kwargs,
    ) -> (
        pl.DataFrame
        | pd.DataFrame
        | dict[SeriesKey, pl.DataFrame]
        | dict[SeriesKey, pd.DataFrame]
        | dict[EdgeSeriesKey, pl.DataFrame]
        | dict[EdgeSeriesKey, pd.DataFrame]
        | ReadResult
    ):
        """Bulk relative read via manifest.

        See :meth:`read` for the ``output`` / ``backend`` contract, and for
        ``on_missing``, which switches the return type to
        :class:`ReadResult` when set to ``"skip"``, exactly as it does there.
        ``**td_kwargs`` are forwarded to :meth:`timedb.TimeDBClient.read_relative`;
        see that signature for accepted arguments (window selectors, etc.).
        """
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            manifest = to_polars(df)
        result, _n_series, missing = await execute_read(
            self._pool,
            self.td,
            self,
            manifest=manifest,
            engine_meta=lambda: engine_meta_for_manifest(manifest),
            relative=True,
            unit=unit,
            output=output,
            on_missing=on_missing,
            td_kwargs=td_kwargs,
        )
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            return self._with_missing(result, missing, backend=backend, on_missing=on_missing)

    @staticmethod
    def _with_missing(
        result: pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame],
        missing: pl.DataFrame,
        *,
        backend: Backend,
        on_missing: OnMissing,
    ) -> (
        pl.DataFrame
        | pd.DataFrame
        | dict[SeriesKey, pl.DataFrame]
        | dict[SeriesKey, pd.DataFrame]
        | dict[EdgeSeriesKey, pl.DataFrame]
        | dict[EdgeSeriesKey, pd.DataFrame]
        | ReadResult
    ):
        """Convert a read to ``backend``, wrapping it iff ``on_missing="skip"``.

        The return-type switch lives in one place so :meth:`read` and
        :meth:`read_relative` can't drift apart on it.
        """
        data = to_backend(result, backend)
        if on_missing == "skip":
            # to_backend is typed for the whole read-result union, but missing is
            # always a plain frame; narrow before it lands on ReadResult.missing.
            return ReadResult(data, cast("pl.DataFrame | pd.DataFrame", to_backend(missing, backend)))
        return data

    async def read_runs_for_series(self, *, series_id: int) -> list[dict[str, Any]]:
        """Return runs that wrote data for a given series_id, latest first."""
        run_ids = await asyncio.to_thread(self.td.read_run_series, series_id=series_id)
        if not run_ids:
            return []
        async with self._read_conn() as conn:
            return await runs_mod.get_runs(conn, run_ids)

    def _sqlalchemy_url(self) -> str:
        return f"postgresql+psycopg://{self._dsn.split('://', 1)[-1]}"
