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

import os
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    from energydb._transaction import Transaction

import pandas as pd
import polars as pl
from psycopg_pool import ConnectionPool
from sqlalchemy import create_engine
from timedatamodel import DataType, TimeSeries, TimeSeriesType
from timedb import TimeDBClient, profiling

from energydb import runs as runs_mod
from energydb._frames import Backend, Output, to_backend, to_polars
from energydb._io import read_manifest, read_relative_manifest, write_manifest
from energydb._join import EdgeSeriesKey, SeriesKey
from energydb._persist import create_edge, register_tree_under
from energydb._resolve_cache import SeriesRegistry
from energydb.diff import TreeDiff
from energydb.models import Base
from energydb.paths import (
    Path,
    build_filter_conditions,
    resolve_node_uuid,
    resolve_subtree_uuids,
)
from energydb.scope import EdgeScope, NodeScope, _coerce_path
from energydb.serialization import reconstruct_edge, reconstruct_node

_SEARCH_PATH = "SET search_path TO energydb, public"


class Client:
    """Client for energy assets, hierarchy, and time series.

    Owns the psycopg connection pool (used for all PG ops) and constructs
    a :class:`TimeDBClient` for ClickHouse I/O.
    """

    def __init__(
        self,
        *,
        pg_conninfo: str | None = None,
        ch_url: str | None = None,
    ):
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

        def _configure(conn):
            conn.execute(_SEARCH_PATH)
            conn.commit()

        self._pool = ConnectionPool(
            conninfo=conninfo,
            min_size=1,
            max_size=10,
            open=True,
            configure=_configure,
        )
        self.td = TimeDBClient(ch_url=ch_url)
        self._series_registry = SeriesRegistry()

    def __repr__(self) -> str:
        """Repr with credentials stripped from the DSN.

        Shows scheme + host(:port) + db; the userinfo segment (user:pass) is
        replaced with ``***``. Pure formatting — no I/O.
        """
        dsn = self._dsn
        if "://" in dsn:
            scheme, rest = dsn.split("://", 1)
            if "@" in rest:
                _userinfo, hostpart = rest.split("@", 1)
                safe = f"{scheme}://***@{hostpart}"
            else:
                safe = dsn
        else:
            safe = dsn
        return f"Client(pg={safe!r})"

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def create(self) -> None:
        """Create PG schema + CH tables.

        Schema is defined entirely by SQLAlchemy models in :mod:`energydb.models`
        — including the ``energydb`` schema, the partial unique index on root
        names, and the immutability trigger on ``series``. No raw SQL.
        """
        engine = create_engine(self._sqlalchemy_url())
        try:
            Base.metadata.create_all(engine, checkfirst=True)
        finally:
            engine.dispose()

        self.td.create()

    def delete(self) -> None:
        """Drop PG schema (CASCADE) and CH tables."""
        with self._pool.connection() as conn:
            conn.execute("DROP SCHEMA IF EXISTS energydb CASCADE")
            conn.commit()
        self.td.delete()

    def close(self) -> None:
        self._pool.close()

    # ------------------------------------------------------------------
    # Resolve cache
    # ------------------------------------------------------------------

    def invalidate_series_cache(self, owner_uuid: UUID | None = None) -> None:
        """Drop cached series metadata.

        With no argument, clears every entry. With an ``owner_uuid``, evicts
        only entries owned by that node or edge.

        The cache is normally maintained automatically — in-process
        registrations and node/edge deletions update it transparently.
        Call this only to recover from a cross-process modification that
        invalidated state held in this client (another process registered,
        deleted, or flipped ``timeseries_type`` on a series this client had
        cached).
        """
        if owner_uuid is None:
            self._series_registry.clear()
        else:
            self._series_registry.evict_owner(str(owner_uuid))

    def resolve_cache_stats(self) -> dict[str, int]:
        """Return cumulative resolve cache hits/misses and current size."""
        return self._series_registry.stats()

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

    def register_tree(
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
        with self._pool.connection() as conn:
            parent_uuid = resolve_node_uuid(conn, _coerce_path((), kwarg=under)) if under is not None else None
            root_uuid, diff = register_tree_under(
                conn,
                edm_obj,
                parent_uuid=parent_uuid,
                dry_run=dry_run,
                registry=None if dry_run else self._series_registry,
            )
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        if dry_run:
            return diff
        return root_uuid

    def query_nodes(
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

        with self._pool.connection() as conn:
            filter_conds, filter_params = build_filter_conditions(where_filters, type_col="node_type")
            conditions: list[str] = list(filter_conds)
            params: list[Any] = list(filter_params)

            if within is not None:
                within_uuid = (
                    within if isinstance(within, UUID) else resolve_node_uuid(conn, _coerce_path((), kwarg=within))
                )
                conditions.append("uuid = ANY(%s)")
                params.append(resolve_subtree_uuids(conn, within_uuid))

            where = " AND ".join(conditions) if conditions else "TRUE"
            rows = conn.execute(
                f"SELECT uuid, node_type, name, data FROM energydb.node WHERE {where} ORDER BY name",
                params,
            ).fetchall()

        return [reconstruct_node({"uuid": r[0], "node_type": r[1], "name": r[2], "data": r[3]}) for r in rows]

    def create_edge(self, edm_obj) -> UUID:
        """Upsert an edge between two existing nodes. Idempotent.

        The edge's :class:`Reference` endpoints (``from_element`` /
        ``to_element``) carry the endpoint UUIDs directly — no path
        resolution. The endpoints must already exist as nodes; the FK
        constraint will fail otherwise.

        For edges that are part of a tree, prefer :meth:`register_tree` —
        it walks the structure and validates endpoints against the tree's
        index in one pass.
        """
        with self._pool.connection() as conn:
            edge_uuid = create_edge(conn, edm_obj, tree_root=None, registry=self._series_registry)
            conn.commit()
        return edge_uuid

    def query_edges(
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

        with self._pool.connection() as conn:
            filter_conds, filter_params = build_filter_conditions(where_filters, type_col="edge_type")
            conditions: list[str] = list(filter_conds)
            params: list[Any] = list(filter_params)

            if within is not None:
                within_uuid = (
                    within if isinstance(within, UUID) else resolve_node_uuid(conn, _coerce_path((), kwarg=within))
                )
                subtree = resolve_subtree_uuids(conn, within_uuid)
                conditions.append("(from_node_uuid = ANY(%s) OR to_node_uuid = ANY(%s))")
                params.append(subtree)
                params.append(subtree)

            where = " AND ".join(conditions) if conditions else "TRUE"
            rows = conn.execute(
                f"SELECT uuid, edge_type, name, data, from_node_uuid, to_node_uuid "
                f"FROM energydb.edge WHERE {where} ORDER BY name NULLS LAST",
                params,
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

    def get_tree(
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
        with self._pool.connection() as conn:
            if uuid is not None:
                root_uuid = uuid
            elif names_or_path:
                root_uuid = resolve_node_uuid(conn, _coerce_path(names_or_path))
            else:
                raise ValueError("Provide a path or uuid=.")

            rows = conn.execute(
                """
                WITH RECURSIVE subtree AS (
                    SELECT uuid, node_type, name, data, parent_uuid
                    FROM energydb.node WHERE uuid = %s
                    UNION ALL
                    SELECT n.uuid, n.node_type, n.name, n.data, n.parent_uuid
                    FROM energydb.node n JOIN subtree s ON n.parent_uuid = s.uuid
                ) CYCLE uuid SET _is_cycle USING _cycle_path
                SELECT uuid, node_type, name, data, parent_uuid FROM subtree
                WHERE NOT _is_cycle
                """,
                (root_uuid,),
            ).fetchall()

            nodes: dict[UUID, Any] = {}
            parent_map: dict[UUID, UUID | None] = {}
            for r in rows:
                node_uuid = r[0]
                parent_map[node_uuid] = r[4]
                nodes[node_uuid] = reconstruct_node({"uuid": r[0], "node_type": r[1], "name": r[2], "data": r[3]})

            if include_series:
                series_rows = conn.execute(
                    "SELECT node_uuid, data_type, name, canonical_unit, timeseries_type, description "
                    "FROM energydb.series WHERE node_uuid = ANY(%s)",
                    (list(nodes.keys()),),
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

    def write(
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
    ) -> int:
        """Bulk-write timeseries data via a routing manifest.

        ``df`` is a pandas or polars DataFrame carrying one routing column
        (``node_uuid``, ``edge_uuid``, or ``path`` as ``Utf8`` joined with
        ``/``, e.g. ``"my-portfolio/Offshore-1/T01"``), plus ``data_type``,
        ``name``, and the timedb data columns (``valid_time``, ``value``,
        optional ``knowledge_time``). Optional ``unit`` column triggers
        per-row unit conversion to each series's canonical unit.

        Series must already be registered (typically via
        :meth:`register_tree`). Returns the ``run_id`` used.
        """
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            df_pl = to_polars(df)
        return write_manifest(
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
            registry=self._series_registry,
        )

    def read(
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
        result = read_manifest(
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
            registry=self._series_registry,
        )
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            return to_backend(result, backend)

    def read_relative(
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
        result = read_relative_manifest(
            self._pool,
            self.td,
            manifest,
            unit=unit,
            output=output,
            registry=self._series_registry,
            **td_kwargs,
        )
        with profiling._phase(profiling.PHASE_EDB_OUTPUT_CONVERT):
            return to_backend(result, backend)

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    def read_runs_for_series(self, *, series_id: int) -> list[dict[str, Any]]:
        """Return runs that wrote data for a given series_id, latest first."""
        run_ids = self.td.read_run_series(series_id=series_id)
        if not run_ids:
            return []
        with self._pool.connection() as conn:
            return runs_mod.get_runs(conn, run_ids)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _sqlalchemy_url(self) -> str:
        return f"postgresql+psycopg://{self._dsn.split('://', 1)[-1]}"
