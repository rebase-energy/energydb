"""NodeScope and EdgeScope — fluent APIs for navigating and operating on
nodes and edges.

All ``.node()`` calls are lazy — they accumulate a path without hitting the DB.
Resolution happens in a single query when a terminal operation is called.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

import polars as pl
from psycopg.types.json import Jsonb
from timedatamodel import TimeSeries, TimeSeriesDescriptor, TimeSeriesType
from timedb import profiling

from energydb import runs as runs_mod
from energydb import series as series_mod
from energydb._resolve import (
    join_edge_hierarchy,
    join_hierarchy,
    resolve_edge_id_by_name,
    resolve_node_id,
    resolve_path,
    resolve_subtree_ids,
)
from energydb.serialization import reconstruct_edge, reconstruct_node, serialize_node
from energydb.units import apply_unit_factor

# Default retention tier when the caller does not specify one. 'medium' is the
# least surprising default (~3 years of history).
_DEFAULT_RETENTION = "medium"


def _timeseries_type_from_descriptor(desc: TimeSeriesDescriptor) -> str | None:
    """Extract timeseries_type from a descriptor as 'FLAT' or 'OVERLAPPING'."""
    ts_type = desc.timeseries_type
    if ts_type is None:
        return None
    return ts_type.value if isinstance(ts_type, TimeSeriesType) else str(ts_type)


# ---------------------------------------------------------------------------
# NodeScope
# ---------------------------------------------------------------------------


class NodeScope:
    """Accumulated scope for navigating and operating on nodes.

    Lazy: ``.node()`` and ``.find()`` build up filters without hitting the DB.
    Terminal operations (``.read()``, ``.write()``, ``.children()``, etc.)
    trigger resolution and execute.
    """

    def __init__(
        self,
        pool,
        td,
        *,
        node_id: int | None = None,
        name_chain: list[str] | None = None,
        find_filters: dict[str, Any] | None = None,
    ):
        self._pool = pool
        self._td = td
        self._node_id = node_id
        self._name_chain = name_chain or []
        self._find_filters = find_filters

    # ------------------------------------------------------------------
    # Navigation (lazy)
    # ------------------------------------------------------------------

    def node(self, name: str | None = None, *, id: int | None = None) -> NodeScope:
        if id is not None:
            return NodeScope(self._pool, self._td, node_id=id)
        if name is None:
            raise ValueError("Must provide name or id")
        return NodeScope(
            self._pool,
            self._td,
            node_id=self._node_id,
            name_chain=self._name_chain + [name],
        )

    def find(
        self,
        *,
        type: str | None = None,
        name: str | None = None,
        **property_filters,
    ) -> NodeScope:
        filters: dict[str, Any] = {}
        if type is not None:
            filters["node_type"] = type
        if name is not None:
            filters["name"] = name
        filters.update(property_filters)
        return NodeScope(
            self._pool,
            self._td,
            node_id=self._node_id,
            name_chain=self._name_chain,
            find_filters=filters,
        )

    # ------------------------------------------------------------------
    # Internal: resolve scope → node_id(s)
    # ------------------------------------------------------------------

    def _resolve_node_id(self, conn) -> int:
        if self._node_id is not None and not self._name_chain:
            return self._node_id
        if self._name_chain:
            return resolve_node_id(conn, self._name_chain, start_id=self._node_id)
        raise ValueError("NodeScope has no node_id or name chain to resolve")

    def _resolve_target_node_ids(self, conn) -> list[int]:
        root_id = self._resolve_node_id(conn)
        subtree_ids = resolve_subtree_ids(conn, root_id)
        if not self._find_filters:
            return subtree_ids

        conditions = ["node_id = ANY(%s)"]
        params: list[Any] = [subtree_ids]
        if "node_type" in self._find_filters:
            conditions.append("node_type = %s")
            params.append(self._find_filters["node_type"])
        if "name" in self._find_filters:
            conditions.append("name = %s")
            params.append(self._find_filters["name"])
        for key, value in self._find_filters.items():
            if key in ("node_type", "name"):
                continue
            conditions.append("data->>%s = %s")
            params.append(key)
            params.append(str(value))

        where = " AND ".join(conditions)
        rows = conn.execute(f"SELECT node_id FROM energydb.node WHERE {where}", params).fetchall()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # Get / hierarchy queries
    # ------------------------------------------------------------------

    def get(self):
        with self._pool.connection() as conn:
            node_id = self._resolve_node_id(conn)
            row = conn.execute(
                "SELECT node_id, node_type, name, data FROM energydb.node WHERE node_id = %s",
                (node_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Node not found: id={node_id}")
            return reconstruct_node({"node_id": row[0], "node_type": row[1], "name": row[2], "data": row[3]})

    def children(self, *, type: str | None = None) -> list[dict]:
        with self._pool.connection() as conn:
            node_id = self._resolve_node_id(conn)
            if type:
                rows = conn.execute(
                    "SELECT node_id, node_type, name, data "
                    "FROM energydb.node WHERE parent_id = %s AND node_type = %s "
                    "ORDER BY name",
                    (node_id, type),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT node_id, node_type, name, data FROM energydb.node WHERE parent_id = %s ORDER BY name",
                    (node_id,),
                ).fetchall()
            return [{"node_id": r[0], "node_type": r[1], "name": r[2], "data": r[3]} for r in rows]

    def descendants(self, *, type: str | None = None) -> list[dict]:
        with self._pool.connection() as conn:
            node_id = self._resolve_node_id(conn)
            rows = conn.execute(
                """
                WITH RECURSIVE subtree AS (
                    SELECT node_id FROM energydb.node WHERE node_id = %s
                    UNION ALL
                    SELECT n.node_id FROM energydb.node n
                    JOIN subtree s ON n.parent_id = s.node_id
                )
                SELECT n.node_id, n.node_type, n.name, n.data
                FROM energydb.node n
                JOIN subtree s ON n.node_id = s.node_id
                WHERE n.node_id != %s
                ORDER BY n.name
                """,
                (node_id, node_id),
            ).fetchall()
            if type:
                rows = [r for r in rows if r[1] == type]
            return [{"node_id": r[0], "node_type": r[1], "name": r[2], "data": r[3]} for r in rows]

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def create_child(self, edm_obj) -> int:
        row_data = serialize_node(edm_obj)
        with self._pool.connection() as conn:
            # Empty scope (no id, no name chain) = create a root node.
            parent_id = None if self._node_id is None and not self._name_chain else self._resolve_node_id(conn)
            row = conn.execute(
                "INSERT INTO energydb.node "
                "(node_type, name, parent_id, data) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT ON CONSTRAINT node_child_uniq "
                "DO UPDATE SET data = EXCLUDED.data, updated_at = now() "
                "RETURNING node_id",
                (row_data["node_type"], row_data["name"], parent_id, row_data["data"]),
            ).fetchone()
            conn.commit()
            node_id = row[0]

        NodeScope(self._pool, self._td, node_id=node_id)._register_descriptors(edm_obj)
        return node_id

    def rename(self, new_name: str) -> None:
        with self._pool.connection() as conn:
            node_id = self._resolve_node_id(conn)
            conn.execute(
                "UPDATE energydb.node SET name = %s, updated_at = now() WHERE node_id = %s",
                (new_name, node_id),
            )
            conn.commit()

    def update(self, *, data: dict | None = None, name: str | None = None) -> None:
        sets = ["updated_at = now()"]
        params: list[Any] = []
        if data is not None:
            sets.append("data = data || %s")
            params.append(Jsonb(data))
        if name is not None:
            sets.append("name = %s")
            params.append(name)
        with self._pool.connection() as conn:
            node_id = self._resolve_node_id(conn)
            params.append(node_id)
            conn.execute(
                f"UPDATE energydb.node SET {', '.join(sets)} WHERE node_id = %s",
                params,
            )
            conn.commit()

    def delete(self) -> None:
        with self._pool.connection() as conn:
            node_id = self._resolve_node_id(conn)
            conn.execute("DELETE FROM energydb.node WHERE node_id = %s", (node_id,))
            conn.commit()

    # ------------------------------------------------------------------
    # Series registration
    # ------------------------------------------------------------------

    def _register_descriptors(self, edm_obj) -> list[tuple[int, pl.DataFrame]]:
        """Auto-register series from ``edm_obj.timeseries``.

        Returns ``(series_id, df)`` pairs for TimeSeries entries with data.
        """
        pending: list[tuple[int, pl.DataFrame]] = []
        ts_list = getattr(edm_obj, "timeseries", None)
        if not ts_list:
            return pending
        for ts in ts_list:
            if isinstance(ts, TimeSeries):
                series_id = self.register_series(ts.to_descriptor())
                if ts.df.height > 0:
                    pending.append((series_id, ts.df))
            elif isinstance(ts, TimeSeriesDescriptor):
                self.register_series(ts)
        return pending

    def register_series(
        self,
        descriptor_or_name: TimeSeriesDescriptor | str | None = None,
        *,
        name: str | None = None,
        canonical_unit: str | None = None,
        data_type: str | None = None,
        timeseries_type: str | None = None,
        retention: str = _DEFAULT_RETENTION,
        description: str | None = None,
    ) -> int:
        """Register a time series on this node.

        Accepts a ``TimeSeriesDescriptor`` (unit + data_type + timeseries_type
        extracted) or explicit kwargs. ``retention`` defaults to ``'medium'``
        if not specified.
        """
        if isinstance(descriptor_or_name, TimeSeriesDescriptor):
            desc = descriptor_or_name
            name = name or desc.name
            canonical_unit = canonical_unit or desc.unit
            if data_type is None and desc.data_type is not None:
                data_type = str(desc.data_type).lower()
            if timeseries_type is None:
                timeseries_type = _timeseries_type_from_descriptor(desc)
            description = description or desc.description
        elif isinstance(descriptor_or_name, str):
            name = descriptor_or_name

        if name is None:
            raise ValueError("name is required")
        if data_type is None:
            raise ValueError("data_type is required")
        if canonical_unit is None:
            raise ValueError("canonical_unit is required")
        if timeseries_type is None:
            raise ValueError("timeseries_type is required (FLAT | OVERLAPPING)")

        data_type_str = str(data_type).lower()

        with self._pool.connection() as conn:
            node_id = self._resolve_node_id(conn)
            sid = series_mod.register_series(
                conn,
                node_id=node_id,
                edge_id=None,
                data_type=data_type_str,
                name=name,
                canonical_unit=canonical_unit,
                timeseries_type=timeseries_type,
                retention=retention,
                description=description,
            )
            conn.commit()
        return sid

    # ------------------------------------------------------------------
    # Time series I/O
    # ------------------------------------------------------------------

    def write(
        self,
        df: pl.DataFrame,
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
        """Write time series data for a single series on this node.

        Returns the ``run_id`` used for this write (client-generated if not
        supplied). The caller can use it to correlate downstream operations
        with the ``energydb.runs`` PG row.
        """
        _prof = profiling._enabled
        data_type_str = str(data_type).lower()

        _t = time.perf_counter() if _prof else 0.0
        with self._pool.connection() as conn:
            node_id = self._resolve_node_id(conn)
            meta = series_mod.resolve_for_write(
                conn,
                node_id=node_id,
                data_type=data_type_str,
                name=name,
            )
        if _prof:
            profiling._record(profiling.PHASE_EDB_RESOLVE, time.perf_counter() - _t)

        return _write_one_series(
            self._td,
            self._pool,
            df=df,
            meta=meta,
            unit=unit,
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
    ) -> pl.DataFrame:
        """Read time series data for this scope (node + descendants)."""
        _prof = profiling._enabled

        _t = time.perf_counter() if _prof else 0.0
        with self._pool.connection() as conn:
            target_ids = self._resolve_target_node_ids(conn)
            if not target_ids:
                return pl.DataFrame()

            data_type_str = str(data_type).lower() if data_type else None
            meta = series_mod.resolve_for_read(
                conn,
                node_ids=target_ids,
                data_type=data_type_str,
                name=name,
            )
            if _prof:
                profiling._record(profiling.PHASE_EDB_RESOLVE, time.perf_counter() - _t)
            if meta.is_empty():
                return pl.DataFrame()

            result = _read_and_convert(
                self._td,
                meta,
                unit,
                start_valid=start_valid,
                end_valid=end_valid,
                start_known=start_known,
                end_known=end_known,
                include_updates=include_updates,
                include_knowledge_time=include_knowledge_time,
            )

            _t = time.perf_counter() if _prof else 0.0
            out = join_hierarchy(conn, result, meta)
            if _prof:
                profiling._record(profiling.PHASE_EDB_HIERARCHY_JOIN, time.perf_counter() - _t)
            return out

    def read_relative(
        self,
        *,
        data_type: str,
        name: str,
        unit: str | None = None,
        **td_read_kwargs,
    ) -> pl.DataFrame:
        with self._pool.connection() as conn:
            target_ids = self._resolve_target_node_ids(conn)
            if not target_ids:
                return pl.DataFrame()

            data_type_str = str(data_type).lower()
            meta = series_mod.resolve_for_read(
                conn,
                node_ids=target_ids,
                data_type=data_type_str,
                name=name,
            )
            if meta.is_empty():
                return pl.DataFrame()

            result = _read_relative_and_convert(self._td, meta, unit, td_read_kwargs)
            return join_hierarchy(conn, result, meta)


# ---------------------------------------------------------------------------
# EdgeScope
# ---------------------------------------------------------------------------


class EdgeScope:
    """Scope for operating on a single edge. Flat — no hierarchy lookup."""

    def __init__(
        self,
        pool,
        td,
        *,
        edge_id: int | None = None,
        name: str | None = None,
    ):
        self._pool = pool
        self._td = td
        self._edge_id = edge_id
        self._name = name

    def _resolve_edge_id(self, conn) -> int:
        if self._edge_id is not None:
            return self._edge_id
        if self._name is not None:
            return resolve_edge_id_by_name(conn, self._name)
        raise ValueError("EdgeScope has no edge_id or name to resolve")

    # get / navigation -------------------------------------------------

    def get(self):
        with self._pool.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            row = conn.execute(
                "SELECT edge_id, edge_type, name, data, from_node_id, to_node_id FROM energydb.edge WHERE edge_id = %s",
                (edge_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Edge not found: id={edge_id}")
            from_path = resolve_path(conn, row[4])
            to_path = resolve_path(conn, row[5])
            return reconstruct_edge(
                {
                    "edge_id": row[0],
                    "edge_type": row[1],
                    "name": row[2],
                    "data": row[3],
                    "from_node_path": from_path,
                    "to_node_path": to_path,
                }
            )

    def from_node(self) -> NodeScope:
        with self._pool.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            row = conn.execute(
                "SELECT from_node_id FROM energydb.edge WHERE edge_id = %s",
                (edge_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Edge not found: id={edge_id}")
            return NodeScope(self._pool, self._td, node_id=row[0])

    def to_node(self) -> NodeScope:
        with self._pool.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            row = conn.execute(
                "SELECT to_node_id FROM energydb.edge WHERE edge_id = %s",
                (edge_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Edge not found: id={edge_id}")
            return NodeScope(self._pool, self._td, node_id=row[0])

    # CRUD -------------------------------------------------------------

    def update(self, *, data: dict | None = None, name: str | None = None) -> None:
        sets = ["updated_at = now()"]
        params: list[Any] = []
        if data is not None:
            sets.append("data = data || %s")
            params.append(Jsonb(data))
        if name is not None:
            sets.append("name = %s")
            params.append(name)
        with self._pool.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            params.append(edge_id)
            conn.execute(
                f"UPDATE energydb.edge SET {', '.join(sets)} WHERE edge_id = %s",
                params,
            )
            conn.commit()

    def delete(self) -> None:
        with self._pool.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            conn.execute("DELETE FROM energydb.edge WHERE edge_id = %s", (edge_id,))
            conn.commit()

    # series -----------------------------------------------------------

    def register_series(
        self,
        descriptor_or_name: TimeSeriesDescriptor | str | None = None,
        *,
        name: str | None = None,
        canonical_unit: str | None = None,
        data_type: str | None = None,
        timeseries_type: str | None = None,
        retention: str = _DEFAULT_RETENTION,
        description: str | None = None,
    ) -> int:
        if isinstance(descriptor_or_name, TimeSeriesDescriptor):
            desc = descriptor_or_name
            name = name or desc.name
            canonical_unit = canonical_unit or desc.unit
            if data_type is None and desc.data_type is not None:
                data_type = str(desc.data_type).lower()
            if timeseries_type is None:
                timeseries_type = _timeseries_type_from_descriptor(desc)
            description = description or desc.description
        elif isinstance(descriptor_or_name, str):
            name = descriptor_or_name

        if name is None:
            raise ValueError("name is required")
        if data_type is None:
            raise ValueError("data_type is required")
        if canonical_unit is None:
            raise ValueError("canonical_unit is required")
        if timeseries_type is None:
            raise ValueError("timeseries_type is required (FLAT | OVERLAPPING)")

        data_type_str = str(data_type).lower()

        with self._pool.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            sid = series_mod.register_series(
                conn,
                node_id=None,
                edge_id=edge_id,
                data_type=data_type_str,
                name=name,
                canonical_unit=canonical_unit,
                timeseries_type=timeseries_type,
                retention=retention,
                description=description,
            )
            conn.commit()
        return sid

    def write(
        self,
        df: pl.DataFrame,
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
        data_type_str = str(data_type).lower()
        with self._pool.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            meta = series_mod.resolve_for_write(
                conn,
                edge_id=edge_id,
                data_type=data_type_str,
                name=name,
            )
        return _write_one_series(
            self._td,
            self._pool,
            df=df,
            meta=meta,
            unit=unit,
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
    ) -> pl.DataFrame:
        with self._pool.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            data_type_str = str(data_type).lower() if data_type else None
            meta = series_mod.resolve_for_read(
                conn,
                edge_ids=[edge_id],
                data_type=data_type_str,
                name=name,
            )
            if meta.is_empty():
                return pl.DataFrame()

            result = _read_and_convert(
                self._td,
                meta,
                unit,
                start_valid=start_valid,
                end_valid=end_valid,
                start_known=start_known,
                end_known=end_known,
                include_updates=include_updates,
                include_knowledge_time=include_knowledge_time,
            )
            return join_edge_hierarchy(conn, result, meta)


# ---------------------------------------------------------------------------
# Shared write/read helpers
# ---------------------------------------------------------------------------


def _write_one_series(
    td,
    pool,
    *,
    df: pl.DataFrame,
    meta: dict,
    unit: str | None,
    knowledge_time: datetime | None,
    run_id: int | None,
    workflow_id: str | None,
    model_name: str | None,
    run_start_time: datetime | None,
    run_finish_time: datetime | None,
    run_params: dict | None,
) -> int:
    """Unit-convert, enforce the OVERLAPPING contract, upsert the run, then
    call td.write. Returns the run_id.
    """
    # OVERLAPPING contract: knowledge_time must be supplied.
    if meta["timeseries_type"] == "OVERLAPPING":
        kt_in_df = "knowledge_time" in df.columns
        if knowledge_time is None and not kt_in_df:
            raise ValueError(
                f"knowledge_time is required for OVERLAPPING series "
                f"(series_id={meta['series_id']}). Pass it as a kwarg or as a "
                f"'knowledge_time' column."
            )

    _prof = profiling._enabled

    if unit is not None:
        _t = time.perf_counter() if _prof else 0.0
        df = apply_unit_factor(df, unit, meta["canonical_unit"])
        if _prof:
            profiling._record(profiling.PHASE_EDB_UNIT_CONVERT, time.perf_counter() - _t)

    rid = run_id if run_id is not None else runs_mod.generate_run_id()
    df = df.with_columns(
        pl.lit(meta["series_id"], dtype=pl.Int64).alias("series_id"),
        pl.lit(rid, dtype=pl.Int64).alias("run_id"),
    )

    _t = time.perf_counter() if _prof else 0.0
    with pool.connection() as conn:
        runs_mod.upsert_run(
            conn,
            run_id=rid,
            workflow_id=workflow_id,
            model_name=model_name,
            run_start_time=run_start_time or datetime.now(UTC),
            run_finish_time=run_finish_time,
            run_params=run_params,
        )
        conn.commit()
    if _prof:
        profiling._record(profiling.PHASE_EDB_RUNS_UPSERT, time.perf_counter() - _t)

    td.write(df, retention=meta["retention"], knowledge_time=knowledge_time)
    return rid


def _read_and_convert(
    td,
    meta: pl.DataFrame,
    requested_unit: str | None,
    *,
    start_valid: datetime | None = None,
    end_valid: datetime | None = None,
    start_known: datetime | None = None,
    end_known: datetime | None = None,
    include_updates: bool = False,
    include_knowledge_time: bool = False,
) -> pl.DataFrame:
    """Single td.read over all resolved series_ids, across their retentions.

    The unified events table means one CH query handles cross-retention reads;
    the retention predicate is just for partition pruning.
    """
    series_ids = meta["series_id"].to_list()
    retentions = meta["retention"].unique().to_list()
    result = td.read(
        series_ids=series_ids,
        retention=retentions,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
        include_updates=include_updates,
        include_knowledge_time=include_knowledge_time,
    )
    if result.is_empty():
        return result

    if requested_unit is not None:
        _prof = profiling._enabled
        _t = time.perf_counter() if _prof else 0.0
        result = _apply_per_series_unit(result, meta, requested_unit)
        if _prof:
            profiling._record(profiling.PHASE_EDB_UNIT_CONVERT, time.perf_counter() - _t)
    return result


def _read_relative_and_convert(
    td,
    meta: pl.DataFrame,
    requested_unit: str | None,
    td_kwargs: dict,
) -> pl.DataFrame:
    series_ids = meta["series_id"].to_list()
    retentions = meta["retention"].unique().to_list()
    result = td.read_relative(series_ids=series_ids, retention=retentions, **td_kwargs)
    if result.is_empty():
        return result

    if requested_unit is not None:
        _prof = profiling._enabled
        _t = time.perf_counter() if _prof else 0.0
        result = _apply_per_series_unit(result, meta, requested_unit)
        if _prof:
            profiling._record(profiling.PHASE_EDB_UNIT_CONVERT, time.perf_counter() - _t)
    return result


def _apply_per_series_unit(
    result: pl.DataFrame,
    meta: pl.DataFrame,
    requested_unit: str,
) -> pl.DataFrame:
    """Multiply value by the per-series canonical→requested factor.

    Single join over (series_id, canonical_unit). Factor computed once per
    unique canonical_unit.
    """
    from energydb.units import compute_unit_factor

    unique_units = meta["canonical_unit"].unique().to_list()
    factors = {u: (compute_unit_factor(u, requested_unit) or 1.0) for u in unique_units}
    factor_df = pl.DataFrame(
        {
            "canonical_unit": list(factors.keys()),
            "_factor": list(factors.values()),
        },
        schema={"canonical_unit": pl.Utf8, "_factor": pl.Float64},
    )
    series_factor = (
        meta.select(["series_id", "canonical_unit"])
        .unique(subset=["series_id"])
        .join(factor_df, on="canonical_unit", how="left")
        .select(["series_id", "_factor"])
    )
    return (
        result.join(series_factor, on="series_id", how="left")
        .with_columns((pl.col("value") * pl.col("_factor")).alias("value"))
        .drop("_factor")
    )
