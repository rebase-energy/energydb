"""NodeScope and EdgeScope — fluent APIs for navigating and operating on
nodes and edges.

All .node() calls are lazy — they accumulate a path without hitting the DB.
Resolution happens in a single query when a terminal operation is called.
"""

from __future__ import annotations

from typing import Any

import polars as pl
from psycopg.types.json import Jsonb
from timedatamodel import TimeSeries, TimeSeriesDescriptor

from energydb._resolve import (
    build_edge_series_labels,
    build_series_labels,
    join_edge_hierarchy,
    join_hierarchy,
    resolve_edge_series_ids,
    resolve_node_id,
    resolve_series_ids,
    resolve_subtree_ids,
)
from energydb.serialization import reconstruct_node, reconstruct_edge, serialize_node


class NodeScope:
    """Accumulated scope for navigating and operating on nodes.

    Lazy: ``.node()`` and ``.find()`` build up filters without hitting the DB.
    Terminal operations (``.read()``, ``.write()``, ``.children()``, etc.)
    trigger resolution and execute.
    """

    def __init__(
        self,
        td,
        *,
        node_id: int | None = None,
        name_chain: list[str] | None = None,
        find_filters: dict[str, Any] | None = None,
    ):
        self._td = td
        self._node_id = node_id
        self._name_chain = name_chain or []
        self._find_filters = find_filters

    # ------------------------------------------------------------------
    # Navigation (lazy — returns new scope)
    # ------------------------------------------------------------------

    def node(self, name: str | None = None, *, id: int | None = None) -> NodeScope:
        """Navigate to a child node by name or id."""
        if id is not None:
            return NodeScope(self._td, node_id=id)
        if name is None:
            raise ValueError("Must provide name or id")
        return NodeScope(
            self._td,
            node_id=self._node_id,
            name_chain=self._name_chain + [name],
        )

    def find(self, *, type: str | None = None, name: str | None = None, **property_filters) -> NodeScope:
        """Filter descendants by type, name, and/or property values."""
        filters: dict[str, Any] = {}
        if type is not None:
            filters["node_type"] = type
        if name is not None:
            filters["name"] = name
        filters.update(property_filters)
        return NodeScope(
            self._td,
            node_id=self._node_id,
            name_chain=self._name_chain,
            find_filters=filters,
        )

    # ------------------------------------------------------------------
    # Internal: resolve the lazy scope to a node_id
    # ------------------------------------------------------------------

    def _resolve_node_id(self, conn) -> int:
        """Resolve the accumulated path/id to a concrete node_id."""
        if self._node_id is not None and not self._name_chain:
            return self._node_id
        if self._name_chain:
            return resolve_node_id(conn, self._name_chain, start_id=self._node_id)
        raise ValueError("NodeScope has no node_id or name chain to resolve")

    def _resolve_target_node_ids(self, conn) -> list[int]:
        """Resolve to target node_ids, applying find filters if present."""
        root_id = self._resolve_node_id(conn)
        subtree_ids = resolve_subtree_ids(conn, root_id)

        if not self._find_filters:
            return subtree_ids

        # Apply find filters within the subtree
        conditions = ["node_id = ANY(%s)"]
        params: list[Any] = [subtree_ids]

        if "node_type" in self._find_filters:
            conditions.append("node_type = %s")
            params.append(self._find_filters["node_type"])
        if "name" in self._find_filters:
            conditions.append("name = %s")
            params.append(self._find_filters["name"])

        # Property filters
        for key, value in self._find_filters.items():
            if key in ("node_type", "name"):
                continue
            conditions.append("data->>%s = %s")
            params.append(key)
            params.append(str(value))

        where = " AND ".join(conditions)
        rows = conn.execute(
            f"SELECT node_id FROM energydb.node WHERE {where}",
            params,
        ).fetchall()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # Terminal: get EDM object
    # ------------------------------------------------------------------

    def get(self):
        """Return the EDM Node object for this scope (no children)."""
        with self._td.connection() as conn:
            node_id = self._resolve_node_id(conn)
            row = conn.execute(
                "SELECT node_id, node_type, name, data "
                "FROM energydb.node WHERE node_id = %s",
                (node_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Node not found: id={node_id}")
            return reconstruct_node({
                "node_id": row[0],
                "node_type": row[1],
                "name": row[2],
                "data": row[3],
            })

    # ------------------------------------------------------------------
    # Terminal: hierarchy queries
    # ------------------------------------------------------------------

    def children(self, *, type: str | None = None) -> list[dict]:
        """List direct children of this node."""
        with self._td.connection() as conn:
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
                    "SELECT node_id, node_type, name, data "
                    "FROM energydb.node WHERE parent_id = %s ORDER BY name",
                    (node_id,),
                ).fetchall()
            return [
                {"node_id": r[0], "node_type": r[1], "name": r[2], "data": r[3]}
                for r in rows
            ]

    def descendants(self, *, type: str | None = None) -> list[dict]:
        """List all descendants (recursive), optionally filtered by type."""
        with self._td.connection() as conn:
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

            return [
                {"node_id": r[0], "node_type": r[1], "name": r[2], "data": r[3]}
                for r in rows
            ]

    # ------------------------------------------------------------------
    # Terminal: CRUD mutations
    # ------------------------------------------------------------------

    def create_child(self, edm_obj) -> int:
        """Create a child node under this scope. Upsert semantics.

        If edm_obj.timeseries contains TimeSeriesDescriptors, they are
        automatically registered after the node is created.
        """
        row_data = serialize_node(edm_obj)
        with self._td.connection() as conn:
            parent_id = self._resolve_node_id(conn)
            row = conn.execute(
                "INSERT INTO energydb.node "
                "(node_type, name, parent_id, data) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT ON CONSTRAINT node_child_uniq "
                "DO UPDATE SET data = EXCLUDED.data, "
                "updated_at = now() "
                "RETURNING node_id",
                (
                    row_data["node_type"], row_data["name"], parent_id,
                    row_data["data"],
                ),
            ).fetchone()
            conn.commit()
            node_id = row[0]

        NodeScope(self._td, node_id=node_id)._register_descriptors(edm_obj)
        return node_id

    def rename(self, new_name: str) -> None:
        """Rename this node. Label sync deferred (node_id in labels is immutable anchor)."""
        with self._td.connection() as conn:
            node_id = self._resolve_node_id(conn)
            conn.execute(
                "UPDATE energydb.node SET name = %s, updated_at = now() WHERE node_id = %s",
                (new_name, node_id),
            )
            conn.commit()

    def update(self, *, data: dict | None = None, name: str | None = None) -> None:
        """Update node fields.

        Args:
            data: Dict merged into the existing ``data`` JSONB via
                ``||`` — top-level keys overwrite. Pass geometry/tz/
                domain fields as a single dict.
            name: New node name.
        """
        sets = ["updated_at = now()"]
        params: list[Any] = []

        if data is not None:
            sets.append("data = data || %s")
            params.append(Jsonb(data))

        if name is not None:
            sets.append("name = %s")
            params.append(name)

        with self._td.connection() as conn:
            node_id = self._resolve_node_id(conn)
            params.append(node_id)
            conn.execute(
                f"UPDATE energydb.node SET {', '.join(sets)} WHERE node_id = %s",
                params,
            )
            conn.commit()

    def delete(self) -> None:
        """Delete this node. CASCADE handles children and node_series."""
        with self._td.connection() as conn:
            node_id = self._resolve_node_id(conn)
            conn.execute(
                "DELETE FROM energydb.node WHERE node_id = %s", (node_id,)
            )
            conn.commit()

    def _register_descriptors(self, edm_obj) -> list[tuple[int, pl.DataFrame]]:
        """Auto-register series from ``edm_obj.timeseries``.

        Accepts both :class:`TimeSeriesDescriptor` (schema only) and
        :class:`TimeSeries` (schema + data). For ``TimeSeries`` entries that
        carry non-empty data, returns ``(series_id, df)`` pairs so the caller
        can issue a bulk data write afterwards.
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
        descriptor_or_name=None,
        *,
        name: str | None = None,
        unit: str = "dimensionless",
        data_type: str | None = None,
        overlapping: bool = False,
        retention: str = "medium",
        description: str | None = None,
    ) -> int:
        """Register a time series for this node.

        Accepts either a TimeSeriesDescriptor or explicit kwargs.
        Returns the timedb series_id.
        """
        if isinstance(descriptor_or_name, TimeSeriesDescriptor):
            desc = descriptor_or_name
            name = desc.name
            unit = desc.unit
            data_type = str(desc.data_type).lower() if desc.data_type else data_type
            overlapping = desc.timeseries_type.value == "OVERLAPPING"
            description = desc.description
        elif isinstance(descriptor_or_name, str):
            # Positional string = name shorthand
            name = descriptor_or_name

        if name is None:
            raise ValueError("name is required (either via descriptor or kwarg)")
        if data_type is None:
            raise ValueError("data_type is required (either via descriptor or kwarg)")

        data_type_str = data_type.lower() if hasattr(data_type, "lower") else str(data_type).lower()

        with self._td.connection() as conn:
            node_id = self._resolve_node_id(conn)

            # Build labels for timedb series
            labels = build_series_labels(conn, node_id, data_type_str)

            # Create series in timedb (get-or-create)
            series_id = self._td.create_series(
                name=name,
                unit=unit,
                labels=labels,
                description=description,
                overlapping=overlapping,
                retention=retention,
                conn=conn,
            )

            # Insert link in node_series
            conn.execute(
                "INSERT INTO energydb.node_series (node_id, series_id, data_type, name) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (node_id, series_id, data_type_str, name),
            )
            conn.commit()

        return series_id

    # ------------------------------------------------------------------
    # Terminal: time series I/O
    # ------------------------------------------------------------------

    def read(
        self,
        *,
        data_type: str | None = None,
        name: str | None = None,
        start_valid=None,
        end_valid=None,
        **timedb_kwargs,
    ) -> pl.DataFrame:
        """Read time series data for this scope (node + descendants).

        Resolves subtree → series_ids → td.read() → join hierarchy context.
        """
        with self._td.connection() as conn:
            target_ids = self._resolve_target_node_ids(conn)
            if not target_ids:
                return pl.DataFrame()

            series_ids = resolve_series_ids(
                conn, target_ids, data_type=data_type, name=name
            )
            if not series_ids:
                return pl.DataFrame()

            # Build manifest for timedb
            manifest = pl.DataFrame({"series_id": series_ids})
            result = self._td.read(
                manifest,
                series_col="series_id",
                start_valid=start_valid,
                end_valid=end_valid,
                **timedb_kwargs,
            )

            # Join hierarchy context
            return join_hierarchy(conn, result, series_ids)

    def write(
        self,
        df: pl.DataFrame,
        *,
        name: str,
        data_type: str,
        **timedb_kwargs,
    ) -> list:
        """Write time series data for a single series on this node.

        The node must have a registered series matching (data_type, name).
        df must have valid_time + value columns.
        """
        data_type_str = data_type.lower() if hasattr(data_type, "lower") else str(data_type).lower()

        with self._td.connection() as conn:
            node_id = self._resolve_node_id(conn)
            series_ids = resolve_series_ids(
                conn, [node_id], data_type=data_type_str, name=name
            )
            if not series_ids:
                raise ValueError(
                    f"No series found for node_id={node_id}, "
                    f"data_type={data_type_str}, name={name}"
                )
            if len(series_ids) > 1:
                raise ValueError(
                    f"Multiple series found for node_id={node_id}, "
                    f"data_type={data_type_str}, name={name}: {series_ids}"
                )

            series_id = series_ids[0]

            # Add series_id to the DataFrame and write via timedb
            write_df = df.with_columns(pl.lit(series_id).alias("series_id"))
            return self._td.write(write_df, series_col="series_id", **timedb_kwargs)

    def read_relative(
        self,
        *,
        data_type: str,
        name: str,
        **timedb_kwargs,
    ) -> pl.DataFrame:
        """Read overlapping series with per-window cutoff."""
        with self._td.connection() as conn:
            target_ids = self._resolve_target_node_ids(conn)
            if not target_ids:
                return pl.DataFrame()

            data_type_str = data_type.lower() if hasattr(data_type, "lower") else str(data_type).lower()
            series_ids = resolve_series_ids(
                conn, target_ids, data_type=data_type_str, name=name
            )
            if not series_ids:
                return pl.DataFrame()

            manifest = pl.DataFrame({"series_id": series_ids})
            result = self._td.read_relative(
                manifest, series_col="series_id", **timedb_kwargs
            )
            return join_hierarchy(conn, result, series_ids)


class EdgeScope:
    """Scope for operating on a single edge.

    Edges are flat (no hierarchy), so there's no lazy path navigation.
    All operations resolve directly to an edge_id.
    """

    def __init__(
        self,
        td,
        *,
        edge_id: int | None = None,
        name: str | None = None,
    ):
        self._td = td
        self._edge_id = edge_id
        self._name = name

    # ------------------------------------------------------------------
    # Internal: resolve to edge_id
    # ------------------------------------------------------------------

    def _resolve_edge_id(self, conn) -> int:
        """Resolve to a concrete edge_id."""
        if self._edge_id is not None:
            return self._edge_id
        if self._name is not None:
            from energydb._resolve import resolve_edge_id_by_name
            return resolve_edge_id_by_name(conn, self._name)
        raise ValueError("EdgeScope has no edge_id or name to resolve")

    # ------------------------------------------------------------------
    # Terminal: get EDM object
    # ------------------------------------------------------------------

    def get(self):
        """Return the EDM Edge object for this scope."""
        from energydb._resolve import resolve_path
        with self._td.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            row = conn.execute(
                "SELECT edge_id, edge_type, name, data, "
                "from_node_id, to_node_id "
                "FROM energydb.edge WHERE edge_id = %s",
                (edge_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Edge not found: id={edge_id}")

            from_path = resolve_path(conn, row[4])
            to_path = resolve_path(conn, row[5])

            return reconstruct_edge({
                "edge_id": row[0],
                "edge_type": row[1],
                "name": row[2],
                "data": row[3],
                "from_node_path": from_path,
                "to_node_path": to_path,
            })

    # ------------------------------------------------------------------
    # Terminal: navigation helpers
    # ------------------------------------------------------------------

    def from_node(self) -> NodeScope:
        """Return a NodeScope pointing to this edge's from_node."""
        with self._td.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            row = conn.execute(
                "SELECT from_node_id FROM energydb.edge WHERE edge_id = %s",
                (edge_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Edge not found: id={edge_id}")
            return NodeScope(self._td, node_id=row[0])

    def to_node(self) -> NodeScope:
        """Return a NodeScope pointing to this edge's to_node."""
        with self._td.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            row = conn.execute(
                "SELECT to_node_id FROM energydb.edge WHERE edge_id = %s",
                (edge_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Edge not found: id={edge_id}")
            return NodeScope(self._td, node_id=row[0])

    # ------------------------------------------------------------------
    # Terminal: CRUD mutations
    # ------------------------------------------------------------------

    def update(self, *, data: dict | None = None, name: str | None = None) -> None:
        """Update edge fields.

        Args:
            data: Dict merged into the existing ``data`` JSONB via ``||``
                (top-level keys overwrite). Pass ``directed`` or any domain
                property here.
            name: New edge name.
        """
        sets = ["updated_at = now()"]
        params: list[Any] = []

        if data is not None:
            sets.append("data = data || %s")
            params.append(Jsonb(data))

        if name is not None:
            sets.append("name = %s")
            params.append(name)

        with self._td.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            params.append(edge_id)
            conn.execute(
                f"UPDATE energydb.edge SET {', '.join(sets)} WHERE edge_id = %s",
                params,
            )
            conn.commit()

    def delete(self) -> None:
        """Delete this edge. CASCADE handles edge_series."""
        with self._td.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            conn.execute(
                "DELETE FROM energydb.edge WHERE edge_id = %s",
                (edge_id,),
            )
            conn.commit()

    # ------------------------------------------------------------------
    # Terminal: series registration
    # ------------------------------------------------------------------

    def register_series(
        self,
        descriptor_or_name=None,
        *,
        name: str | None = None,
        unit: str = "dimensionless",
        data_type: str | None = None,
        overlapping: bool = False,
        retention: str = "medium",
        description: str | None = None,
    ) -> int:
        """Register a time series for this edge.

        Accepts either a TimeSeriesDescriptor or explicit kwargs.
        Returns the timedb series_id.
        """
        if isinstance(descriptor_or_name, TimeSeriesDescriptor):
            desc = descriptor_or_name
            name = desc.name
            unit = desc.unit
            data_type = str(desc.data_type).lower() if desc.data_type else data_type
            overlapping = desc.timeseries_type.value == "OVERLAPPING"
            description = desc.description
        elif isinstance(descriptor_or_name, str):
            name = descriptor_or_name

        if name is None:
            raise ValueError("name is required (either via descriptor or kwarg)")
        if data_type is None:
            raise ValueError("data_type is required (either via descriptor or kwarg)")

        data_type_str = data_type.lower() if hasattr(data_type, "lower") else str(data_type).lower()

        with self._td.connection() as conn:
            edge_id = self._resolve_edge_id(conn)

            labels = build_edge_series_labels(conn, edge_id, data_type_str)

            series_id = self._td.create_series(
                name=name,
                unit=unit,
                labels=labels,
                description=description,
                overlapping=overlapping,
                retention=retention,
                conn=conn,
            )

            conn.execute(
                "INSERT INTO energydb.edge_series (edge_id, series_id, data_type, name) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (edge_id, series_id, data_type_str, name),
            )
            conn.commit()

        return series_id

    # ------------------------------------------------------------------
    # Terminal: time series I/O
    # ------------------------------------------------------------------

    def read(
        self,
        *,
        data_type: str | None = None,
        name: str | None = None,
        start_valid=None,
        end_valid=None,
        **timedb_kwargs,
    ) -> pl.DataFrame:
        """Read time series data for this edge."""
        with self._td.connection() as conn:
            edge_id = self._resolve_edge_id(conn)

            series_ids = resolve_edge_series_ids(
                conn, [edge_id], data_type=data_type, name=name
            )
            if not series_ids:
                return pl.DataFrame()

            manifest = pl.DataFrame({"series_id": series_ids})
            result = self._td.read(
                manifest,
                series_col="series_id",
                start_valid=start_valid,
                end_valid=end_valid,
                **timedb_kwargs,
            )

            return join_edge_hierarchy(conn, result, series_ids)

    def write(
        self,
        df: pl.DataFrame,
        *,
        name: str,
        data_type: str,
        **timedb_kwargs,
    ) -> list:
        """Write time series data for a single series on this edge."""
        data_type_str = data_type.lower() if hasattr(data_type, "lower") else str(data_type).lower()

        with self._td.connection() as conn:
            edge_id = self._resolve_edge_id(conn)
            series_ids = resolve_edge_series_ids(
                conn, [edge_id], data_type=data_type_str, name=name
            )
            if not series_ids:
                raise ValueError(
                    f"No series found for edge_id={edge_id}, "
                    f"data_type={data_type_str}, name={name}"
                )
            if len(series_ids) > 1:
                raise ValueError(
                    f"Multiple series found for edge_id={edge_id}, "
                    f"data_type={data_type_str}, name={name}: {series_ids}"
                )

            series_id = series_ids[0]
            write_df = df.with_columns(pl.lit(series_id).alias("series_id"))
            return self._td.write(write_df, series_col="series_id", **timedb_kwargs)
