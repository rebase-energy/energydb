"""EnergyDataClient — main entry point for energydb."""

from __future__ import annotations

from typing import Any

import energydatamodel as edm
import polars as pl
from energydatamodel.reference import Reference
from psycopg.types.json import Jsonb
from sqlalchemy import create_engine
from timedb import TimeDataClient

from energydb._resolve import (
    join_edge_hierarchy,
    join_hierarchy,
    resolve_node_id_by_name,
    resolve_manifest,
    resolve_path,
    resolve_paths_bulk,
    resolve_subtree_ids,
)
from energydb.models import Base, ENERGYDB_TABLES
from energydb.scope import EdgeScope, NodeScope
from energydb.serialization import (
    reconstruct_edge,
    reconstruct_node,
    serialize_edge,
    serialize_node,
)


_SEARCH_PATH = "SET search_path TO energydb, timedb, public"


class EnergyDataClient:
    """Database client for energy assets, hierarchy, and time series.

    Uses composition: holds a reference to a :class:`TimeDataClient` and uses
    its shared connection pool for all Postgres operations.
    """

    def __init__(self, td: TimeDataClient):
        self.td = td

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def create(self, engine_or_conninfo=None):
        """Create EnergyDB schema and tables.

        Accepts an optional SQLAlchemy engine or conninfo string.
        Falls back to td's connection info for a short-lived engine.
        """
        # Ensure schema exists via raw connection
        with self.td.connection() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS energydb")
            conn.commit()

        # Create tables via SQLAlchemy
        if engine_or_conninfo is None:
            conninfo = self.td._pool.conninfo
            engine_or_conninfo = f"postgresql+psycopg://{conninfo.split('://', 1)[-1]}" if "://" in conninfo else conninfo
        if isinstance(engine_or_conninfo, str):
            engine = create_engine(engine_or_conninfo)
        else:
            engine = engine_or_conninfo

        Base.metadata.create_all(engine, tables=ENERGYDB_TABLES, checkfirst=True)

    def delete(self):
        """Drop EnergyDB schema and all tables."""
        with self.td.connection() as conn:
            conn.execute("DROP SCHEMA IF EXISTS energydb CASCADE")
            conn.commit()

    # ------------------------------------------------------------------
    # Fluent entry
    # ------------------------------------------------------------------

    def node(self, name: str | None = None, *, id: int | None = None) -> NodeScope:
        """Entry point for fluent node navigation.

        Args:
            name: Node name (raises if ambiguous across roots).
            id: Node id (always unambiguous, what platform uses).
        """
        if id is not None:
            return NodeScope(self.td, node_id=id)
        if name is not None:
            return NodeScope(self.td, name_chain=[name])
        raise ValueError("Must provide name or id")

    def edge(self, name: str | None = None, *, id: int | None = None) -> EdgeScope:
        """Entry point for edge operations.

        Args:
            name: Edge name.
            id: Edge id.
        """
        if id is not None:
            return EdgeScope(self.td, edge_id=id)
        if name is not None:
            return EdgeScope(self.td, name=name)
        raise ValueError("Must provide name or id")

    # ------------------------------------------------------------------
    # Imperative CRUD — nodes
    # ------------------------------------------------------------------

    def create_node(self, edm_obj, *, parent: int | str | None = None) -> int:
        """Create a node in the hierarchy. Upsert semantics (idempotent).

        If edm_obj.timeseries contains TimeSeriesDescriptors or TimeSeries,
        they are automatically registered after the node is created. Any
        attached data on TimeSeries is ignored here — use :meth:`write` to
        persist a whole tree including data in one call.

        Args:
            edm_obj: Any EDM Node object (Portfolio, Site, WindTurbine, etc.)
            parent: Parent node — int (node_id), str (name), or None (root).

        Returns:
            node_id of the created/existing node.
        """
        node_id, _ = self._create_node(edm_obj, parent=parent)
        return node_id

    def _create_node(
        self, edm_obj, *, parent: int | str | None = None
    ) -> tuple[int, list[tuple[int, pl.DataFrame]]]:
        """Create a node and return ``(node_id, pending_data_writes)``.

        ``pending_data_writes`` contains ``(series_id, df)`` pairs collected
        from any :class:`TimeSeries` entries in ``edm_obj.timeseries``. The
        caller is responsible for writing these — :meth:`create_node` ignores
        them, :meth:`write` batches them into a single timedb write.
        """
        data = serialize_node(edm_obj)

        with self.td.connection() as conn:
            conn.execute(_SEARCH_PATH)

            # Resolve parent
            parent_id = None
            if isinstance(parent, int):
                parent_id = parent
            elif isinstance(parent, str):
                parent_id = resolve_node_id_by_name(conn, parent)

            if parent_id is not None:
                # Child node — use child uniqueness constraint
                row = conn.execute(
                    "INSERT INTO node "
                    "(node_type, name, parent_id, properties, "
                    "latitude, longitude, altitude, timezone) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT ON CONSTRAINT node_child_uniq "
                    "DO UPDATE SET properties = EXCLUDED.properties, "
                    "latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, "
                    "altitude = EXCLUDED.altitude, timezone = EXCLUDED.timezone, "
                    "updated_at = now() "
                    "RETURNING node_id",
                    (
                        data["node_type"], data["name"], parent_id,
                        data["properties"], data["latitude"],
                        data["longitude"], data["altitude"], data["timezone"],
                    ),
                ).fetchone()
            else:
                # Root node — no unique constraint on roots (multi-tenant),
                # so do an explicit check first
                existing = conn.execute(
                    "SELECT node_id FROM node "
                    "WHERE name = %s AND node_type = %s AND parent_id IS NULL",
                    (data["name"], data["node_type"]),
                ).fetchone()

                if existing:
                    # Update existing root
                    conn.execute(
                        "UPDATE node SET properties = %s, "
                        "latitude = %s, longitude = %s, altitude = %s, "
                        "timezone = %s, updated_at = now() "
                        "WHERE node_id = %s",
                        (
                            data["properties"], data["latitude"],
                            data["longitude"], data["altitude"],
                            data["timezone"], existing[0],
                        ),
                    )
                    conn.commit()
                    node_id = existing[0]
                else:
                    row = conn.execute(
                        "INSERT INTO node "
                        "(node_type, name, parent_id, properties, "
                        "latitude, longitude, altitude, timezone) "
                        "VALUES (%s, %s, NULL, %s, %s, %s, %s, %s) "
                        "RETURNING node_id",
                        (
                            data["node_type"], data["name"],
                            data["properties"], data["latitude"],
                            data["longitude"], data["altitude"],
                            data["timezone"],
                        ),
                    ).fetchone()
                    conn.commit()
                    node_id = row[0]

            if parent_id is not None:
                conn.commit()
                node_id = row[0]

        pending = self.node(id=node_id)._register_descriptors(edm_obj)
        return node_id, pending

    # ------------------------------------------------------------------
    # Imperative CRUD — edges
    # ------------------------------------------------------------------

    def create_edge(
        self,
        edm_obj,
        *,
        from_node: int | str | None = None,
        to_node: int | str | None = None,
    ) -> int:
        """Create an edge between two nodes. Upsert semantics.

        Args:
            edm_obj: An EDM Edge object (Line, Link, Transformer, etc.)
            from_node: Source node — int (node_id) or str (name).
                Falls back to edm_obj.from_entity Reference if not provided.
            to_node: Target node — int (node_id) or str (name).
                Falls back to edm_obj.to_entity Reference if not provided.

        Returns:
            edge_id of the created/existing edge.
        """
        data = serialize_edge(edm_obj)

        with self.td.connection() as conn:
            conn.execute(_SEARCH_PATH)

            # Resolve from_node / to_node
            from_id = _resolve_node_ref(conn, from_node, edm_obj, "from_entity")
            to_id = _resolve_node_ref(conn, to_node, edm_obj, "to_entity")

            row = conn.execute(
                "INSERT INTO edge "
                "(edge_type, name, from_node_id, to_node_id, "
                "properties, directed) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT ON CONSTRAINT edge_uniq "
                "DO UPDATE SET properties = EXCLUDED.properties, "
                "name = EXCLUDED.name, directed = EXCLUDED.directed, "
                "updated_at = now() "
                "RETURNING edge_id",
                (
                    data["edge_type"], data["name"],
                    from_id, to_id,
                    data["properties"], data["directed"],
                ),
            ).fetchone()
            conn.commit()
            edge_id = row[0]

        # Auto-register series from edm_obj.timeseries
        ts_list = getattr(edm_obj, "timeseries", None)
        if ts_list:
            scope = EdgeScope(self.td, edge_id=edge_id)
            for ts in ts_list:
                if isinstance(ts, edm.TimeSeries):
                    scope.register_series(ts.to_descriptor())
                elif hasattr(ts, "name"):
                    scope.register_series(ts)

        return edge_id

    def get_edge(self, name: str | None = None, *, id: int | None = None):
        """Get a single edge as an EDM object.

        Args:
            name: Edge name (raises if ambiguous).
            id: Edge id.
        """
        with self.td.connection() as conn:
            conn.execute(_SEARCH_PATH)

            if id is not None:
                row = conn.execute(
                    "SELECT edge_id, edge_type, name, properties, "
                    "directed, from_node_id, to_node_id "
                    "FROM edge WHERE edge_id = %s",
                    (id,),
                ).fetchone()
            elif name is not None:
                rows = conn.execute(
                    "SELECT edge_id, edge_type, name, properties, "
                    "directed, from_node_id, to_node_id "
                    "FROM edge WHERE name = %s",
                    (name,),
                ).fetchall()
                if len(rows) == 0:
                    raise ValueError(f"Edge not found: {name}")
                if len(rows) > 1:
                    ids = [r[0] for r in rows]
                    raise ValueError(
                        f"Multiple edges named '{name}' (ids: {ids}). "
                        f"Use get_edge(id=...) to disambiguate."
                    )
                row = rows[0]
            else:
                raise ValueError("Must provide name or id")

            if row is None:
                raise ValueError(f"Edge not found: id={id}")

            # Resolve from/to node paths
            from_path = resolve_path(conn, row[5])
            to_path = resolve_path(conn, row[6])

            return reconstruct_edge({
                "edge_id": row[0],
                "edge_type": row[1],
                "name": row[2],
                "properties": row[3],
                "directed": row[4],
                "from_node_path": from_path,
                "to_node_path": to_path,
            })

    def query_edges(
        self,
        *,
        type: str | None = None,
        within: str | int | None = None,
        **property_filters,
    ) -> list:
        """Query edges as a flat list of EDM objects.

        Args:
            type: Filter by edge_type.
            within: Restrict to edges where from_node or to_node
                is a descendant of this node (name or id).
            **property_filters: Filter by JSONB properties.
        """
        with self.td.connection() as conn:
            conn.execute(_SEARCH_PATH)

            conditions = []
            params: list[Any] = []

            if within is not None:
                if isinstance(within, int):
                    within_id = within
                else:
                    within_id = resolve_node_id_by_name(conn, within)
                subtree_ids = resolve_subtree_ids(conn, within_id)
                conditions.append(
                    "(from_node_id = ANY(%s) OR to_node_id = ANY(%s))"
                )
                params.append(subtree_ids)
                params.append(subtree_ids)

            if type:
                conditions.append("edge_type = %s")
                params.append(type)

            for key, value in property_filters.items():
                conditions.append("properties->>%s = %s")
                params.append(key)
                params.append(str(value))

            where = " AND ".join(conditions) if conditions else "TRUE"
            rows = conn.execute(
                f"SELECT edge_id, edge_type, name, properties, "
                f"directed, from_node_id, to_node_id "
                f"FROM edge WHERE {where} ORDER BY name",
                params,
            ).fetchall()

            # Resolve all node paths in bulk
            all_node_ids = list(
                set(r[5] for r in rows) | set(r[6] for r in rows)
            )
            paths = resolve_paths_bulk(conn, all_node_ids)

            return [
                reconstruct_edge({
                    "edge_id": r[0],
                    "edge_type": r[1],
                    "name": r[2],
                    "properties": r[3],
                    "directed": r[4],
                    "from_node_path": paths.get(r[5], ""),
                    "to_node_path": paths.get(r[6], ""),
                })
                for r in rows
            ]

    # ------------------------------------------------------------------
    # Read hierarchy
    # ------------------------------------------------------------------

    def get_node(self, name: str | None = None, *, id: int | None = None):
        """Get a single node as an EDM object (no children).

        Args:
            name: Node name (raises if ambiguous).
            id: Node id.
        """
        with self.td.connection() as conn:
            conn.execute(_SEARCH_PATH)

            if id is not None:
                row = conn.execute(
                    "SELECT node_id, node_type, name, properties, "
                    "latitude, longitude, altitude, timezone "
                    "FROM node WHERE node_id = %s",
                    (id,),
                ).fetchone()
            elif name is not None:
                rows = conn.execute(
                    "SELECT node_id, node_type, name, properties, "
                    "latitude, longitude, altitude, timezone "
                    "FROM node WHERE name = %s",
                    (name,),
                ).fetchall()
                if len(rows) == 0:
                    raise ValueError(f"Node not found: {name}")
                if len(rows) > 1:
                    ids = [r[0] for r in rows]
                    raise ValueError(
                        f"Multiple nodes named '{name}' (ids: {ids}). "
                        f"Use get_node(id=...) to disambiguate."
                    )
                row = rows[0]
            else:
                raise ValueError("Must provide name or id")

            if row is None:
                raise ValueError(f"Node not found: id={id}")

            return reconstruct_node(_row_to_dict(row))

    def get_tree(
        self,
        name: str | None = None,
        *,
        id: int | None = None,
        include_series: bool = False,
    ):
        """Get a full EDM tree with all descendants.

        Args:
            name: Root node name.
            id: Root node id.
            include_series: If True, attach TimeSeriesDescriptors to .timeseries.
        """
        with self.td.connection() as conn:
            conn.execute(_SEARCH_PATH)

            # Resolve root
            if id is not None:
                root_id = id
            elif name is not None:
                root_id = resolve_node_id_by_name(conn, name)
            else:
                raise ValueError("Must provide name or id")

            # Fetch entire subtree in one query
            rows = conn.execute(
                """
                WITH RECURSIVE subtree AS (
                    SELECT node_id, node_type, name, properties, parent_id,
                           latitude, longitude, altitude, timezone
                    FROM node WHERE node_id = %s
                    UNION ALL
                    SELECT n.node_id, n.node_type, n.name, n.properties, n.parent_id,
                           n.latitude, n.longitude, n.altitude, n.timezone
                    FROM node n JOIN subtree s ON n.parent_id = s.node_id
                )
                SELECT * FROM subtree
                """,
                (root_id,),
            ).fetchall()

            # Reconstruct each node as an EDM object
            nodes: dict[int, Any] = {}
            parent_map: dict[int, int | None] = {}
            for r in rows:
                node_id = r[0]
                parent_map[node_id] = r[4]  # parent_id
                nodes[node_id] = reconstruct_node(_row_to_dict_full(r))

            # Optionally attach series descriptors
            if include_series:
                from timedatamodel import TimeSeriesDescriptor, DataType, TimeSeriesType

                all_node_ids = list(nodes.keys())
                series_rows = conn.execute(
                    """
                    SELECT ns.node_id, ns.data_type, ns.name,
                           s.unit, s.overlapping, s.description
                    FROM node_series ns
                    JOIN series s ON ns.series_id = s.series_id
                    WHERE ns.node_id = ANY(%s)
                    """,
                    (all_node_ids,),
                ).fetchall()

                for sr in series_rows:
                    nid, dt, met, unit, overlapping, desc = sr
                    node_obj = nodes.get(nid)
                    if node_obj is None:
                        continue
                    descriptor = TimeSeriesDescriptor(
                        name=met,
                        unit=unit or "dimensionless",
                        data_type=DataType(dt.upper()) if dt else None,
                        timeseries_type=TimeSeriesType.OVERLAPPING if overlapping else TimeSeriesType.FLAT,
                        description=desc,
                    )
                    if node_obj.timeseries is None:
                        node_obj.timeseries = []
                    node_obj.timeseries.append(descriptor)

            # Build tree bottom-up using add_child
            for node_id, parent_id in parent_map.items():
                if parent_id is not None and parent_id in nodes:
                    nodes[parent_id].add_child(nodes[node_id])

            return nodes[root_id]

    def query_nodes(
        self,
        *,
        type: str | None = None,
        within: str | int | None = None,
        **property_filters,
    ) -> list:
        """Query nodes as a flat list of EDM objects.

        Args:
            type: Filter by node_type.
            within: Restrict to descendants of this node (name or id).
            **property_filters: Filter by JSONB properties.
        """
        with self.td.connection() as conn:
            conn.execute(_SEARCH_PATH)

            conditions = []
            params: list[Any] = []

            if within is not None:
                if isinstance(within, int):
                    within_id = within
                else:
                    within_id = resolve_node_id_by_name(conn, within)
                subtree_ids = resolve_subtree_ids(conn, within_id)
                conditions.append("node_id = ANY(%s)")
                params.append(subtree_ids)

            if type:
                conditions.append("node_type = %s")
                params.append(type)

            for key, value in property_filters.items():
                conditions.append("properties->>%s = %s")
                params.append(key)
                params.append(str(value))

            where = " AND ".join(conditions) if conditions else "TRUE"
            rows = conn.execute(
                f"SELECT node_id, node_type, name, properties, "
                f"latitude, longitude, altitude, timezone "
                f"FROM node WHERE {where} ORDER BY name",
                params,
            ).fetchall()

            return [reconstruct_node(_row_to_dict(r)) for r in rows]

    # ------------------------------------------------------------------
    # Bulk I/O
    # ------------------------------------------------------------------

    def read(
        self,
        manifest: pl.DataFrame,
        *,
        start_valid=None,
        end_valid=None,
        **timedb_kwargs,
    ) -> pl.DataFrame:
        """Bulk read via manifest. Supports node_id, path, or edge_id columns."""
        is_edge = "edge_id" in manifest.columns
        with self.td.connection() as conn:
            conn.execute(_SEARCH_PATH)
            resolved = resolve_manifest(conn, manifest)
            series_ids = resolved["series_id"].unique().to_list()

            result = self.td.read(
                resolved.select(["series_id"]),
                series_col="series_id",
                start_valid=start_valid,
                end_valid=end_valid,
                **timedb_kwargs,
            )

            if is_edge:
                return join_edge_hierarchy(conn, result, series_ids)
            return join_hierarchy(conn, result, series_ids)

    def write(self, target, *, parent: int | str | None = None, **timedb_kwargs):
        """Persist either a manifest DataFrame or an EDM tree.

        - **Manifest write** (``target`` is a ``polars.DataFrame``): the frame
          carries routing columns (``node_id`` or ``path``) and timedb data
          columns; series_ids are resolved server-side and written as a single
          bulk write.
        - **Tree write** (``target`` is an EDM ``Node``): walks the tree
          depth-first in two passes:
          1. Create all Nodes (skip Edges)
          2. Create all Edges (from/to References now resolvable)
          Both passes collect pending series data, single bulk write at the end.
          Returns the root ``node_id``.

        Both forms are idempotent.
        """
        if isinstance(target, pl.DataFrame):
            return self._write_manifest(target, **timedb_kwargs)
        return self._write_tree(target, parent=parent, **timedb_kwargs)

    def _write_manifest(self, df: pl.DataFrame, **timedb_kwargs) -> list:
        """Bulk write via DataFrame. Supports node_id or path routing columns."""
        with self.td.connection() as conn:
            conn.execute(_SEARCH_PATH)
            resolved = resolve_manifest(conn, df)

            # Keep only series_id + data columns for timedb
            data_cols = ["series_id", "valid_time", "value"]
            optional_cols = [
                "knowledge_time", "valid_time_end", "change_time",
                "changed_by", "annotation",
            ]
            for col in optional_cols:
                if col in resolved.columns:
                    data_cols.append(col)

            write_df = resolved.select(
                [c for c in data_cols if c in resolved.columns]
            )
            return self.td.write(write_df, series_col="series_id", **timedb_kwargs)

    def _write_tree(
        self, tree, *, parent: int | str | None = None, **timedb_kwargs
    ) -> int:
        """Walk an EDM tree, upsert every node + edge + series, bulk-write all data.

        Two-pass approach:
        1. Walk node.children(), create all Nodes (skip Edges)
        2. Walk again, create all Edges (from/to References now resolvable)
        Both passes collect pending series data for a single bulk write.
        """
        pending: list[tuple[int, pl.DataFrame]] = []
        root_id: int | None = None

        # Pass 1: Create all nodes
        def _visit_nodes(obj, parent_ref):
            nonlocal root_id
            node_id, node_pending = self._create_node(obj, parent=parent_ref)
            pending.extend(node_pending)
            if root_id is None:
                root_id = node_id
            for child in obj.children():
                if isinstance(child, edm.Edge):
                    continue
                _visit_nodes(child, node_id)

        _visit_nodes(tree, parent)

        # Pass 2: Create all edges
        def _visit_edges(obj):
            for child in obj.children():
                if isinstance(child, edm.Edge):
                    self.create_edge(child)
                else:
                    _visit_edges(child)

        _visit_edges(tree)

        if pending:
            frames = [
                df.with_columns(pl.lit(series_id).alias("series_id"))
                for series_id, df in pending
            ]
            self.td.write(
                pl.concat(frames, how="diagonal_relaxed"),
                series_col="series_id",
                **timedb_kwargs,
            )

        assert root_id is not None  # tree always has at least one node
        return root_id

    def read_relative(
        self,
        manifest: pl.DataFrame,
        **timedb_kwargs,
    ) -> pl.DataFrame:
        """Bulk relative read via manifest."""
        is_edge = "edge_id" in manifest.columns
        with self.td.connection() as conn:
            conn.execute(_SEARCH_PATH)
            resolved = resolve_manifest(conn, manifest)
            series_ids = resolved["series_id"].unique().to_list()

            result = self.td.read_relative(
                resolved.select(["series_id"]),
                series_col="series_id",
                **timedb_kwargs,
            )

            if is_edge:
                return join_edge_hierarchy(conn, result, series_ids)
            return join_hierarchy(conn, result, series_ids)


# ---------------------------------------------------------------------------
# Row conversion helpers
# ---------------------------------------------------------------------------


def _row_to_dict(row) -> dict[str, Any]:
    """Convert a (node_id, node_type, name, properties, lat, lon, alt, tz) tuple."""
    return {
        "node_id": row[0],
        "node_type": row[1],
        "name": row[2],
        "properties": row[3],
        "latitude": row[4],
        "longitude": row[5],
        "altitude": row[6],
        "timezone": row[7],
    }


def _row_to_dict_full(row) -> dict[str, Any]:
    """Convert a subtree CTE row (node_id, type, name, props, parent_id, lat, lon, alt, tz)."""
    return {
        "node_id": row[0],
        "node_type": row[1],
        "name": row[2],
        "properties": row[3],
        # row[4] is parent_id — not needed for reconstruction
        "latitude": row[5],
        "longitude": row[6],
        "altitude": row[7],
        "timezone": row[8],
    }


def _resolve_node_ref(
    conn, explicit: int | str | None, edm_obj, attr: str
) -> int:
    """Resolve a node reference from explicit arg or edm_obj attribute.

    Args:
        conn: DB connection.
        explicit: Explicitly provided node_id (int) or str (name or path).
        edm_obj: EDM Edge object with from_entity/to_entity References.
        attr: "from_entity" or "to_entity".
    """
    if isinstance(explicit, int):
        return explicit
    if isinstance(explicit, str):
        return _resolve_name_or_path(conn, explicit)

    # Fall back to EDM Reference
    ref = getattr(edm_obj, attr, None)
    if ref is None:
        raise ValueError(
            f"No {attr} provided and edm_obj.{attr} is None"
        )
    if isinstance(ref, Reference):
        target = ref.target
        if isinstance(target, str):
            return _resolve_name_or_path(conn, target)
        # Resolved Entity — look up by name
        name = getattr(target, "name", None)
        if name is None:
            raise ValueError(f"Cannot resolve {attr}: Reference target has no name")
        return resolve_node_id_by_name(conn, name)

    raise ValueError(f"Cannot resolve {attr}: expected int, str, or Reference, got {type(ref)}")


def _resolve_name_or_path(conn, value: str) -> int:
    """Resolve a string that is either a simple name or a slash-separated path."""
    if "/" in value:
        from energydb._resolve import resolve_node_id
        return resolve_node_id(conn, value.split("/"))
    return resolve_node_id_by_name(conn, value)
