"""EnergyDataClient — extends TimeDB with energy asset storage."""

from __future__ import annotations

import importlib.resources
from typing import Any, Dict, List, Optional

import energydatamodel as edm
import psycopg
from timedb import TimeDataClient

from energydb.hierarchy import reconstruct_tree, walk_tree
from energydb.serialization import reconstruct_asset, serialize_asset, serialize_collection


class EnergyDataClient(TimeDataClient):
    """Database client for energy assets and time series.

    Extends TimeDB's TimeDataClient with tables for energy assets,
    collections (portfolios, sites), and asset-to-series links.
    """

    # ── Schema management ───────────────────────────────────

    def create(self, retention=None, **kwargs):
        """Create TimeDB tables and EnergyDB asset tables."""
        super().create(retention=retention, **kwargs)
        self._create_asset_tables()

    def delete(self):
        """Drop EnergyDB asset tables and TimeDB tables."""
        self._drop_asset_tables()
        super().delete()

    def _create_asset_tables(self):
        sql = importlib.resources.files("energydb.sql").joinpath("create_tables.sql").read_text()
        with self._pool.connection() as conn:
            conn.execute(sql)
            conn.commit()

    def _drop_asset_tables(self):
        sql = importlib.resources.files("energydb.sql").joinpath("drop_tables.sql").read_text()
        with self._pool.connection() as conn:
            conn.execute(sql)
            conn.commit()

    # ── Asset CRUD ──────────────────────────────────────────

    def save_asset(self, asset: edm.EnergyAsset) -> int:
        """Persist an EnergyAsset. Returns the asset_id.

        Uses upsert semantics — updates properties if the asset already exists.
        """
        data = serialize_asset(asset)
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO asset (asset_type, name, properties, latitude, longitude, altitude, timezone)
                VALUES (%(asset_type)s, %(name)s, %(properties)s::jsonb,
                        %(latitude)s, %(longitude)s, %(altitude)s, %(timezone)s)
                ON CONFLICT (name, asset_type)
                DO UPDATE SET properties = EXCLUDED.properties,
                             latitude = EXCLUDED.latitude,
                             longitude = EXCLUDED.longitude,
                             altitude = EXCLUDED.altitude,
                             timezone = EXCLUDED.timezone,
                             updated_at = now()
                RETURNING asset_id
                """,
                data,
            ).fetchone()
            conn.commit()
            return row[0]

    def get_asset(self, name: str, asset_type: Optional[str] = None) -> edm.EnergyAsset:
        """Load an asset from the database.

        Args:
            name: Asset name
            asset_type: Optional type filter (e.g., 'WindTurbine')
        """
        row = self._fetch_asset_row(name, asset_type)
        if row is None:
            raise ValueError(f"Asset not found: {name}")
        return reconstruct_asset(row)

    def list_assets(self, asset_type: Optional[str] = None) -> List[edm.EnergyAsset]:
        """List all assets, optionally filtered by type."""
        with self._pool.connection() as conn:
            if asset_type:
                rows = conn.execute(
                    "SELECT asset_id, asset_type, name, properties, latitude, longitude, altitude, timezone "
                    "FROM asset WHERE asset_type = %s ORDER BY name",
                    (asset_type,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT asset_id, asset_type, name, properties, latitude, longitude, altitude, timezone "
                    "FROM asset ORDER BY asset_type, name"
                ).fetchall()
        return [reconstruct_asset(self._row_to_dict(r)) for r in rows]

    def delete_asset(self, name: str, asset_type: Optional[str] = None):
        """Delete an asset and its series links (cascading)."""
        with self._pool.connection() as conn:
            if asset_type:
                conn.execute(
                    "DELETE FROM asset WHERE name = %s AND asset_type = %s",
                    (name, asset_type),
                )
            else:
                conn.execute("DELETE FROM asset WHERE name = %s", (name,))
            conn.commit()

    # ── Hierarchy CRUD ──────────────────────────────────────

    def save_portfolio(self, portfolio: edm.Portfolio):
        """Persist a full Portfolio tree (collections, assets, links).

        The entire tree is saved in a single atomic transaction.
        """
        with self._pool.connection() as conn:
            with conn.transaction():
                def on_collection(collection, parent_id):
                    data = serialize_collection(collection)
                    row = conn.execute(
                        """
                        INSERT INTO collection (collection_type, name, properties, parent_id, latitude, longitude)
                        VALUES (%(collection_type)s, %(name)s, %(properties)s::jsonb,
                                %(parent_id)s, %(latitude)s, %(longitude)s)
                        ON CONFLICT (name, collection_type)
                        DO UPDATE SET properties = EXCLUDED.properties,
                                     parent_id = EXCLUDED.parent_id,
                                     latitude = EXCLUDED.latitude,
                                     longitude = EXCLUDED.longitude
                        RETURNING collection_id
                        """,
                        {**data, "parent_id": parent_id},
                    ).fetchone()
                    return row[0]

                def on_asset(asset):
                    data = serialize_asset(asset)
                    row = conn.execute(
                        """
                        INSERT INTO asset (asset_type, name, properties, latitude, longitude, altitude, timezone)
                        VALUES (%(asset_type)s, %(name)s, %(properties)s::jsonb,
                                %(latitude)s, %(longitude)s, %(altitude)s, %(timezone)s)
                        ON CONFLICT (name, asset_type)
                        DO UPDATE SET properties = EXCLUDED.properties,
                                     latitude = EXCLUDED.latitude,
                                     longitude = EXCLUDED.longitude,
                                     altitude = EXCLUDED.altitude,
                                     timezone = EXCLUDED.timezone,
                                     updated_at = now()
                        RETURNING asset_id
                        """,
                        data,
                    ).fetchone()
                    return row[0]

                def on_link(col_id, asset_id):
                    conn.execute(
                        """
                        INSERT INTO collection_asset (collection_id, asset_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (col_id, asset_id),
                    )

                walk_tree(portfolio, on_collection, on_asset, on_link)

    def get_portfolio(self, name: str) -> edm.Portfolio:
        """Reconstruct a full Portfolio tree from the database."""
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT collection_id, collection_type, name, properties, parent_id, latitude, longitude "
                "FROM collection WHERE name = %s AND collection_type = 'Portfolio'",
                (name,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Portfolio not found: {name}")
            root = self._collection_row_to_dict(row)

            def get_assets(col_id):
                rows = conn.execute(
                    """
                    SELECT a.asset_id, a.asset_type, a.name, a.properties,
                           a.latitude, a.longitude, a.altitude, a.timezone
                    FROM asset a
                    JOIN collection_asset ca ON ca.asset_id = a.asset_id
                    WHERE ca.collection_id = %s
                    ORDER BY a.name
                    """,
                    (col_id,),
                ).fetchall()
                return [self._row_to_dict(r) for r in rows]

            def get_subcollections(parent_id):
                rows = conn.execute(
                    "SELECT collection_id, collection_type, name, properties, parent_id, latitude, longitude "
                    "FROM collection WHERE parent_id = %s ORDER BY name",
                    (parent_id,),
                ).fetchall()
                return [self._collection_row_to_dict(r) for r in rows]

            return reconstruct_tree(root, get_assets, get_subcollections, reconstruct_asset)

    def save_site(self, site: edm.Site, parent: Optional[str] = None):
        """Persist a Site and its assets.

        Args:
            site: The Site to persist
            parent: Optional parent collection name to attach to
        """
        with self._pool.connection() as conn:
            with conn.transaction():
                parent_id = None
                if parent:
                    row = conn.execute(
                        "SELECT collection_id FROM collection WHERE name = %s",
                        (parent,),
                    ).fetchone()
                    if row:
                        parent_id = row[0]

                data = serialize_collection(site)
                row = conn.execute(
                    """
                    INSERT INTO collection (collection_type, name, properties, parent_id, latitude, longitude)
                    VALUES (%(collection_type)s, %(name)s, %(properties)s::jsonb,
                            %(parent_id)s, %(latitude)s, %(longitude)s)
                    ON CONFLICT (name, collection_type)
                    DO UPDATE SET properties = EXCLUDED.properties,
                                 parent_id = EXCLUDED.parent_id,
                                 latitude = EXCLUDED.latitude,
                                 longitude = EXCLUDED.longitude
                    RETURNING collection_id
                    """,
                    {**data, "parent_id": parent_id},
                ).fetchone()
                col_id = row[0]

                for asset in getattr(site, "assets", []):
                    asset_data = serialize_asset(asset)
                    asset_row = conn.execute(
                        """
                        INSERT INTO asset (asset_type, name, properties, latitude, longitude, altitude, timezone)
                        VALUES (%(asset_type)s, %(name)s, %(properties)s::jsonb,
                                %(latitude)s, %(longitude)s, %(altitude)s, %(timezone)s)
                        ON CONFLICT (name, asset_type)
                        DO UPDATE SET properties = EXCLUDED.properties,
                                     latitude = EXCLUDED.latitude,
                                     longitude = EXCLUDED.longitude,
                                     altitude = EXCLUDED.altitude,
                                     timezone = EXCLUDED.timezone,
                                     updated_at = now()
                        RETURNING asset_id
                        """,
                        asset_data,
                    ).fetchone()
                    conn.execute(
                        "INSERT INTO collection_asset (collection_id, asset_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (col_id, asset_row[0]),
                    )

    def get_site(self, name: str) -> edm.Site:
        """Reconstruct a Site with its assets from the database."""
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT collection_id, collection_type, name, properties, parent_id, latitude, longitude "
                "FROM collection WHERE name = %s AND collection_type = 'Site'",
                (name,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Site not found: {name}")
            col = self._collection_row_to_dict(row)

            asset_rows = conn.execute(
                """
                SELECT a.asset_id, a.asset_type, a.name, a.properties,
                       a.latitude, a.longitude, a.altitude, a.timezone
                FROM asset a
                JOIN collection_asset ca ON ca.asset_id = a.asset_id
                WHERE ca.collection_id = %s
                ORDER BY a.name
                """,
                (col["collection_id"],),
            ).fetchall()

        assets = [reconstruct_asset(self._row_to_dict(r)) for r in asset_rows]
        kwargs = {"name": col["name"], "assets": assets}
        if col.get("latitude") is not None:
            kwargs["latitude"] = col["latitude"]
        if col.get("longitude") is not None:
            kwargs["longitude"] = col["longitude"]
        return edm.Site(**kwargs)

    # ── Series on assets ────────────────────────────────────

    def add_series_to_asset(
        self,
        asset_name: str,
        series_name: str,
        role: Optional[str] = None,
        asset_type: Optional[str] = None,
        **series_kwargs,
    ) -> int:
        """Create a TimeDB series and link it to an asset.

        Args:
            asset_name: Name of the asset to link to
            series_name: Name for the TimeDB series
            role: Role label (e.g., 'active_power', 'wind_speed')
            asset_type: Optional asset type filter
            **series_kwargs: Passed to TimeDB's create_series() (unit, labels, etc.)

        Returns:
            The series_id of the created/existing series
        """
        row = self._fetch_asset_row(asset_name, asset_type)
        if row is None:
            raise ValueError(f"Asset not found: {asset_name}")
        asset_id = row["asset_id"]

        # Create the series in TimeDB
        series_id = self.create_series(series_name, **series_kwargs)

        # Link it to the asset
        with self._pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO asset_series (asset_id, series_id, role)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (asset_id, series_id, role or series_name),
            )
            conn.commit()

        return series_id

    def get_asset_series(self, asset_name: str, role: Optional[str] = None, asset_type: Optional[str] = None):
        """Get TimeDB SeriesCollection(s) for an asset's linked series.

        Args:
            asset_name: Name of the asset
            role: Optional role filter (e.g., 'active_power')
            asset_type: Optional asset type filter

        Returns:
            A SeriesCollection if role is specified (single series),
            or a list of (role, SeriesCollection) tuples if role is None.
        """
        row = self._fetch_asset_row(asset_name, asset_type)
        if row is None:
            raise ValueError(f"Asset not found: {asset_name}")
        asset_id = row["asset_id"]

        with self._pool.connection() as conn:
            if role:
                link = conn.execute(
                    "SELECT series_id, role FROM asset_series WHERE asset_id = %s AND role = %s",
                    (asset_id, role),
                ).fetchone()
                if link is None:
                    raise ValueError(f"No series with role '{role}' on asset '{asset_name}'")
                return self.get_series(series_id=link[0])
            else:
                links = conn.execute(
                    "SELECT series_id, role FROM asset_series WHERE asset_id = %s ORDER BY role",
                    (asset_id,),
                ).fetchall()
                return [(link[1], self.get_series(series_id=link[0])) for link in links]

    # ── Cross-domain queries ────────────────────────────────

    def query_assets(
        self,
        portfolio: Optional[str] = None,
        site: Optional[str] = None,
        asset_type: Optional[str] = None,
        **property_filters,
    ) -> List[edm.EnergyAsset]:
        """Query assets across the hierarchy with filters.

        Args:
            portfolio: Filter by portfolio name
            site: Filter by site name
            asset_type: Filter by asset type (e.g., 'WindTurbine')
            **property_filters: Filter by JSONB properties (e.g., capacity=3.5)
        """
        conditions = []
        params: List[Any] = []
        joins = []

        if portfolio or site:
            joins.append("JOIN collection_asset ca ON ca.asset_id = a.asset_id")
            joins.append("JOIN collection c ON c.collection_id = ca.collection_id")
            if portfolio:
                # Find all collections under this portfolio (recursive)
                conditions.append("""
                    ca.collection_id IN (
                        WITH RECURSIVE tree AS (
                            SELECT collection_id FROM collection WHERE name = %s AND collection_type = 'Portfolio'
                            UNION ALL
                            SELECT c2.collection_id FROM collection c2 JOIN tree t ON c2.parent_id = t.collection_id
                        )
                        SELECT collection_id FROM tree
                    )
                """)
                params.append(portfolio)
            if site:
                conditions.append("c.name = %s AND c.collection_type = 'Site'")
                params.append(site)

        if asset_type:
            conditions.append("a.asset_type = %s")
            params.append(asset_type)

        for key, value in property_filters.items():
            conditions.append(f"a.properties->>'{key}' = %s")
            params.append(str(value))

        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        join_clause = " ".join(joins)

        query = f"""
            SELECT DISTINCT a.asset_id, a.asset_type, a.name, a.properties,
                   a.latitude, a.longitude, a.altitude, a.timezone
            FROM asset a
            {join_clause}
            WHERE {where_clause}
            ORDER BY a.asset_type, a.name
        """

        with self._pool.connection() as conn:
            rows = conn.execute(query, params).fetchall()
        return [reconstruct_asset(self._row_to_dict(r)) for r in rows]

    # ── Internal helpers ────────────────────────────────────

    def _fetch_asset_row(self, name: str, asset_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        with self._pool.connection() as conn:
            if asset_type:
                row = conn.execute(
                    "SELECT asset_id, asset_type, name, properties, latitude, longitude, altitude, timezone "
                    "FROM asset WHERE name = %s AND asset_type = %s",
                    (name, asset_type),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT asset_id, asset_type, name, properties, latitude, longitude, altitude, timezone "
                    "FROM asset WHERE name = %s",
                    (name,),
                ).fetchone()
        if row is None:
            return None
        return self._row_to_dict(row)

    @staticmethod
    def _row_to_dict(row) -> Dict[str, Any]:
        """Convert a psycopg row tuple to a dict."""
        return {
            "asset_id": row[0],
            "asset_type": row[1],
            "name": row[2],
            "properties": row[3],
            "latitude": row[4],
            "longitude": row[5],
            "altitude": row[6],
            "timezone": row[7],
        }

    @staticmethod
    def _collection_row_to_dict(row) -> Dict[str, Any]:
        """Convert a collection row tuple to a dict."""
        return {
            "collection_id": row[0],
            "collection_type": row[1],
            "name": row[2],
            "properties": row[3],
            "parent_id": row[4],
            "latitude": row[5],
            "longitude": row[6],
        }
