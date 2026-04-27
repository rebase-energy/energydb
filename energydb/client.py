"""EnergyDBClient — owns the psycopg pool and constructs TimeDBClient."""

from __future__ import annotations

import os
from importlib import resources
from typing import Any

from psycopg_pool import ConnectionPool
from sqlalchemy import create_engine
from timedb import TimeDBClient

from energydb import runs as runs_mod
from energydb.models import ENERGYDB_TABLES, Base
from energydb.scope import EdgeScope, NodeScope

_SEARCH_PATH = "SET search_path TO energydb, public"


def _read_pg_sql() -> str:
    return resources.files("energydb").joinpath("sql", "pg_create_tables.sql").read_text(encoding="utf-8")


class EnergyDBClient:
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

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def create(self) -> None:
        """Create PG schema + CH tables."""
        with self._pool.connection() as conn:
            conn.execute(_read_pg_sql())
            conn.commit()

        engine = create_engine(self._sqlalchemy_url())
        try:
            Base.metadata.create_all(engine, tables=ENERGYDB_TABLES, checkfirst=True)
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
    # Fluent entry
    # ------------------------------------------------------------------

    def node(self, name: str | None = None, *, id: int | None = None) -> NodeScope:
        if id is not None:
            return NodeScope(self._pool, self.td, node_id=id)
        if name is not None:
            return NodeScope(self._pool, self.td, name_chain=[name])
        return NodeScope(self._pool, self.td)

    def edge(self, name: str | None = None, *, id: int | None = None) -> EdgeScope:
        if id is not None:
            return EdgeScope(self._pool, self.td, edge_id=id)
        if name is not None:
            return EdgeScope(self._pool, self.td, name=name)
        raise ValueError("Must provide name or id")

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    def read_runs_for_series(self, *, series_id: int) -> list[dict[str, Any]]:
        """Return runs that wrote data for a given series_id, latest first.

        Two-step: ClickHouse ``run_series`` mapping gives the run_ids, then PG
        hydrates the metadata. Replaces the old ``td.read_runs`` FINAL-join.
        """
        run_ids = self.td.read_run_series(series_id=series_id)
        if not run_ids:
            return []
        with self._pool.connection() as conn:
            return runs_mod.get_runs(conn, run_ids)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _sqlalchemy_url(self) -> str:
        conninfo = self._pool.conninfo
        if "://" in conninfo:
            return f"postgresql+psycopg://{conninfo.split('://', 1)[-1]}"
        return conninfo
