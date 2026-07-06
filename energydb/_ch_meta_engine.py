"""The ClickHouse ↔ PostgreSQL metadata bridge for the ``concurrent`` read.

Both halves of the bridge live here: the PG ``series_meta`` view (a read-only
projection of node-owned series) and the ClickHouse ``PostgreSQL()`` engine
table over it, through which ClickHouse resolves a subtree's ``series_id`` set
server-side instead of via a client-side PG round-trip. ``Client.create()``
provisions both best-effort; ``Client.setup_ch_meta_engine()`` is the explicit
(raising) path. The read path references :data:`CH_ENGINE_TABLE`.
"""

from __future__ import annotations

import os
from urllib.parse import unquote, urlparse

# Overridable so a deployment can point at its own table name.
CH_ENGINE_TABLE = os.environ.get("ENERGYDB_CH_ENGINE_TABLE", "energydb_series_meta_pg")

DROP_ENGINE_TABLE = f"DROP TABLE IF EXISTS {CH_ENGINE_TABLE}"

_ENGINE_COLS = (
    "(series_id Int64, path String, data_type String, name String, "
    "canonical_unit String, retention String, timeseries_type String)"
)


def series_meta_view_ddl(qualifier: str) -> tuple[str, str]:
    """``(CREATE, DROP)`` DDL for the PG ``series_meta`` view.

    ``qualifier`` is the schema prefix including the trailing dot (``""`` for
    ``public``). Node-owned series only — the inner join drops edge-owned rows.
    Consumed by ``models.py`` for its metadata DDL events (the view is created
    by ``Client.create()``; Alembic autogenerate does not track views) and by
    ``Client.setup_ch_meta_engine()``.
    """
    view = f"{qualifier}series_meta"
    create = (
        f"CREATE OR REPLACE VIEW {view} AS "
        "SELECT s.series_id, n.path, s.data_type, s.name, s.canonical_unit, s.retention, s.timeseries_type "
        f"FROM {qualifier}node n JOIN {qualifier}series s ON s.node_uuid = n.uuid"
    )
    return create, f"DROP VIEW IF EXISTS {view}"


def engine_table_ddl(pg_dsn: str, pg_schema: str) -> str:
    """DDL for the ClickHouse ``PostgreSQL()`` engine table over ``series_meta``.

    Connection-source resolution, in order:

    * ``ENERGYDB_CH_PG_COLLECTION`` set → a ClickHouse named collection holds
      the PG connection (production shape where the CH role allows it: the PG
      password stays out of the DDL / ``SHOW CREATE TABLE``).
    * else creds are inlined from ``pg_dsn``. ``ENERGYDB_CH_PG_HOST``
      (``host:port``) overrides the DSN's host for **ClickHouse's network
      vantage**: the DSN addresses PG from the app (e.g. ``127.0.0.1:5433``
      for a local docker port-map), which is not necessarily resolvable from
      ClickHouse itself (which needs e.g. ``postgres:5432`` on the compose
      network). Database, user, and password always come from the DSN — only
      the network path differs by vantage.

    The engine table must NOT be wrapped in a CH VIEW — that kills predicate
    pushdown of ``path LIKE 'root/%'`` (ClickHouse #86178).
    """
    named_collection = os.environ.get("ENERGYDB_CH_PG_COLLECTION")
    if named_collection:
        source = f"{named_collection}, table = 'series_meta'"
    else:
        u = urlparse(pg_dsn)
        host = os.environ.get("ENERGYDB_CH_PG_HOST") or f"{u.hostname}:{u.port or 5432}"
        db = (u.path or "/postgres").lstrip("/") or "postgres"
        user = unquote(u.username or "")
        pw = unquote(u.password or "")
        source = f"'{host}', '{db}', 'series_meta', '{user}', '{pw}', '{pg_schema}'"
    return f"CREATE TABLE IF NOT EXISTS {CH_ENGINE_TABLE} {_ENGINE_COLS} ENGINE = PostgreSQL({source})"
