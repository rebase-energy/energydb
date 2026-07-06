"""ClickHouse engine-table scaffolding for the ``concurrent`` read.

Resolves the subtree's ``series_id`` set server-side in ClickHouse via a ``PostgreSQL()`` table
engine over the ``series_meta`` view, instead of a client-side PG round-trip. Holds the engine
table's identity + DDL; ``Client.setup_ch_fast_read()`` provisions it and the read path references
:data:`CH_ENGINE_TABLE`.
"""

from __future__ import annotations

import os
from urllib.parse import unquote, urlparse

# Overridable so a deployment can point at its own table name.
CH_ENGINE_TABLE = os.environ.get("ENERGYDB_CH_ENGINE_TABLE", "energydb_series_meta_pg")

# Production: set ENERGYDB_CH_PG_COLLECTION to a ClickHouse named collection holding the PG creds
# (kept out of the engine-table DDL). Unset -> creds are inlined from the client DSN, which works
# with an unprivileged CH role (no CREATE/GRANT NAMED COLLECTION needed) but embeds the password
# in `SHOW CREATE TABLE`.
CH_NAMED_COLLECTION = os.environ.get("ENERGYDB_CH_PG_COLLECTION")

_ENGINE_COLS = (
    "(series_id Int64, path String, data_type String, name String, "
    "canonical_unit String, retention String, timeseries_type String)"
)


def engine_table_ddl(pg_dsn: str, pg_schema: str) -> str:
    """DDL for the ClickHouse ``PostgreSQL()`` engine table over the ``series_meta`` view.

    Uses the :data:`CH_NAMED_COLLECTION` named collection if configured (production shape: the PG
    password stays out of the DDL), else inlines the creds from ``pg_dsn`` (works with an
    unprivileged CH role). Must NOT be wrapped in a CH VIEW -- that kills predicate pushdown of
    ``path LIKE 'root/%'`` (ClickHouse #86178).
    """
    if CH_NAMED_COLLECTION:
        source = f"{CH_NAMED_COLLECTION}, table = 'series_meta'"
    else:
        u = urlparse(pg_dsn)
        host = f"{u.hostname}:{u.port or 5432}"
        db = (u.path or "/postgres").lstrip("/") or "postgres"
        user = unquote(u.username or "")
        pw = unquote(u.password or "")
        source = f"'{host}', '{db}', 'series_meta', '{user}', '{pw}', '{pg_schema}'"
    return f"CREATE TABLE IF NOT EXISTS {CH_ENGINE_TABLE} {_ENGINE_COLS} ENGINE = PostgreSQL({source})"
