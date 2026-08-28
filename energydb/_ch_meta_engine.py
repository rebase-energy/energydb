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

from psycopg.conninfo import conninfo_to_dict

from .errors import ConfigurationError


def _engine_table_name(base: str, schema: str) -> str:
    """ClickHouse engine-table name for a PostgreSQL ``schema``.

    The engine table's PG-schema target is fixed at CREATE time, so the name must
    encode the schema: a client with ``ENERGYDB_SCHEMA=X`` then only ever references
    a table provisioned for ``X``, which makes cross-schema mis-targeting impossible
    by construction. ``public`` keeps the bare (historical) name so existing default
    deployments are undisturbed; a named schema gets a ``__<schema>`` suffix.
    """
    return base if schema == "public" else f"{base}__{schema}"


# Overridable base so a deployment can point at its own table name.
_ENGINE_TABLE_BASE = os.environ.get("ENERGYDB_CH_ENGINE_TABLE", "energydb_series_meta_pg")
_ENGINE_SCHEMA = os.environ.get("ENERGYDB_SCHEMA", "public") or "public"
CH_ENGINE_TABLE = _engine_table_name(_ENGINE_TABLE_BASE, _ENGINE_SCHEMA)

DROP_ENGINE_TABLE = f"DROP TABLE IF EXISTS {CH_ENGINE_TABLE}"
# A bare-named table left by an older named-schema deployment would mislead a
# public client, which expects the bare name to mean public. None when this
# already is the bare name.
DROP_LEGACY_ENGINE_TABLE = (
    None if CH_ENGINE_TABLE == _ENGINE_TABLE_BASE else f"DROP TABLE IF EXISTS {_ENGINE_TABLE_BASE}"
)

# LEFT-JOIN-sourced columns are Nullable: path and node_uuid are NULL for
# edge-owned series, the edge columns for node-owned ones.
_ENGINE_COLS = (
    "(series_id Int64, path Nullable(String), data_type String, name String, "
    "canonical_unit String, retention String, timeseries_type String, "
    "node_uuid Nullable(String), edge_uuid Nullable(String), edge_type Nullable(String), "
    "from_path Nullable(String), to_path Nullable(String), edge_name Nullable(String))"
)


def series_meta_view_ddl(qualifier: str) -> tuple[str, str]:
    """``(CREATE, DROP)`` DDL for the PG ``series_meta`` view.

    ``qualifier`` is the schema prefix including the trailing dot (``""`` for
    ``public``). Covers node-owned AND edge-owned series (LEFT JOINs; the owner
    columns of the other kind are NULL). The first seven columns keep their
    historical order: PG's ``CREATE OR REPLACE VIEW`` only allows appending
    columns. Consumed by ``models.py`` for its metadata DDL events (the view is
    created by ``Client.create()``; Alembic autogenerate does not track views)
    and by ``Client.setup_ch_meta_engine()``.

    ``edge_name`` is appended last (0.11.0) for exactly that append-only
    reason; it is the edge's own name, which tells parallel edges of a
    multigraph apart. Existing deployments pick it up by re-running
    ``setup_ch_meta_engine()``.
    """
    view = f"{qualifier}series_meta"
    create = (
        f"CREATE OR REPLACE VIEW {view} AS "
        "SELECT s.series_id, n.path, s.data_type, s.name, s.canonical_unit, s.retention, s.timeseries_type, "
        "s.node_uuid::text AS node_uuid, s.edge_uuid::text AS edge_uuid, "
        "e.edge_type AS edge_type, fn.path AS from_path, tn.path AS to_path, "
        "e.name AS edge_name "
        f"FROM {qualifier}series s "
        f"LEFT JOIN {qualifier}node n ON n.uuid = s.node_uuid "
        f"LEFT JOIN {qualifier}edge e ON e.uuid = s.edge_uuid "
        f"LEFT JOIN {qualifier}node fn ON fn.uuid = e.from_node_uuid "
        f"LEFT JOIN {qualifier}node tn ON tn.uuid = e.to_node_uuid"
    )
    return create, f"DROP VIEW IF EXISTS {view}"


def inlines_pg_password(pg_dsn: str) -> bool:
    """True when :func:`engine_table_ddl` would embed a real password in the DDL.

    Lives next to the function that does the inlining so the two cannot drift.
    ``False`` when a named collection is configured (the password never reaches
    the DDL) *and* when the DSN carries no password at all: a trust-auth or
    passwordless dev DSN inlines nothing worth warning about, and a warning that
    fires on every local ``create()`` is a warning nobody reads.
    """
    if os.environ.get("ENERGYDB_CH_PG_COLLECTION"):
        return False
    return bool(conninfo_to_dict(pg_dsn).get("password"))


def engine_pg_host(pg_dsn: str) -> str | None:
    """``host:port`` ClickHouse can dial for ``pg_dsn``, or ``None`` if there is none.

    ``ENERGYDB_CH_PG_HOST`` always wins, same as in :func:`engine_table_ddl`.
    Otherwise the DSN's host is used, but only if it is a TCP host: missing, or a
    Unix-socket directory (starting with ``/``, e.g. from
    ``postgresql:///db?host=/run/postgresql``), yields ``None`` since ClickHouse
    has no socket to dial. The DSN's ``host`` query parameter is honored, so
    ``postgresql:///db?host=tcphost`` resolves to ``tcphost:5432``.
    """
    override = os.environ.get("ENERGYDB_CH_PG_HOST")
    if override:
        return override
    info = conninfo_to_dict(pg_dsn)
    host = str(info.get("host") or "")
    if not host or host.startswith("/"):
        return None
    return f"{host}:{info.get('port') or 5432}"


def engine_table_ddl(pg_dsn: str, pg_schema: str) -> str:
    """DDL for the ClickHouse ``PostgreSQL()`` engine table over ``series_meta``.

    Connection-source resolution, in order:

    * ``ENERGYDB_CH_PG_COLLECTION`` set → a ClickHouse named collection holds
      the PG connection (production shape where the CH role allows it: the PG
      password stays out of the DDL / ``SHOW CREATE TABLE``).
    * else creds are inlined from ``pg_dsn``: which means the PostgreSQL
      password is readable via ``SHOW CREATE TABLE`` to any ClickHouse user with
      read access. Fine for dev/compose, not for production; the caller warns
      about it at provisioning time (see :func:`inlines_pg_password`).
      ``ENERGYDB_CH_PG_HOST``
      (``host:port``) overrides the DSN's host for **ClickHouse's network
      vantage**: the DSN addresses PG from the app (e.g. ``127.0.0.1:5433``
      for a local docker port-map), which is not necessarily resolvable from
      ClickHouse itself (which needs e.g. ``postgres:5432`` on the compose
      network). Database, user, and password always come from the DSN, only
      the network path differs by vantage.

    The engine table must NOT be wrapped in a CH VIEW, that kills predicate
    pushdown of ``path LIKE 'root/%'`` (ClickHouse #86178).

    Raises :class:`~energydb.errors.ConfigurationError` if ``pg_dsn`` has no TCP host and is not
    overridden by ``ENERGYDB_CH_PG_HOST`` (see :func:`engine_pg_host`); callers
    that provision from a DSN should check :func:`engine_pg_host` first so this
    never fires as a surprise.
    """
    named_collection = os.environ.get("ENERGYDB_CH_PG_COLLECTION")
    if named_collection:
        # Without an explicit schema the engine table inherits whatever the named
        # collection encodes, resolving against the wrong series_meta.
        source = f"{named_collection}, table = 'series_meta', schema = '{pg_schema}'"
    else:
        host = engine_pg_host(pg_dsn)
        if host is None:
            raise ConfigurationError(
                "the PostgreSQL DSN has no TCP host for ClickHouse to dial "
                "(socket-only or missing); set ENERGYDB_CH_PG_HOST to override"
            )
        info = conninfo_to_dict(pg_dsn)
        db = info.get("dbname") or "postgres"
        user = info.get("user") or ""
        pw = info.get("password") or ""
        source = f"'{host}', '{db}', 'series_meta', '{user}', '{pw}', '{pg_schema}'"
    return f"CREATE TABLE IF NOT EXISTS {CH_ENGINE_TABLE} {_ENGINE_COLS} ENGINE = PostgreSQL({source})"
