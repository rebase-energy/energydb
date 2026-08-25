"""SQLAlchemy declarative models for EnergyDB PostgreSQL tables.

These models are the single schema source of truth and Alembic-friendly. They
live in the schema named by ``ENERGYDB_SCHEMA`` (default ``public``). The
partial unique index on root names is declared in ``Node.__table_args__``;
series immutability is enforced in Python (see
:func:`energydb.series.register_series`), not by a DB trigger.

UUID is the primary identity for every row in ``node`` and ``edge``.
``parent_uuid`` and ``edge.from_node_uuid`` / ``to_node_uuid`` are FKs by
UUID: the application Reference holds a UUID and writes it directly into the
FK column, with no translation step. ``series.series_id`` stays BIGINT (it's
timedb-internal, not an EDM identity).

Retention tier names are owned by :data:`timedb.RETENTION_TIERS`; energydb
does **not** encode them in a CHECK constraint, so adding a tier in timedb
does not require an energydb migration.
"""

import os

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase

from energydb._ch_meta_engine import series_meta_view_ddl

# None for "public" so the ORM tables carry no explicit schema, which keeps
# Alembic autogenerate from churning on a redundant schema="public" qualifier.
_SCHEMA_ENV = os.environ.get("ENERGYDB_SCHEMA", "public")
SCHEMA: str | None = None if _SCHEMA_ENV == "public" else _SCHEMA_ENV

# Every raw-SQL relation reference is prefixed with this, so names resolve from
# the SQL text and energydb never touches the connection's search path. That is
# what keeps it correct behind a transaction-mode pooler, where per-connection
# session state is not the client's to rely on. Empty for the default schema.
# SCHEMA goes in unquoted, so lowercase simple identifiers are a constraint of
# ENERGYDB_SCHEMA.
SQL_SCHEMA_PREFIX: str = f"{SCHEMA}." if SCHEMA else ""


def _fk(target: str) -> str:
    """Schema-qualified foreign-key target string for the ORM models."""
    return f"{SCHEMA}.{target}" if SCHEMA else target


class Base(DeclarativeBase):
    pass


# The tenancy partition key. A namespaced client view binds the
# transaction-local energydb.namespace GUC and this server default reads it
# back, so write paths never mention the column. Root clients land in 'default'.
# The same GUC doubles as the row filter if the host application enables RLS.
NAMESPACE_DEFAULT = sa.text("COALESCE(NULLIF(current_setting('energydb.namespace', true), ''), 'default')")


class Node(Base):
    __tablename__ = "node"

    uuid = sa.Column(UUID(as_uuid=True), primary_key=True)
    namespace = sa.Column(sa.Text, nullable=False, server_default=NAMESPACE_DEFAULT)
    node_type = sa.Column(sa.Text, nullable=False)
    name = sa.Column(sa.Text, nullable=False)
    parent_uuid = sa.Column(UUID(as_uuid=True), nullable=True)
    path = sa.Column(sa.Text, nullable=False)
    data = sa.Column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))
    created_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())

    __table_args__ = (
        sa.UniqueConstraint("uuid", "namespace", name="node_uuid_namespace_uniq"),
        # A child lives in its parent's namespace, structurally.
        sa.ForeignKeyConstraint(
            ["parent_uuid", "namespace"],
            [_fk("node.uuid"), _fk("node.namespace")],
            ondelete="CASCADE",
            name="node_parent_namespace_fkey",
        ),
        sa.UniqueConstraint("parent_uuid", "name", name="node_child_uniq"),
        sa.UniqueConstraint("namespace", "path", name="node_path_uniq"),
        sa.Index(
            "ix_node_root_uniq",
            "namespace",
            "name",
            unique=True,
            postgresql_where=sa.text("parent_uuid IS NULL"),
        ),
        sa.Index("ix_node_parent_uuid", "parent_uuid"),
        sa.Index("ix_node_data_gin", "data", postgresql_using="gin"),
        sa.Index(
            "ix_node_path_prefix",
            "namespace",
            "path",
            postgresql_ops={"path": "text_pattern_ops"},
        ),
        sa.Index("ix_node_namespace_type", "namespace", "node_type"),
        sa.CheckConstraint("name !~ '/' AND length(name) > 0", name="node_name_valid"),
        sa.CheckConstraint("length(path) > 0", name="node_path_nonempty"),
        {"schema": SCHEMA},
    )


class Edge(Base):
    __tablename__ = "edge"

    uuid = sa.Column(UUID(as_uuid=True), primary_key=True)
    namespace = sa.Column(sa.Text, nullable=False, server_default=NAMESPACE_DEFAULT)
    edge_type = sa.Column(sa.Text, nullable=False)
    name = sa.Column(sa.Text, nullable=True)
    from_node_uuid = sa.Column(UUID(as_uuid=True), nullable=False)
    to_node_uuid = sa.Column(UUID(as_uuid=True), nullable=False)
    data = sa.Column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))
    created_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())

    __table_args__ = (
        sa.UniqueConstraint("uuid", "namespace", name="edge_uuid_namespace_uniq"),
        # A cross-namespace edge is unrepresentable, not merely checked.
        sa.ForeignKeyConstraint(
            ["from_node_uuid", "namespace"],
            [_fk("node.uuid"), _fk("node.namespace")],
            ondelete="CASCADE",
            name="edge_from_node_namespace_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["to_node_uuid", "namespace"],
            [_fk("node.uuid"), _fk("node.namespace")],
            ondelete="CASCADE",
            name="edge_to_node_namespace_fkey",
        ),
        # Parallel edges share an endpoint pair and type and are told apart by
        # name. NULLS NOT DISTINCT gives at most one unnamed edge per
        # (type, from, to) and unlimited named ones; standard NULL semantics
        # would accept unlimited unnamed duplicates. Requires PostgreSQL 15+.
        # namespace needs no seat in the key: node uuids are globally unique and
        # the composite endpoint FKs already pin the edge's namespace.
        sa.UniqueConstraint(
            "edge_type",
            "from_node_uuid",
            "to_node_uuid",
            "name",
            name="edge_uniq",
            postgresql_nulls_not_distinct=True,
        ),
        # The CHECK runs only when name IS NOT NULL, and guards a key column of
        # edge_uniq.
        sa.CheckConstraint(
            "name IS NULL OR (name !~ '/' AND length(name) > 0)",
            name="edge_name_valid",
        ),
        {"schema": SCHEMA},
    )


class Series(Base):
    """Polymorphic series owned by either a node or an edge (exactly one).

    ``retention``, ``canonical_unit``, and the owner columns are immutable
    after insert (enforced in Python by ``register_series``). ``timeseries_type``
    is mutable: a series can legitimately transition from flat to overlapping
    if the producer changes behavior.

    ``series_id`` stays BIGINT: it's the timedb-internal handle and never
    leaves the energydb / timedb pair.
    """

    __tablename__ = "series"

    series_id = sa.Column(sa.BigInteger, sa.Identity(always=False), primary_key=True)
    namespace = sa.Column(sa.Text, nullable=False, server_default=NAMESPACE_DEFAULT)
    node_uuid = sa.Column(UUID(as_uuid=True), nullable=True)
    edge_uuid = sa.Column(UUID(as_uuid=True), nullable=True)
    data_type = sa.Column(sa.Text, nullable=False)
    name = sa.Column(sa.Text, nullable=False)
    canonical_unit = sa.Column(sa.Text, nullable=False)
    timeseries_type = sa.Column(sa.Text, nullable=False)
    retention = sa.Column(sa.Text, nullable=False)
    description = sa.Column(sa.Text, nullable=True)
    inserted_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())

    __table_args__ = (
        sa.CheckConstraint("(node_uuid IS NULL) <> (edge_uuid IS NULL)", name="series_owner_xor"),
        sa.ForeignKeyConstraint(
            ["node_uuid", "namespace"],
            [_fk("node.uuid"), _fk("node.namespace")],
            ondelete="CASCADE",
            name="series_node_namespace_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["edge_uuid", "namespace"],
            [_fk("edge.uuid"), _fk("edge.namespace")],
            ondelete="CASCADE",
            name="series_edge_namespace_fkey",
        ),
        sa.UniqueConstraint("node_uuid", "data_type", "name", name="series_node_uniq"),
        sa.UniqueConstraint("edge_uuid", "data_type", "name", name="series_edge_uniq"),
        sa.CheckConstraint("timeseries_type IN ('FLAT','OVERLAPPING')", name="valid_timeseries_type"),
        sa.CheckConstraint("name !~ '/' AND length(name) > 0", name="series_name_valid"),
        sa.Index("ix_series_node_uuid", "node_uuid", postgresql_where=sa.text("node_uuid IS NOT NULL")),
        sa.Index("ix_series_edge_uuid", "edge_uuid", postgresql_where=sa.text("edge_uuid IS NOT NULL")),
        {"schema": SCHEMA},
    )


class Run(Base):
    """Run metadata. ``run_id`` is client-generated (uuid7 → UInt64 truncate),
    so writes don't wait on a PG allocation round-trip.
    """

    __tablename__ = "runs"

    run_id = sa.Column(sa.BigInteger, primary_key=True)
    workflow_id = sa.Column(sa.Text, nullable=True)
    model_name = sa.Column(sa.Text, nullable=True)
    run_start_time = sa.Column(sa.DateTime(timezone=True), nullable=True)
    run_finish_time = sa.Column(sa.DateTime(timezone=True), nullable=True)
    run_params = sa.Column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))
    inserted_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())

    __table_args__ = (
        sa.Index("ix_runs_workflow", "workflow_id", "inserted_at", postgresql_using="btree"),
        {"schema": SCHEMA},
    )


# A no-op for the default public schema, which always exists and whose CREATE
# privilege would be a needless ask.
#
# Series immutability is enforced in Python by register_series rather than a DB
# trigger, so the schema stays fully Alembic-autogeneratable.
if SCHEMA is not None:
    event.listen(
        Base.metadata,
        "before_create",
        sa.DDL(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"),
    )

# The read-only projection CH's PostgreSQL table engine selects from. Its DDL
# lives with the engine-table counterpart in _ch_meta_engine and is recreated by
# Client.setup_ch_meta_engine(), since Alembic autogenerate does not track views.
CREATE_SERIES_META_VIEW, DROP_SERIES_META_VIEW = series_meta_view_ddl(SQL_SCHEMA_PREFIX)

event.listen(Base.metadata, "after_create", sa.DDL(CREATE_SERIES_META_VIEW))
event.listen(Base.metadata, "before_drop", sa.DDL(DROP_SERIES_META_VIEW))
