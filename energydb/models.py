"""SQLAlchemy declarative models for EnergyDB PostgreSQL tables.

These models are the single schema source of truth — Alembic-friendly. They
live in the schema named by ``ENERGYDB_SCHEMA`` (default ``public``). The
partial unique index on root names is declared in ``Node.__table_args__``;
series immutability is enforced in Python (see
:func:`energydb.series.register_series`), not by a DB trigger.

UUID is the primary identity for every row in ``node`` and ``edge``.
``parent_uuid`` and ``edge.from_node_uuid`` / ``to_node_uuid`` are FKs by
UUID — the application Reference holds a UUID and writes it directly into
the FK column, no translation step. ``series.series_id`` stays BIGINT (it's
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

# Schema is configurable at import time via ``ENERGYDB_SCHEMA`` (default
# ``"public"``). The default co-locates EnergyDB's tables with a host
# application's tables in ``public``; a named schema isolates them. ``SCHEMA``
# is ``None`` for ``"public"`` so the ORM tables carry no explicit schema —
# identical to unqualified host tables, which keeps Alembic autogenerate from
# churning on a redundant ``schema="public"`` qualifier. Raw SQL relies on the
# session ``search_path`` (set per connection in ``Client``).
_SCHEMA_ENV = os.environ.get("ENERGYDB_SCHEMA", "public")
SCHEMA: str | None = None if _SCHEMA_ENV == "public" else _SCHEMA_ENV


def _fk(target: str) -> str:
    """Schema-qualified foreign-key target string for the ORM models."""
    return f"{SCHEMA}.{target}" if SCHEMA else target


class Base(DeclarativeBase):
    pass


class Node(Base):
    __tablename__ = "node"

    uuid = sa.Column(UUID(as_uuid=True), primary_key=True)
    node_type = sa.Column(sa.Text, nullable=False)
    name = sa.Column(sa.Text, nullable=False)
    parent_uuid = sa.Column(
        UUID(as_uuid=True),
        sa.ForeignKey(_fk("node.uuid"), ondelete="CASCADE"),
        nullable=True,
    )
    path = sa.Column(sa.Text, nullable=False)
    data = sa.Column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))
    created_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())

    __table_args__ = (
        sa.UniqueConstraint("parent_uuid", "name", name="node_child_uniq"),
        sa.UniqueConstraint("path", name="node_path_uniq"),
        sa.Index(
            "ix_node_root_uniq",
            "name",
            unique=True,
            postgresql_where=sa.text("parent_uuid IS NULL"),
        ),
        sa.Index("ix_node_parent_uuid", "parent_uuid"),
        sa.Index("ix_node_data_gin", "data", postgresql_using="gin"),
        sa.Index(
            "ix_node_path_prefix",
            "path",
            postgresql_ops={"path": "text_pattern_ops"},
        ),
        sa.CheckConstraint("name !~ '/' AND length(name) > 0", name="node_name_valid"),
        sa.CheckConstraint("length(path) > 0", name="node_path_nonempty"),
        {"schema": SCHEMA},
    )


class Edge(Base):
    __tablename__ = "edge"

    uuid = sa.Column(UUID(as_uuid=True), primary_key=True)
    edge_type = sa.Column(sa.Text, nullable=False)
    name = sa.Column(sa.Text, nullable=True)
    from_node_uuid = sa.Column(
        UUID(as_uuid=True),
        sa.ForeignKey(_fk("node.uuid"), ondelete="CASCADE"),
        nullable=False,
    )
    to_node_uuid = sa.Column(
        UUID(as_uuid=True),
        sa.ForeignKey(_fk("node.uuid"), ondelete="CASCADE"),
        nullable=False,
    )
    data = sa.Column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))
    created_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())

    __table_args__ = (
        sa.UniqueConstraint("edge_type", "from_node_uuid", "to_node_uuid", name="edge_uniq"),
        # NULL names are allowed; CHECK runs only when name IS NOT NULL.
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
    is mutable — a series can legitimately transition from flat to overlapping
    if the producer changes behavior.

    ``series_id`` stays BIGINT — it's the timedb-internal handle and never
    leaves the energydb / timedb pair.
    """

    __tablename__ = "series"

    series_id = sa.Column(sa.BigInteger, sa.Identity(always=False), primary_key=True)
    node_uuid = sa.Column(
        UUID(as_uuid=True),
        sa.ForeignKey(_fk("node.uuid"), ondelete="CASCADE"),
        nullable=True,
    )
    edge_uuid = sa.Column(
        UUID(as_uuid=True),
        sa.ForeignKey(_fk("edge.uuid"), ondelete="CASCADE"),
        nullable=True,
    )
    data_type = sa.Column(sa.Text, nullable=False)
    name = sa.Column(sa.Text, nullable=False)
    canonical_unit = sa.Column(sa.Text, nullable=False)
    timeseries_type = sa.Column(sa.Text, nullable=False)
    retention = sa.Column(sa.Text, nullable=False)
    description = sa.Column(sa.Text, nullable=True)
    inserted_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())

    __table_args__ = (
        sa.CheckConstraint("(node_uuid IS NULL) <> (edge_uuid IS NULL)", name="series_owner_xor"),
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


# ---------------------------------------------------------------------------
# DDL events — schema creation only
# ---------------------------------------------------------------------------

# A named schema must exist before its tables are created. For the default
# ``public`` schema (``SCHEMA is None``) this is a no-op — ``public`` always
# exists, and requiring CREATE-on-public privilege would be a needless ask.
#
# Series immutability (retention / canonical_unit / owner columns) is enforced
# in Python by :func:`energydb.series.register_series`; there is intentionally
# no DB trigger, so the schema is fully Alembic-autogeneratable.
if SCHEMA is not None:
    event.listen(
        Base.metadata,
        "before_create",
        sa.DDL(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"),
    )

# ``series_meta`` view — the read-only projection the ClickHouse ``concurrent`` read
# path resolves against: CH's PostgreSQL table engine selects from it. The DDL lives
# with its engine-table counterpart in ``_ch_meta_engine``; created after the tables
# by ``Client.create()`` and (re)created idempotently by ``Client.setup_ch_meta_engine()``
# since Alembic autogenerate does not track views.
CREATE_SERIES_META_VIEW, DROP_SERIES_META_VIEW = series_meta_view_ddl(f"{SCHEMA}." if SCHEMA else "")

event.listen(Base.metadata, "after_create", sa.DDL(CREATE_SERIES_META_VIEW))
event.listen(Base.metadata, "before_drop", sa.DDL(DROP_SERIES_META_VIEW))
