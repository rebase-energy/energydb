"""SQLAlchemy declarative models for EnergyDB PostgreSQL tables.

These models are the single schema source of truth — Alembic-friendly. The
``energydb`` schema, the immutability trigger on ``series``, and the partial
unique index on root names are all attached as ``DDL`` events on
``Base.metadata``.

UUID is the primary identity for every row in ``node`` and ``edge``.
``parent_uuid`` and ``edge.from_node_uuid`` / ``to_node_uuid`` are FKs by
UUID — the application Reference holds a UUID and writes it directly into
the FK column, no translation step. ``series.series_id`` stays BIGINT (it's
timedb-internal, not an EDM identity).

Retention tier names are owned by :data:`timedb.RETENTION_TIERS`; energydb
does **not** encode them in a CHECK constraint, so adding a tier in timedb
does not require an energydb migration.
"""

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Node(Base):
    __tablename__ = "node"

    uuid = sa.Column(UUID(as_uuid=True), primary_key=True)
    node_type = sa.Column(sa.Text, nullable=False)
    name = sa.Column(sa.Text, nullable=False)
    parent_uuid = sa.Column(
        UUID(as_uuid=True),
        sa.ForeignKey("energydb.node.uuid", ondelete="CASCADE"),
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
        {"schema": "energydb"},
    )


class Edge(Base):
    __tablename__ = "edge"

    uuid = sa.Column(UUID(as_uuid=True), primary_key=True)
    edge_type = sa.Column(sa.Text, nullable=False)
    name = sa.Column(sa.Text, nullable=True)
    from_node_uuid = sa.Column(
        UUID(as_uuid=True),
        sa.ForeignKey("energydb.node.uuid", ondelete="CASCADE"),
        nullable=False,
    )
    to_node_uuid = sa.Column(
        UUID(as_uuid=True),
        sa.ForeignKey("energydb.node.uuid", ondelete="CASCADE"),
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
        {"schema": "energydb"},
    )


class Series(Base):
    """Polymorphic series owned by either a node or an edge (exactly one).

    ``retention``, ``canonical_unit``, and the owner columns are immutable
    after insert (enforced by DB trigger). ``timeseries_type`` is mutable — a
    series can legitimately transition from flat to overlapping if the
    producer changes behavior.

    ``series_id`` stays BIGINT — it's the timedb-internal handle and never
    leaves the energydb / timedb pair.
    """

    __tablename__ = "series"

    series_id = sa.Column(sa.BigInteger, sa.Identity(always=False), primary_key=True)
    node_uuid = sa.Column(
        UUID(as_uuid=True),
        sa.ForeignKey("energydb.node.uuid", ondelete="CASCADE"),
        nullable=True,
    )
    edge_uuid = sa.Column(
        UUID(as_uuid=True),
        sa.ForeignKey("energydb.edge.uuid", ondelete="CASCADE"),
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
        {"schema": "energydb"},
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
        {"schema": "energydb"},
    )


# ---------------------------------------------------------------------------
# DDL events — schema, trigger function, immutability trigger
# ---------------------------------------------------------------------------

# The ``energydb`` schema has to exist before any of the tables (which all
# carry ``schema="energydb"`` in their __table_args__) can be created.
event.listen(
    Base.metadata,
    "before_create",
    sa.DDL("CREATE SCHEMA IF NOT EXISTS energydb"),
)

# Immutability guard on ``series``: retention, canonical_unit, and the owner
# columns can't change after insert. Reclassifying a series means registering
# a new one (preserves CH-side data integrity).
_SERIES_GUARD_TRIGGER_FN = sa.DDL(
    """
    CREATE OR REPLACE FUNCTION energydb._series_guard_immutable()
    RETURNS TRIGGER AS $$
    BEGIN
        IF NEW.retention      IS DISTINCT FROM OLD.retention
        OR NEW.canonical_unit IS DISTINCT FROM OLD.canonical_unit
        OR NEW.node_uuid      IS DISTINCT FROM OLD.node_uuid
        OR NEW.edge_uuid      IS DISTINCT FROM OLD.edge_uuid THEN
            RAISE EXCEPTION
                'energydb.series: retention, canonical_unit, and owner columns '
                'are immutable. Register a new series to change tier, unit, or '
                'ownership.';
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """
)

_SERIES_GUARD_TRIGGER = sa.DDL(
    """
    DROP TRIGGER IF EXISTS series_guard_immutable ON energydb.series;
    CREATE TRIGGER series_guard_immutable
        BEFORE UPDATE ON energydb.series
        FOR EACH ROW EXECUTE FUNCTION energydb._series_guard_immutable();
    """
)

event.listen(
    Series.__table__,
    "after_create",
    _SERIES_GUARD_TRIGGER_FN,
)
event.listen(
    Series.__table__,
    "after_create",
    _SERIES_GUARD_TRIGGER,
)

# LIKE-escape helper. The materialized ``node.path`` column is used as a
# literal prefix in ``LIKE`` patterns for subtree queries. Names may legally
# contain ``_`` (and, less commonly, ``%`` / ``\``) which would be interpreted
# as LIKE metacharacters — escape them at the SQL boundary instead of forcing
# every read site to do a per-row regex. ``CREATE OR REPLACE`` is idempotent.
_LIKE_ESC_FN = sa.DDL(
    r"""
    CREATE OR REPLACE FUNCTION energydb._like_esc(text)
    RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS $$
        SELECT replace(replace(replace($1, E'\\', E'\\\\'),
                               '%%', E'\\%%'),
                       '_',  E'\\_')
    $$;
    """
)

event.listen(
    Base.metadata,
    "after_create",
    _LIKE_ESC_FN.execute_if(dialect="postgresql"),
)
