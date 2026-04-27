"""SQLAlchemy declarative models for EnergyDB PostgreSQL tables."""

from typing import cast

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Node(Base):
    __tablename__ = "node"

    node_id = sa.Column(sa.BigInteger, sa.Identity(always=False), primary_key=True)
    node_type = sa.Column(sa.Text, nullable=False)
    name = sa.Column(sa.Text, nullable=False)
    parent_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("energydb.node.node_id", ondelete="CASCADE"),
        nullable=True,
    )
    data = sa.Column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))
    created_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())

    parent = relationship("Node", remote_side=[node_id], back_populates="children_rel")
    children_rel = relationship("Node", back_populates="parent", cascade="all, delete-orphan")

    __table_args__ = (
        sa.UniqueConstraint("name", "node_type", "parent_id", name="node_child_uniq"),
        sa.Index("ix_node_parent_id", "parent_id"),
        sa.Index("ix_node_data_gin", "data", postgresql_using="gin"),
        {"schema": "energydb"},
    )


class Edge(Base):
    __tablename__ = "edge"

    edge_id = sa.Column(sa.BigInteger, sa.Identity(always=False), primary_key=True)
    edge_type = sa.Column(sa.Text, nullable=False)
    name = sa.Column(sa.Text, nullable=True)
    from_node_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("energydb.node.node_id", ondelete="CASCADE"),
        nullable=False,
    )
    to_node_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("energydb.node.node_id", ondelete="CASCADE"),
        nullable=False,
    )
    data = sa.Column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))
    created_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())
    updated_at = sa.Column(sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())

    __table_args__ = (
        sa.UniqueConstraint("edge_type", "from_node_id", "to_node_id", name="edge_uniq"),
        {"schema": "energydb"},
    )


class Series(Base):
    """Polymorphic series owned by either a node or an edge (exactly one).

    ``retention``, ``canonical_unit``, and the owner columns are immutable
    after insert (enforced by DB trigger). ``timeseries_type`` is mutable — a
    series can legitimately transition from flat to overlapping if the
    producer changes behavior.
    """

    __tablename__ = "series"

    series_id = sa.Column(sa.BigInteger, sa.Identity(always=False), primary_key=True)
    node_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("energydb.node.node_id", ondelete="CASCADE"),
        nullable=True,
    )
    edge_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("energydb.edge.edge_id", ondelete="CASCADE"),
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
        sa.CheckConstraint("(node_id IS NULL) <> (edge_id IS NULL)", name="series_owner_xor"),
        sa.UniqueConstraint("node_id", "data_type", "name", name="series_node_uniq"),
        sa.UniqueConstraint("edge_id", "data_type", "name", name="series_edge_uniq"),
        sa.CheckConstraint("timeseries_type IN ('FLAT','OVERLAPPING')", name="valid_timeseries_type"),
        sa.CheckConstraint("retention IN ('short','medium','long')", name="valid_retention"),
        sa.Index("ix_series_node", "node_id", postgresql_where=sa.text("node_id IS NOT NULL")),
        sa.Index("ix_series_edge", "edge_id", postgresql_where=sa.text("edge_id IS NOT NULL")),
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


ENERGYDB_TABLES: list[sa.Table] = [
    cast(sa.Table, Node.__table__),
    cast(sa.Table, Edge.__table__),
    cast(sa.Table, Series.__table__),
    cast(sa.Table, Run.__table__),
]
