"""
SQLAlchemy declarative models for EnergyDB PostgreSQL tables.

All tables live in the ``energydb`` Postgres schema. Cross-schema FK
references point to ``series.series_id`` (in the public schema).

Usage in an Alembic-managed monorepo::

    from energydb.models import Base as EnergyDBBase
    from timedb.models import Base as TimeDBBase

    target_metadata = [TimeDBBase.metadata, EnergyDBBase.metadata]
"""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


# Proxy: register public.series in this MetaData so cross-schema FKs resolve.
# Only declares the PK — the real table is created by timedb.
_series_proxy = sa.Table(
    "series",
    Base.metadata,
    sa.Column("series_id", sa.BigInteger, primary_key=True),
)


# ---------------------------------------------------------------------------
# Operational tables
# ---------------------------------------------------------------------------


class Node(Base):
    """Any node in the hierarchy — Portfolio, Site, WindFarm, WindTurbine, etc."""

    __tablename__ = "node"

    node_id = sa.Column(sa.BigInteger, sa.Identity(always=False), primary_key=True)
    node_type = sa.Column(sa.Text, nullable=False)
    name = sa.Column(sa.Text, nullable=False)
    parent_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("energydb.node.node_id", ondelete="CASCADE"),
        nullable=True,
    )
    properties = sa.Column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))
    latitude = sa.Column(sa.Float, nullable=True)
    longitude = sa.Column(sa.Float, nullable=True)
    altitude = sa.Column(sa.Float, nullable=True)
    timezone = sa.Column(sa.Text, nullable=True)
    created_at = sa.Column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
    )
    updated_at = sa.Column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
    )

    # Self-referential relationship
    parent = relationship("Node", remote_side=[node_id], back_populates="children_rel")
    children_rel = relationship(
        "Node", back_populates="parent", cascade="all, delete-orphan"
    )

    __table_args__ = (
        # Two children of the same parent can't share (name, type)
        sa.UniqueConstraint("name", "node_type", "parent_id", name="node_child_uniq"),
        # No uniqueness on root names — platform handles multi-tenant scoping.
        # The UNIQUE constraint with NULL parent_id naturally allows duplicates
        # because NULL != NULL in SQL.
        sa.Index("ix_node_parent_id", "parent_id"),
        sa.Index("ix_node_properties_gin", "properties", postgresql_using="gin"),
        {"schema": "energydb"},
    )


class NodeSeries(Base):
    """Links a node to a timedb series."""

    __tablename__ = "node_series"

    node_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("energydb.node.node_id", ondelete="CASCADE"),
        primary_key=True,
    )
    series_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("series.series_id"),
        primary_key=True,
    )
    data_type = sa.Column(sa.Text, nullable=False)
    name = sa.Column(sa.Text, nullable=False)

    __table_args__ = (
        sa.Index("ix_node_series_series_id", "series_id"),
        {"schema": "energydb"},
    )


# ---------------------------------------------------------------------------
# Edge tables
# ---------------------------------------------------------------------------


class Edge(Base):
    """Typed edge between two nodes (grid topology, power flow, etc.)."""

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
    properties = sa.Column(JSONB, nullable=False, server_default=sa.text("'{}'::jsonb"))
    directed = sa.Column(sa.Boolean, nullable=False, server_default=sa.text("true"))
    created_at = sa.Column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
    )
    updated_at = sa.Column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
    )

    __table_args__ = (
        sa.UniqueConstraint(
            "edge_type", "from_node_id", "to_node_id", name="edge_uniq"
        ),
        {"schema": "energydb"},
    )


class EdgeSeries(Base):
    """Links an edge to a timedb series (power flow, thermal data, etc.)."""

    __tablename__ = "edge_series"

    edge_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("energydb.edge.edge_id", ondelete="CASCADE"),
        primary_key=True,
    )
    series_id = sa.Column(
        sa.BigInteger,
        sa.ForeignKey("series.series_id"),
        primary_key=True,
    )
    data_type = sa.Column(sa.Text, nullable=False)
    name = sa.Column(sa.Text, nullable=False)

    __table_args__ = (
        sa.Index("ix_edge_series_series_id", "series_id"),
        {"schema": "energydb"},
    )


# Tables that EnergyDB owns and should create. Excludes the series proxy
# (owned by TimeDB) so create_all() never creates an incomplete public.series.
ENERGYDB_TABLES = [
    Node.__table__,
    NodeSeries.__table__,
    Edge.__table__,
    EdgeSeries.__table__,
]
