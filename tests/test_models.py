"""Tests for energydb SQLAlchemy models (no DB required — schema inspection only)."""

from energydb.models import Base, Edge, EdgeSeries, Node, NodeSeries


class TestNodeModel:
    def test_table_name(self):
        assert Node.__tablename__ == "node"
        assert Node.__table__.schema == "energydb"

    def test_columns(self):
        col_names = {c.name for c in Node.__table__.columns}
        expected = {
            "node_id", "node_type", "name", "parent_id", "properties",
            "latitude", "longitude", "altitude", "timezone",
            "created_at", "updated_at",
        }
        assert col_names == expected

    def test_primary_key(self):
        pk_cols = [c.name for c in Node.__table__.primary_key.columns]
        assert pk_cols == ["node_id"]

    def test_parent_fk(self):
        parent_col = Node.__table__.c.parent_id
        fk = list(parent_col.foreign_keys)[0]
        assert "node.node_id" in str(fk.column)

    def test_unique_constraint(self):
        constraints = {c.name for c in Node.__table__.constraints if hasattr(c, "name") and c.name}
        assert "node_child_uniq" in constraints

    def test_indexes(self):
        index_names = {idx.name for idx in Node.__table__.indexes}
        assert "ix_node_parent_id" in index_names
        assert "ix_node_properties_gin" in index_names


class TestNodeSeriesModel:
    def test_table_name(self):
        assert NodeSeries.__tablename__ == "node_series"
        assert NodeSeries.__table__.schema == "energydb"

    def test_columns(self):
        col_names = {c.name for c in NodeSeries.__table__.columns}
        assert col_names == {"node_id", "series_id", "data_type", "name"}

    def test_composite_pk(self):
        pk_cols = {c.name for c in NodeSeries.__table__.primary_key.columns}
        assert pk_cols == {"node_id", "series_id"}

    def test_fks(self):
        fk_targets = set()
        for col in NodeSeries.__table__.columns:
            for fk in col.foreign_keys:
                fk_targets.add(str(fk.column))
        assert any("node.node_id" in t for t in fk_targets)
        assert any("series.series_id" in t for t in fk_targets)


class TestEdgeModel:
    def test_table_name(self):
        assert Edge.__tablename__ == "edge"
        assert Edge.__table__.schema == "energydb"

    def test_columns(self):
        col_names = {c.name for c in Edge.__table__.columns}
        expected = {
            "edge_id", "edge_type", "name",
            "from_node_id", "to_node_id",
            "properties", "directed", "created_at", "updated_at",
        }
        assert col_names == expected

    def test_unique_constraint(self):
        constraints = {c.name for c in Edge.__table__.constraints if hasattr(c, "name") and c.name}
        assert "edge_uniq" in constraints

    def test_name_nullable(self):
        name_col = Edge.__table__.c.name
        assert name_col.nullable is True

    def test_updated_at_present(self):
        col_names = {c.name for c in Edge.__table__.columns}
        assert "updated_at" in col_names


class TestEdgeSeriesModel:
    def test_table_name(self):
        assert EdgeSeries.__tablename__ == "edge_series"
        assert EdgeSeries.__table__.schema == "energydb"

    def test_composite_pk(self):
        pk_cols = {c.name for c in EdgeSeries.__table__.primary_key.columns}
        assert pk_cols == {"edge_id", "series_id"}


class TestMetadata:
    def test_all_tables_present(self):
        table_names = {t.name for t in Base.metadata.sorted_tables}
        assert "node" in table_names
        assert "node_series" in table_names
        assert "edge" in table_names
        assert "edge_series" in table_names
        assert "series" in table_names  # timedb proxy

    def test_table_count(self):
        assert len(Base.metadata.sorted_tables) == 5
