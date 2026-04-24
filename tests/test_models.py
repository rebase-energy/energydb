"""Tests for energydb SQLAlchemy models (no DB required)."""

from energydb.models import Base, Edge, Node, Series


class TestNodeModel:
    def test_table_name(self):
        assert Node.__tablename__ == "node"
        assert Node.__table__.schema == "energydb"

    def test_columns(self):
        col_names = {c.name for c in Node.__table__.columns}
        expected = {
            "node_id",
            "node_type",
            "name",
            "parent_id",
            "data",
            "created_at",
            "updated_at",
        }
        assert col_names == expected

    def test_primary_key(self):
        assert [c.name for c in Node.__table__.primary_key.columns] == ["node_id"]

    def test_parent_fk(self):
        fk = next(iter(Node.__table__.c.parent_id.foreign_keys))
        assert "node.node_id" in str(fk.column)

    def test_unique_constraint(self):
        names = {c.name for c in Node.__table__.constraints if hasattr(c, "name") and c.name}
        assert "node_child_uniq" in names


class TestEdgeModel:
    def test_table_name(self):
        assert Edge.__tablename__ == "edge"
        assert Edge.__table__.schema == "energydb"

    def test_columns(self):
        col_names = {c.name for c in Edge.__table__.columns}
        assert col_names == {
            "edge_id",
            "edge_type",
            "name",
            "from_node_id",
            "to_node_id",
            "data",
            "created_at",
            "updated_at",
        }


class TestSeriesModel:
    def test_table_name(self):
        assert Series.__tablename__ == "series"
        assert Series.__table__.schema == "energydb"

    def test_columns(self):
        col_names = {c.name for c in Series.__table__.columns}
        assert col_names == {
            "series_id",
            "node_id",
            "edge_id",
            "data_type",
            "name",
            "canonical_unit",
            "target_table",
            "description",
            "inserted_at",
        }

    def test_owner_xor_check(self):
        names = {c.name for c in Series.__table__.constraints if hasattr(c, "name") and c.name}
        assert "series_owner_xor" in names
        assert "series_node_uniq" in names
        assert "series_edge_uniq" in names

    def test_node_id_and_edge_id_nullable(self):
        assert Series.__table__.c.node_id.nullable is True
        assert Series.__table__.c.edge_id.nullable is True

    def test_target_table_not_nullable(self):
        assert Series.__table__.c.target_table.nullable is False
        assert Series.__table__.c.canonical_unit.nullable is False


def test_base_has_expected_tables():
    assert "energydb.node" in Base.metadata.tables
    assert "energydb.edge" in Base.metadata.tables
    assert "energydb.series" in Base.metadata.tables
