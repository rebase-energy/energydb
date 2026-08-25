"""Tests for energydb SQLAlchemy models (no DB required).

Schema-agnostic: ENERGYDB_SCHEMA is import-time configuration (the platform
runs in ``public`` → ``SCHEMA is None``; the integration suite isolates into
a named schema), so expectations derive from ``SCHEMA`` instead of
hardcoding either layout.
"""

from energydb.models import SCHEMA, Base, Edge, Node, Run, Series


def _metadata_key(name: str) -> str:
    return f"{SCHEMA}.{name}" if SCHEMA else name


class TestNodeModel:
    def test_table_name(self):
        assert Node.__tablename__ == "node"
        assert Node.__table__.schema == SCHEMA

    def test_columns(self):
        col_names = {c.name for c in Node.__table__.columns}
        expected = {
            "uuid",
            "namespace",
            "node_type",
            "name",
            "parent_uuid",
            "path",
            "data",
            "created_at",
            "updated_at",
        }
        assert col_names == expected

    def test_primary_key(self):
        assert [c.name for c in Node.__table__.primary_key.columns] == ["uuid"]

    def test_parent_fk(self):
        fk = next(iter(Node.__table__.c.parent_uuid.foreign_keys))
        assert "node.uuid" in str(fk.column)

    def test_unique_constraint(self):
        names = {c.name for c in Node.__table__.constraints if hasattr(c, "name") and c.name}
        assert "node_child_uniq" in names


class TestEdgeModel:
    def test_table_name(self):
        assert Edge.__tablename__ == "edge"
        assert Edge.__table__.schema == SCHEMA

    def test_columns(self):
        col_names = {c.name for c in Edge.__table__.columns}
        assert col_names == {
            "uuid",
            "namespace",
            "edge_type",
            "name",
            "from_node_uuid",
            "to_node_uuid",
            "data",
            "created_at",
            "updated_at",
        }


class TestSeriesModel:
    def test_table_name(self):
        assert Series.__tablename__ == "series"
        assert Series.__table__.schema == SCHEMA

    def test_columns(self):
        col_names = {c.name for c in Series.__table__.columns}
        assert col_names == {
            "series_id",
            "namespace",
            "node_uuid",
            "edge_uuid",
            "data_type",
            "name",
            "canonical_unit",
            "timeseries_type",
            "retention",
            "description",
            "inserted_at",
        }

    def test_owner_xor_check(self):
        names = {c.name for c in Series.__table__.constraints if hasattr(c, "name") and c.name}
        assert "series_owner_xor" in names
        assert "series_node_uniq" in names
        assert "series_edge_uniq" in names

    def test_node_uuid_and_edge_uuid_nullable(self):
        assert Series.__table__.c.node_uuid.nullable is True
        assert Series.__table__.c.edge_uuid.nullable is True

    def test_immutable_fields_not_nullable(self):
        assert Series.__table__.c.retention.nullable is False
        assert Series.__table__.c.canonical_unit.nullable is False
        assert Series.__table__.c.timeseries_type.nullable is False

    def test_no_target_table_column(self):
        """target_table was removed in the unified ``series_values`` redesign."""
        col_names = {c.name for c in Series.__table__.columns}
        assert "target_table" not in col_names

    def test_check_constraints(self):
        names = {c.name for c in Series.__table__.constraints if hasattr(c, "name") and c.name}
        assert "valid_timeseries_type" in names
        # retention vocabulary is owned by timedb (RETENTION_TIERS), no PG CHECK
        assert "valid_retention" not in names


class TestRunModel:
    def test_table_name(self):
        assert Run.__tablename__ == "runs"
        assert Run.__table__.schema == SCHEMA

    def test_columns(self):
        col_names = {c.name for c in Run.__table__.columns}
        assert col_names == {
            "run_id",
            "workflow_id",
            "model_name",
            "run_start_time",
            "run_finish_time",
            "run_params",
            "inserted_at",
        }

    def test_primary_key(self):
        assert [c.name for c in Run.__table__.primary_key.columns] == ["run_id"]


class TestNamespaceTenancy:
    """The namespace partition key is structural: composite FKs make
    cross-namespace children/edges/series unrepresentable."""

    def test_namespace_columns_not_nullable(self):
        for model in (Node, Edge, Series):
            col = model.__table__.c.namespace
            assert col.nullable is False
            assert col.server_default is not None  # GUC write-stamp default

    def test_composite_same_namespace_fkeys(self):
        node_names = {c.name for c in Node.__table__.constraints if getattr(c, "name", None)}
        assert "node_uuid_namespace_uniq" in node_names
        assert "node_parent_namespace_fkey" in node_names
        assert "node_path_uniq" in node_names  # now (namespace, path)
        edge_names = {c.name for c in Edge.__table__.constraints if getattr(c, "name", None)}
        assert "edge_uuid_namespace_uniq" in edge_names
        assert "edge_from_node_namespace_fkey" in edge_names
        assert "edge_to_node_namespace_fkey" in edge_names
        series_names = {c.name for c in Series.__table__.constraints if getattr(c, "name", None)}
        assert "series_node_namespace_fkey" in series_names
        assert "series_edge_namespace_fkey" in series_names


def test_base_has_expected_tables():
    for name in ("node", "edge", "series", "runs"):
        assert _metadata_key(name) in Base.metadata.tables
