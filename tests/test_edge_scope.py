"""Tests for EdgeScope (monkeypatched, same pattern as tree tests)."""

from __future__ import annotations

from unittest.mock import MagicMock, call

import polars as pl
import pytest

from energydb.scope import EdgeScope


def _make_scope(edge_id: int | None = None, name: str | None = None):
    td = MagicMock()
    return EdgeScope(td, edge_id=edge_id, name=name)


class TestEdgeScopeResolution:
    def test_resolve_by_id(self):
        scope = _make_scope(edge_id=42)
        conn = MagicMock()
        assert scope._resolve_edge_id(conn) == 42

    def test_resolve_by_name(self, monkeypatch):
        scope = _make_scope(name="L1")
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [(99,)]

        from energydb import _resolve
        monkeypatch.setattr(
            _resolve, "resolve_edge_id_by_name",
            lambda c, n: 99,
        )
        assert scope._resolve_edge_id(conn) == 99

    def test_resolve_no_id_or_name_raises(self):
        scope = _make_scope()
        conn = MagicMock()
        with pytest.raises(ValueError, match="no edge_id or name"):
            scope._resolve_edge_id(conn)


class TestEdgeScopeDelete:
    def test_delete_calls_correct_sql(self):
        td = MagicMock()
        scope = EdgeScope(td, edge_id=7)

        mock_conn = MagicMock()
        td.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        td.connection.return_value.__exit__ = MagicMock(return_value=False)

        scope.delete()

        mock_conn.execute.assert_called_once_with(
            "DELETE FROM energydb.edge WHERE edge_id = %s",
            (7,),
        )
        mock_conn.commit.assert_called_once()


class TestEdgeScopeUpdate:
    def test_update_data(self):
        td = MagicMock()
        scope = EdgeScope(td, edge_id=7)

        mock_conn = MagicMock()
        td.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        td.connection.return_value.__exit__ = MagicMock(return_value=False)

        scope.update(data={"capacity": 999})

        # Should have called execute with UPDATE SQL
        sql = mock_conn.execute.call_args[0][0]
        assert "UPDATE energydb.edge SET" in sql
        assert "data = data ||" in sql
        mock_conn.commit.assert_called_once()


class TestEnergyDataClientEdgeRead:
    """Bulk read/read_relative must dispatch to the edge-aware join helper
    when the manifest is edge-routed."""

    def _make_client_with_conn(self):
        from energydb.client import EnergyDataClient

        td = MagicMock()
        mock_conn = MagicMock()
        td.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        td.connection.return_value.__exit__ = MagicMock(return_value=False)
        return EnergyDataClient(td), td, mock_conn

    def _install_stubs(self, monkeypatch, resolved_df):
        from energydb import client as client_mod

        node_spy = MagicMock(return_value=pl.DataFrame({"hierarchy": ["node"]}))
        edge_spy = MagicMock(return_value=pl.DataFrame({"hierarchy": ["edge"]}))
        monkeypatch.setattr(client_mod, "resolve_manifest", lambda conn, m: resolved_df)
        monkeypatch.setattr(client_mod, "join_hierarchy", node_spy)
        monkeypatch.setattr(client_mod, "join_edge_hierarchy", edge_spy)
        return node_spy, edge_spy

    def test_read_edge_manifest_uses_edge_hierarchy(self, monkeypatch):
        client, td, _conn = self._make_client_with_conn()
        manifest = pl.DataFrame(
            {"edge_id": [1], "data_type": ["flow"], "name": ["mw"]}
        )
        resolved = manifest.with_columns(pl.lit(101).alias("series_id"))
        td.read.return_value = pl.DataFrame({"series_id": [101], "value": [1.0]})
        node_spy, edge_spy = self._install_stubs(monkeypatch, resolved)

        out = client.read(manifest)

        edge_spy.assert_called_once()
        node_spy.assert_not_called()
        assert out.equals(pl.DataFrame({"hierarchy": ["edge"]}))

    def test_read_node_manifest_uses_node_hierarchy(self, monkeypatch):
        client, td, _conn = self._make_client_with_conn()
        manifest = pl.DataFrame(
            {"node_id": [1], "data_type": ["production"], "name": ["mw"]}
        )
        resolved = manifest.with_columns(pl.lit(101).alias("series_id"))
        td.read.return_value = pl.DataFrame({"series_id": [101], "value": [1.0]})
        node_spy, edge_spy = self._install_stubs(monkeypatch, resolved)

        out = client.read(manifest)

        node_spy.assert_called_once()
        edge_spy.assert_not_called()
        assert out.equals(pl.DataFrame({"hierarchy": ["node"]}))

    def test_read_relative_edge_manifest_uses_edge_hierarchy(self, monkeypatch):
        client, td, _conn = self._make_client_with_conn()
        manifest = pl.DataFrame(
            {"edge_id": [1], "data_type": ["flow"], "name": ["mw"]}
        )
        resolved = manifest.with_columns(pl.lit(101).alias("series_id"))
        td.read_relative.return_value = pl.DataFrame({"series_id": [101], "value": [1.0]})
        node_spy, edge_spy = self._install_stubs(monkeypatch, resolved)

        out = client.read_relative(manifest)

        edge_spy.assert_called_once()
        node_spy.assert_not_called()
        assert out.equals(pl.DataFrame({"hierarchy": ["edge"]}))

    def test_read_relative_node_manifest_uses_node_hierarchy(self, monkeypatch):
        client, td, _conn = self._make_client_with_conn()
        manifest = pl.DataFrame(
            {"node_id": [1], "data_type": ["production"], "name": ["mw"]}
        )
        resolved = manifest.with_columns(pl.lit(101).alias("series_id"))
        td.read_relative.return_value = pl.DataFrame({"series_id": [101], "value": [1.0]})
        node_spy, edge_spy = self._install_stubs(monkeypatch, resolved)

        out = client.read_relative(manifest)

        node_spy.assert_called_once()
        edge_spy.assert_not_called()
        assert out.equals(pl.DataFrame({"hierarchy": ["node"]}))
