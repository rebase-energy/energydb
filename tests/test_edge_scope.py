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
    def test_update_properties(self):
        td = MagicMock()
        scope = EdgeScope(td, edge_id=7)

        mock_conn = MagicMock()
        td.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        td.connection.return_value.__exit__ = MagicMock(return_value=False)

        scope.update(properties={"capacity": 999})

        # Should have called execute with UPDATE SQL
        sql = mock_conn.execute.call_args[0][0]
        assert "UPDATE energydb.edge SET" in sql
        assert "properties = properties ||" in sql
        mock_conn.commit.assert_called_once()
