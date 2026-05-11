"""Tests for ``register_tree`` and edge support on ``Client``.

Mocked-pool unit tests for the tree-walking, plus a small set of live
integration tests when ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL`` are set.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock
from uuid import UUID

import energydb as edb
import polars as pl
import pytest
from energydatamodel.reference import Reference
from energydb.client import Client

# ---------------------------------------------------------------------------
# Unit tests — mocked pool / td
# ---------------------------------------------------------------------------


def _simple_df(n: int = 3) -> pl.DataFrame:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    return pl.DataFrame(
        {
            "valid_time": [base + timedelta(hours=i) for i in range(n)],
            "value": [float(i) for i in range(n)],
        }
    )


def _mock_client(monkeypatch) -> Client:
    """Build a Client with both pool and td fully mocked."""
    monkeypatch.setattr(Client, "__init__", lambda self: None)
    client = Client()  # type: ignore[call-arg]
    client._pool = MagicMock()
    client.td = MagicMock()
    return client


class TestRegisterSeriesShapeDefault:
    """``series_mod.register_series`` derives the retention default from the
    series shape when the caller passes ``retention=None``."""

    @staticmethod
    def _mock_conn(returned_sid: int = 1):
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (returned_sid,)
        return conn

    def test_flat_defaults_to_forever(self):
        from uuid import uuid4

        from energydb import series as series_mod

        conn = self._mock_conn()
        series_mod.register_series(
            conn,
            node_uuid=uuid4(),
            edge_uuid=None,
            data_type="actual",
            name="power",
            canonical_unit="MW",
            timeseries_type="FLAT",
        )
        params = conn.execute.call_args.args[1]
        # retention is the 7th positional INSERT param (0-indexed 6).
        assert params[6] == "forever"

    def test_overlapping_defaults_to_medium(self):
        from uuid import uuid4

        from energydb import series as series_mod

        conn = self._mock_conn()
        series_mod.register_series(
            conn,
            node_uuid=uuid4(),
            edge_uuid=None,
            data_type="forecast",
            name="power",
            canonical_unit="MW",
            timeseries_type="OVERLAPPING",
        )
        params = conn.execute.call_args.args[1]
        assert params[6] == "medium"

    def test_explicit_retention_wins_over_default(self):
        from uuid import uuid4

        from energydb import series as series_mod

        conn = self._mock_conn()
        series_mod.register_series(
            conn,
            node_uuid=uuid4(),
            edge_uuid=None,
            data_type="actual",
            name="power",
            canonical_unit="MW",
            timeseries_type="FLAT",
            retention="short",
        )
        params = conn.execute.call_args.args[1]
        assert params[6] == "short"


class TestRegisterTree:
    def test_visits_every_node_with_correct_parent(self, monkeypatch):
        """``register_tree`` calls ``create_node`` once per node, with the
        uuid of the previous level as ``parent_uuid``."""
        client = _mock_client(monkeypatch)

        calls: list[tuple[str, UUID | None]] = []

        def fake_create_node(_pool, edm_obj, *, parent_uuid=None):
            calls.append((edm_obj.name, parent_uuid))
            return edm_obj.id

        monkeypatch.setattr("energydb._persist.create_node", fake_create_node)
        monkeypatch.setattr(
            "energydb._persist.create_edge",
            lambda *a, **kw: (_ for _ in ()).throw(AssertionError("edges shouldn't be visited")),
        )

        portfolio = edb.Portfolio(name="P")
        s1 = edb.Site(name="S1")
        t1 = edb.wind.WindTurbine(name="T1", capacity=3.5)
        t2 = edb.wind.WindTurbine(name="T2", capacity=3.5)
        s1.members = [t1, t2]
        s2 = edb.Site(name="S2")
        b1 = edb.battery.Battery(name="B1", storage_capacity=10)
        s2.members = [b1]
        portfolio.members = [s1, s2]

        root_uuid = client.register_tree(portfolio)

        assert root_uuid == portfolio.id
        assert calls == [
            ("P", None),
            ("S1", portfolio.id),
            ("T1", s1.id),
            ("T2", s1.id),
            ("S2", portfolio.id),
            ("B1", s2.id),
        ]
        client.td.write.assert_not_called()

    def test_metadata_only_tree_registers_no_data(self, monkeypatch):
        """Metadata-only TimeSeries are fine for register_tree; td.write is never called."""
        client = _mock_client(monkeypatch)
        monkeypatch.setattr(
            "energydb._persist.create_node",
            lambda _pool, obj, *, parent_uuid=None: obj.id,
        )

        tree = edb.wind.WindTurbine(
            name="T",
            capacity=3.5,
            timeseries=[edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
        )
        client.register_tree(tree)
        client.td.write.assert_not_called()

    def test_inline_data_rejected(self, monkeypatch):
        """register_tree raises if any TimeSeries has non-empty data."""
        client = _mock_client(monkeypatch)
        monkeypatch.setattr(
            "energydb._persist.create_node",
            lambda *a, **kw: (_ for _ in ()).throw(AssertionError("should not be called")),
        )

        tree = edb.wind.WindTurbine(
            name="T",
            capacity=3.5,
            timeseries=[edb.TimeSeries(_simple_df(2), name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
        )
        with pytest.raises(ValueError, match="register_tree"):
            client.register_tree(tree)

    def test_under_resolves_parent_path(self, monkeypatch):
        """``under=`` resolves to a parent_uuid and the tree is grafted there."""
        from uuid import uuid4

        client = _mock_client(monkeypatch)

        parent_uuid = uuid4()

        # Different SQL queries fire during register_tree_under:
        #   1. resolve_node_uuid("Region/Site")           → returns 1-col row
        #   2. _fetch_nodes_by_uuids([turbine.id])         → returns 5-col rows
        # Use side_effect to dispatch based on the SQL substring.
        def execute_side_effect(sql, *args, **kwargs):
            res = MagicMock()
            if "WITH RECURSIVE walk" in sql:
                res.fetchall.return_value = [(parent_uuid,)]
            elif "FROM energydb.node WHERE uuid = ANY" in sql:
                # New target node — not yet in DB.
                res.fetchall.return_value = []
            else:
                res.fetchall.return_value = []
            return res

        conn = MagicMock()
        conn.execute.side_effect = execute_side_effect
        client._pool.connection.return_value.__enter__.return_value = conn

        captured_parent: list[UUID | None] = []

        def fake_create_node(_pool, obj, *, parent_uuid=None):
            captured_parent.append(parent_uuid)
            return obj.id

        monkeypatch.setattr("energydb._persist.create_node", fake_create_node)

        client.register_tree(edb.wind.WindTurbine(name="T", capacity=3.5), under=("Region", "Site"))
        assert captured_parent == [parent_uuid]


class TestTwoPassWalk:
    def test_edges_skipped_in_node_pass_then_visited(self, monkeypatch):
        client = _mock_client(monkeypatch)

        node_calls: list[str] = []
        edge_calls: list[str] = []

        def fake_create_node(_pool, obj, *, parent_uuid=None):
            node_calls.append(obj.name)
            return obj.id

        def fake_create_edge(_pool, obj, *, tree_root=None):
            edge_calls.append(obj.name)
            return obj.id

        monkeypatch.setattr("energydb._persist.create_node", fake_create_node)
        monkeypatch.setattr("energydb._persist.create_edge", fake_create_edge)

        bus_a = edb.grid.JunctionPoint(name="BusA")
        bus_b = edb.grid.JunctionPoint(name="BusB")
        line = edb.grid.Line(name="L1", capacity=500, from_element=Reference(bus_a), to_element=Reference(bus_b))
        tree = edb.Portfolio(name="Grid", members=[bus_a, bus_b, line])
        client.register_tree(tree)

        assert "Grid" in node_calls
        assert "BusA" in node_calls
        assert "BusB" in node_calls
        assert "L1" not in node_calls
        assert "L1" in edge_calls


# ---------------------------------------------------------------------------
# Live integration tests (skipped without env vars)
# ---------------------------------------------------------------------------


pytestmark_live = pytest.mark.skipif(
    not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")),
    reason="TIMEDB_PG_DSN / TIMEDB_CH_URL not set",
)


@pytest.fixture
def live_edb():
    client = Client()
    client.delete()
    client.create()
    yield client
    client.delete()
    client.close()


@pytestmark_live
def test_live_register_tree_then_write_and_read(live_edb):
    """Structure registration + manifest write + scope read round-trip."""
    base = datetime(2026, 1, 1, tzinfo=UTC)
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.Site(
                name="S",
                members=[
                    edb.wind.WindTurbine(
                        name="T1",
                        capacity=3.5,
                        timeseries=[edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
                    )
                ],
            )
        ],
    )
    root_uuid = live_edb.register_tree(tree)
    assert isinstance(root_uuid, UUID)

    write_df = pl.DataFrame(
        {
            "path": [["P", "S", "T1"]] * 3,
            "data_type": ["actual"] * 3,
            "name": ["power"] * 3,
            "valid_time": [base + timedelta(hours=i) for i in range(3)],
            "value": [1.0, 2.0, 3.0],
        }
    )
    live_edb.write(write_df)

    out = live_edb.get_node("P").get_node("S").get_node("T1").read(data_type="actual", name="power", output="polars")
    assert out["value"].to_list() == [1.0, 2.0, 3.0]


@pytestmark_live
def test_live_create_edge_with_series(live_edb):
    """Create two grid nodes, an edge between them, register a series on the
    edge, write data, read it back."""
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
    live_edb.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b]))

    line = edb.grid.Line(name="Cable-1", capacity=500, from_element=Reference(bus_a), to_element=Reference(bus_b))
    edge_uuid = live_edb.create_edge(line)
    assert isinstance(edge_uuid, UUID)

    edge = live_edb.get_edge(uuid=edge_uuid).get()
    assert type(edge).__name__ == "Line"

    scope = live_edb.get_edge(uuid=edge_uuid)
    scope.register_series(
        name="power_flow",
        canonical_unit="MW",
        data_type="actual",
        timeseries_type="FLAT",
        retention="medium",
    )
    base = datetime(2026, 2, 1, tzinfo=UTC)
    flow = pl.DataFrame(
        {
            "valid_time": [base + timedelta(hours=i) for i in range(2)],
            "value": [200.0, 250.0],
        }
    )
    scope.write(flow, data_type="actual", name="power_flow")
    out = scope.read(data_type="actual", name="power_flow", output="polars")
    assert out["value"].to_list() == [200.0, 250.0]


@pytestmark_live
def test_live_manifest_read(live_edb):
    base = datetime(2026, 3, 1, tzinfo=UTC)
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.wind.WindTurbine(
                name="T1",
                capacity=3.5,
                timeseries=[edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
            ),
            edb.wind.WindTurbine(
                name="T2",
                capacity=3.5,
                timeseries=[edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
            ),
        ],
    )
    live_edb.register_tree(tree)

    write_df = pl.DataFrame(
        {
            "path": [["P", "T1"]] * 2 + [["P", "T2"]] * 2,
            "data_type": ["actual"] * 4,
            "name": ["power"] * 4,
            "valid_time": [base + timedelta(hours=i) for i in range(2)] * 2,
            "value": [1.0, 2.0, 10.0, 20.0],
        }
    )
    live_edb.write(write_df)

    manifest = pl.DataFrame(
        {
            "path": [["P", "T1"], ["P", "T2"]],
            "data_type": ["actual", "actual"],
            "name": ["power", "power"],
        }
    )
    out = live_edb.read(manifest, output="polars")
    assert set(out["node"].unique().to_list()) == {"T1", "T2"}
    assert "path" in out.columns


@pytestmark_live
def test_live_get_tree_with_series(live_edb):
    tree = edb.Portfolio(
        name="Pg",
        members=[
            edb.wind.WindTurbine(
                name="Tg",
                capacity=3.5,
                timeseries=[edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
            )
        ],
    )
    live_edb.register_tree(tree)

    rebuilt = live_edb.get_tree("Pg", include_series=True)
    assert rebuilt.name == "Pg"
    turbine = rebuilt.children()[0]
    assert turbine.name == "Tg"
    assert turbine.timeseries is not None
    assert any(t.name == "power" for t in turbine.timeseries)


@pytestmark_live
def test_live_query_within_accepts_str_tuple_list_uuid(live_edb):
    """``within=`` should accept a bare str, tuple, list, or UUID."""
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
    portfolio = edb.Portfolio(name="My Portfolio", members=[bus_a, bus_b])
    portfolio_uuid = live_edb.register_tree(portfolio)

    line = edb.grid.Line(name="Cable-1", capacity=500, from_element=Reference(bus_a), to_element=Reference(bus_b))
    live_edb.create_edge(line)

    by_str = live_edb.query_nodes(within="My Portfolio")
    by_tuple = live_edb.query_nodes(within=("My Portfolio",))
    by_list = live_edb.query_nodes(within=["My Portfolio"])
    by_uuid = live_edb.query_nodes(within=portfolio_uuid)
    expected_names = {"My Portfolio", "BusA", "BusB"}
    assert {n.name for n in by_str} == expected_names
    assert {n.name for n in by_tuple} == expected_names
    assert {n.name for n in by_list} == expected_names
    assert {n.name for n in by_uuid} == expected_names

    e_str = live_edb.query_edges(type="Line", within="My Portfolio")
    e_tuple = live_edb.query_edges(type="Line", within=("My Portfolio",))
    e_list = live_edb.query_edges(type="Line", within=["My Portfolio"])
    e_uuid = live_edb.query_edges(type="Line", within=portfolio_uuid)
    assert len(e_str) == 1
    assert len(e_tuple) == 1
    assert len(e_list) == 1
    assert len(e_uuid) == 1
