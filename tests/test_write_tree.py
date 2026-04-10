"""Tests for the declarative ``EnergyDataClient.write(tree)`` flow.

These are pure unit tests — they patch the two I/O entry points
(``_create_node`` and ``td.write``) and verify that tree walking, parent
resolution, and pending-data collection behave correctly. Integration against
a live database is covered by running the quickstart notebook end-to-end.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import polars as pl
import pytest

import energydb as edb
from energydb.client import EnergyDataClient


# ---------------------------------------------------------------------------
# Re-exports
# ---------------------------------------------------------------------------


class TestReExports:
    def test_edm_core_classes_exposed(self):
        assert edb.Entity is not None
        assert edb.Node is not None
        assert edb.Edge is not None
        assert edb.Reference is not None

    def test_edm_base_classes_exposed(self):
        assert edb.Asset is not None
        assert edb.GridNode is not None
        assert edb.Sensor is not None
        assert edb.Collection is not None
        assert edb.Area is not None

    def test_edm_asset_classes_exposed(self):
        assert edb.Portfolio is not None
        assert edb.Site is not None
        assert edb.WindFarm is not None
        assert edb.WindTurbine is not None
        assert edb.PVSystem is not None
        assert edb.Battery is not None
        assert edb.Building is not None
        assert edb.House is not None
        assert edb.MultiSite is not None
        assert edb.HeatPump is not None
        assert edb.HydroPowerPlant is not None
        assert edb.HydroTurbine is not None
        assert edb.Reservoir is not None
        assert edb.PVArray is not None
        assert edb.SolarPowerArea is not None
        assert edb.WindPowerArea is not None

    def test_edm_edge_classes_exposed(self):
        assert edb.Line is not None
        assert edb.Link is not None
        assert edb.Transformer is not None
        assert edb.Pipe is not None
        assert edb.Interconnection is not None

    def test_edm_grid_and_sensor_classes_exposed(self):
        assert edb.JunctionPoint is not None
        assert edb.Meter is not None
        assert edb.DeliveryPoint is not None
        assert edb.TemperatureSensor is not None
        assert edb.RadiationSensor is not None
        assert edb.WindSpeedSensor is not None
        assert edb.HumiditySensor is not None
        assert edb.RainSensor is not None

    def test_edm_area_classes_exposed(self):
        assert edb.BiddingZone is not None
        assert edb.Country is not None
        assert edb.ControlArea is not None
        assert edb.SynchronousArea is not None
        assert edb.WeatherCell is not None

    def test_edm_container_classes_exposed(self):
        assert edb.SubNetwork is not None
        assert edb.Network is not None
        assert edb.Region is not None
        assert edb.EnergyCommunity is not None
        assert edb.VirtualPowerPlant is not None

    def test_scope_classes_exposed(self):
        assert edb.NodeScope is not None
        assert edb.EdgeScope is not None

    def test_series_types_exposed(self):
        assert edb.TimeSeries is not None
        assert edb.TimeSeriesDescriptor is not None
        assert edb.DataType is not None
        assert edb.DataShape is not None
        assert edb.Frequency is not None
        assert edb.TimeSeriesType is not None

    def test_canonical_tree_construction(self):
        """Sebastian's canonical form: a full tree built from only ``edb.*``."""
        from shapely.geometry import Point

        tree = edb.Portfolio(name="P", members=[
            edb.Site(name="S", geometry=Point(3.0, 55.0), members=[
                edb.WindTurbine(name="T", capacity=3.5),
            ]),
        ])
        # Tree walks via the EDM abstraction energydb relies on
        assert tree.name == "P"
        assert tree.children()[0].name == "S"
        assert tree.children()[0].children()[0].name == "T"


# ---------------------------------------------------------------------------
# Tree walking
# ---------------------------------------------------------------------------


def _make_client_with_mock_td() -> EnergyDataClient:
    td = MagicMock()
    client = EnergyDataClient(td)
    return client


def _simple_df(n: int = 3) -> pl.DataFrame:
    """Tiny valid_time+value polars frame that ``TimeSeries`` will accept."""
    from datetime import datetime, timedelta, timezone
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    return pl.DataFrame({
        "valid_time": [base + timedelta(hours=i) for i in range(n)],
        "value": [float(i) for i in range(n)],
    })


class TestTreeWalk:
    def test_visits_every_node_with_correct_parent(self, monkeypatch):
        """_write_tree should call _create_node once per node, with the
        parent from the previous level."""
        client = _make_client_with_mock_td()

        calls: list[tuple[str, object]] = []
        counter = iter(range(1, 100))

        def fake_create_node(edm_obj, *, parent=None):
            nid = next(counter)
            calls.append((edm_obj.name, parent))
            return nid, []

        monkeypatch.setattr(client, "_create_node", fake_create_node)

        tree = edb.Portfolio(name="P", members=[
            edb.Site(name="S1", members=[
                edb.WindTurbine(name="T1", capacity=3.5),
                edb.WindTurbine(name="T2", capacity=3.5),
            ]),
            edb.Site(name="S2", members=[
                edb.Battery(name="B1", storage_capacity=10),
            ]),
        ])

        root_id = client.write(tree)

        # Root resolved first → node_id=1
        assert root_id == 1
        # DFS order; parent is the node_id of the preceding ancestor
        assert calls == [
            ("P", None),
            ("S1", 1),
            ("T1", 2),
            ("T2", 2),
            ("S2", 1),
            ("B1", 5),
        ]
        # No data attached → td.write never called
        client.td.write.assert_not_called()

    def test_descriptor_only_tree_writes_no_data(self, monkeypatch):
        client = _make_client_with_mock_td()
        monkeypatch.setattr(
            client, "_create_node",
            lambda edm_obj, *, parent=None: (42, []),
        )

        tree = edb.WindTurbine(name="T", capacity=3.5, timeseries=[
            edb.TimeSeriesDescriptor(name="power", unit="MW",
                                     data_type=edb.DataType.ACTUAL),
        ])
        client.write(tree)
        client.td.write.assert_not_called()

    def test_timeseries_with_data_triggers_single_bulk_write(self, monkeypatch):
        """Multiple TimeSeries across the tree should coalesce into one
        ``td.write`` call with a concatenated frame."""
        client = _make_client_with_mock_td()

        df_a = _simple_df(3)
        df_b = _simple_df(4)

        counter = iter(range(1, 100))
        series_counter = iter(range(100, 200))

        def fake_create_node(edm_obj, *, parent=None):
            nid = next(counter)
            pending = []
            for ts in getattr(edm_obj, "timeseries", None) or []:
                if isinstance(ts, edb.TimeSeries):
                    pending.append((next(series_counter), ts.df))
            return nid, pending

        monkeypatch.setattr(client, "_create_node", fake_create_node)

        tree = edb.Portfolio(name="P", members=[
            edb.WindTurbine(name="T1", capacity=3.5, timeseries=[
                edb.TimeSeries(df_a, name="power", unit="MW",
                               data_type=edb.DataType.ACTUAL),
            ]),
            edb.WindTurbine(name="T2", capacity=3.5, timeseries=[
                edb.TimeSeries(df_b, name="power", unit="MW",
                               data_type=edb.DataType.ACTUAL),
            ]),
        ])

        client.write(tree)

        assert client.td.write.call_count == 1
        written_df = client.td.write.call_args.args[0]
        # Rows from both dataframes should be present, tagged with series_id
        assert written_df.height == df_a.height + df_b.height
        assert set(written_df["series_id"].unique().to_list()) == {100, 101}
        assert client.td.write.call_args.kwargs["series_col"] == "series_id"

    def test_dispatch_dataframe_goes_to_manifest_write(self, monkeypatch):
        """A DataFrame argument must take the manifest path, not the tree path."""
        client = _make_client_with_mock_td()
        manifest_called = {"n": 0}

        def fake_manifest(df, **kw):
            manifest_called["n"] += 1
            return []

        def fake_tree(tree, **kw):  # pragma: no cover — must NOT be called
            raise AssertionError("DataFrame dispatched to tree write")

        monkeypatch.setattr(client, "_write_manifest", fake_manifest)
        monkeypatch.setattr(client, "_write_tree", fake_tree)

        manifest = pl.DataFrame({"node_id": [1], "name": ["x"],
                                 "data_type": ["actual"],
                                 "valid_time": [None], "value": [1.0]})
        client.write(manifest)
        assert manifest_called["n"] == 1

    def test_idempotent_rewalk(self, monkeypatch):
        """Re-running write() on the same tree should traverse it identically
        — the idempotency guarantee lives in _create_node (DB upsert), this
        test just checks the walk is deterministic."""
        client = _make_client_with_mock_td()
        seen: list[list[str]] = []

        def fake_create_node(edm_obj, *, parent=None):
            seen.append([edm_obj.name, parent])
            return 1, []

        monkeypatch.setattr(client, "_create_node", fake_create_node)
        tree = edb.Portfolio(name="P", members=[edb.WindTurbine(name="T", capacity=1)])

        client.write(tree)
        first = [list(x) for x in seen]
        seen.clear()
        client.write(tree)
        second = [list(x) for x in seen]
        assert first == second


class TestTwoPassTreeWalk:
    """Tests for the two-pass tree walk that handles Edges in members."""

    def test_edges_skipped_in_node_pass(self, monkeypatch):
        """Pass 1 should only create Nodes, skipping Edges."""
        client = _make_client_with_mock_td()

        node_calls: list[str] = []
        counter = iter(range(1, 100))

        def fake_create_node(edm_obj, *, parent=None):
            node_calls.append(edm_obj.name)
            return next(counter), []

        edge_calls: list[str] = []

        def fake_create_edge(edm_obj, *, from_node=None, to_node=None):
            edge_calls.append(edm_obj.name)
            return next(counter)

        monkeypatch.setattr(client, "_create_node", fake_create_node)
        monkeypatch.setattr(client, "create_edge", fake_create_edge)

        bus_a = edb.JunctionPoint(name="BusA")
        bus_b = edb.JunctionPoint(name="BusB")
        line = edb.Line(name="L1", capacity=500)

        tree = edb.Portfolio(name="Grid", members=[bus_a, bus_b, line])
        client.write(tree)

        # Only nodes should appear in node pass
        assert "Grid" in node_calls
        assert "BusA" in node_calls
        assert "BusB" in node_calls
        assert "L1" not in node_calls

        # Edge should appear in edge pass
        assert "L1" in edge_calls
