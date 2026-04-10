"""Tests for energydb serialization (EDM ↔ DB row conversion)."""

import pytest
from shapely.geometry import Point

import energydatamodel as edm
from energydatamodel.reference import Reference

from energydb.serialization import (
    _type_registry,
    reconstruct_edge,
    reconstruct_node,
    serialize_edge,
    serialize_node,
)


class TestTypeRegistry:
    def test_all_common_types_present(self):
        type_map = _type_registry()
        expected = [
            "Portfolio", "Site", "EnergyCommunity",
            "WindTurbine", "WindFarm", "PVSystem", "SolarPowerArea",
            "Battery", "HydroPowerPlant", "HeatPump", "PVArray",
            "Building", "House",
            # Areas:
            "BiddingZone", "SynchronousArea", "Country",
            # Grid nodes:
            "JunctionPoint", "Meter", "Interconnection",
            # Edges:
            "Line", "Link", "Transformer", "Pipe",
        ]
        for t in expected:
            assert t in type_map, f"Missing type: {t}"

    def test_values_are_classes(self):
        for name, cls in _type_registry().items():
            assert isinstance(cls, type), f"{name} maps to non-class: {cls}"


class TestSerializeNode:
    def test_wind_turbine(self):
        t = edm.WindTurbine(
            name="T01", capacity=3.5, hub_height=80,
            geometry=Point(3.0, 55.0),
        )
        data = serialize_node(t)
        assert data["node_type"] == "WindTurbine"
        assert data["name"] == "T01"
        assert data["latitude"] == 55.0
        assert data["longitude"] == 3.0
        # Properties should contain domain fields only
        props = data["properties"].obj  # unwrap Jsonb
        assert props["capacity"] == 3.5
        assert props["hub_height"] == 80
        assert "name" not in props
        assert "geometry" not in props

    def test_windfarm(self):
        t01 = edm.WindTurbine(name="T01", capacity=3.5)
        farm = edm.WindFarm(name="Lillgrund", capacity=110, members=[t01])
        data = serialize_node(farm)
        assert data["node_type"] == "WindFarm"
        assert data["name"] == "Lillgrund"
        props = data["properties"].obj
        assert props["capacity"] == 110
        assert "members" not in props  # children excluded

    def test_portfolio(self):
        p = edm.Portfolio(name="Europe")
        data = serialize_node(p)
        assert data["node_type"] == "Portfolio"
        assert data["name"] == "Europe"
        props = data["properties"].obj
        assert props == {}  # no domain-specific properties

    def test_site_with_geometry_point(self):
        s = edm.Site(name="Offshore-1", geometry=Point(3.0, 55.0))
        data = serialize_node(s)
        assert data["latitude"] == 55.0
        assert data["longitude"] == 3.0

    def test_battery(self):
        b = edm.Battery(name="B01", storage_capacity=100, min_soc=0.1)
        data = serialize_node(b)
        props = data["properties"].obj
        assert props["storage_capacity"] == 100
        assert props["min_soc"] == 0.1

    def test_none_base_fields(self):
        t = edm.WindTurbine(name="T01", capacity=3.5)
        data = serialize_node(t)
        assert data["latitude"] is None
        assert data["longitude"] is None
        assert data["altitude"] is None
        assert data["timezone"] is None

    def test_synchronous_area(self):
        nsa = edm.SynchronousArea(name="NSA", nominal_frequency=50.0)
        data = serialize_node(nsa)
        assert data["node_type"] == "SynchronousArea"
        assert data["name"] == "NSA"
        props = data["properties"].obj
        assert props["nominal_frequency"] == 50.0


class TestReconstructNode:
    def test_wind_turbine(self):
        row = {
            "node_type": "WindTurbine",
            "name": "T01",
            "properties": {"capacity": 3.5, "hub_height": 80},
            "latitude": 55.0,
            "longitude": 3.0,
            "altitude": None,
            "timezone": None,
        }
        obj = reconstruct_node(row)
        assert isinstance(obj, edm.WindTurbine)
        assert obj.name == "T01"
        assert obj.capacity == 3.5
        assert obj.hub_height == 80
        # Geometry rebuilt from lat/lon columns
        assert isinstance(obj.geometry, Point)
        assert obj.lat == 55.0
        assert obj.lon == 3.0

    def test_portfolio(self):
        row = {
            "node_type": "Portfolio",
            "name": "Europe",
            "properties": {},
            "latitude": None,
            "longitude": None,
            "altitude": None,
            "timezone": None,
        }
        obj = reconstruct_node(row)
        assert isinstance(obj, edm.Portfolio)
        assert obj.name == "Europe"
        assert obj.geometry is None

    def test_site(self):
        row = {
            "node_type": "Site",
            "name": "Sweden",
            "properties": {},
            "latitude": 55.0,
            "longitude": 13.0,
            "altitude": None,
            "timezone": None,
        }
        obj = reconstruct_node(row)
        assert isinstance(obj, edm.Site)
        assert obj.name == "Sweden"
        assert obj.lat == 55.0
        assert obj.lon == 13.0

    def test_unknown_type_raises(self):
        row = {
            "node_type": "UnknownAsset",
            "name": "X",
            "properties": {},
            "latitude": None, "longitude": None, "altitude": None, "timezone": None,
        }
        with pytest.raises(ValueError, match="Unknown node type"):
            reconstruct_node(row)

    def test_edge_type_raises(self):
        row = {
            "node_type": "Line",
            "name": "L1",
            "properties": {},
            "latitude": None, "longitude": None, "altitude": None, "timezone": None,
        }
        with pytest.raises(TypeError, match="not a Node subclass"):
            reconstruct_node(row)

    def test_synchronous_area(self):
        row = {
            "node_type": "SynchronousArea",
            "name": "NSA",
            "properties": {"nominal_frequency": 50.0},
            "latitude": None, "longitude": None, "altitude": None, "timezone": None,
        }
        obj = reconstruct_node(row)
        assert isinstance(obj, edm.SynchronousArea)
        assert obj.nominal_frequency == 50.0


class TestSerializeReconstructRoundTrip:
    """Verify that serialize → reconstruct preserves the essential data."""

    @pytest.mark.parametrize("edm_obj", [
        edm.WindTurbine(name="T01", capacity=3.5, hub_height=80),
        edm.Battery(name="B01", storage_capacity=100),
        edm.HeatPump(name="HP01", capacity=10, cop=3.0, energy_source="air"),
        edm.Portfolio(name="Europe"),
        edm.Site(name="Sweden", geometry=Point(13.0, 55.0)),
        edm.BiddingZone(name="SE-SE1"),
        edm.SynchronousArea(name="NSA", nominal_frequency=50.0),
    ])
    def test_round_trip(self, edm_obj):
        data = serialize_node(edm_obj)
        # Simulate what comes back from DB — unwrap Jsonb
        row = {
            "node_type": data["node_type"],
            "name": data["name"],
            "properties": data["properties"].obj,
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "altitude": data["altitude"],
            "timezone": data["timezone"],
        }
        reconstructed = reconstruct_node(row)
        assert type(reconstructed).__name__ == type(edm_obj).__name__
        assert reconstructed.name == edm_obj.name
        # Domain properties should match
        assert reconstructed.to_properties() == edm_obj.to_properties()


class TestSerializeEdge:
    def test_line(self):
        line = edm.Line(name="L1", capacity=500)
        data = serialize_edge(line)
        assert data["edge_type"] == "Line"
        assert data["name"] == "L1"
        assert data["directed"] is True
        props = data["properties"].obj
        assert props["capacity"] == 500

    def test_interconnection(self):
        ic = edm.Interconnection(
            name="IC-1",
            capacity_forward=1000,
            capacity_backward=500,
        )
        data = serialize_edge(ic)
        assert data["edge_type"] == "Interconnection"
        assert data["name"] == "IC-1"
        props = data["properties"].obj
        assert props["capacity_forward"] == 1000
        assert props["capacity_backward"] == 500

    def test_pipe(self):
        pipe = edm.Pipe(name="P1", capacity=200, medium="gas")
        data = serialize_edge(pipe)
        assert data["edge_type"] == "Pipe"
        props = data["properties"].obj
        assert props["capacity"] == 200
        assert props["medium"] == "gas"

    def test_undirected(self):
        link = edm.Link(name="LK1", capacity=100, directed=False)
        data = serialize_edge(link)
        assert data["directed"] is False


class TestReconstructEdge:
    def test_line_with_refs(self):
        row = {
            "edge_type": "Line",
            "name": "L1",
            "properties": {"capacity": 500},
            "directed": True,
            "from_node_path": "Europe/Sweden/BusA",
            "to_node_path": "Europe/Sweden/BusB",
        }
        obj = reconstruct_edge(row)
        assert isinstance(obj, edm.Line)
        assert obj.name == "L1"
        assert obj.capacity == 500
        assert obj.directed is True
        assert isinstance(obj.from_entity, Reference)
        assert obj.from_entity.target == "Europe/Sweden/BusA"
        assert isinstance(obj.to_entity, Reference)
        assert obj.to_entity.target == "Europe/Sweden/BusB"

    def test_unknown_type_raises(self):
        row = {
            "edge_type": "UnknownEdge",
            "name": "X",
            "properties": {},
            "directed": True,
            "from_node_path": "A",
            "to_node_path": "B",
        }
        with pytest.raises(ValueError, match="Unknown edge type"):
            reconstruct_edge(row)

    def test_node_type_raises(self):
        row = {
            "edge_type": "WindTurbine",
            "name": "T1",
            "properties": {},
            "directed": True,
            "from_node_path": "A",
            "to_node_path": "B",
        }
        with pytest.raises(TypeError, match="not an Edge subclass"):
            reconstruct_edge(row)


class TestEdgeRoundTrip:
    @pytest.mark.parametrize("edm_obj", [
        edm.Line(name="L1", capacity=500),
        edm.Link(name="LK1", capacity=100),
        edm.Transformer(name="T1", capacity=200),
        edm.Pipe(name="P1", capacity=300, medium="gas"),
        edm.Interconnection(name="IC1", capacity_forward=1000, capacity_backward=500),
    ])
    def test_round_trip(self, edm_obj):
        data = serialize_edge(edm_obj)
        row = {
            "edge_type": data["edge_type"],
            "name": data["name"],
            "properties": data["properties"].obj,
            "directed": data["directed"],
            "from_node_path": "A/B",
            "to_node_path": "C/D",
        }
        reconstructed = reconstruct_edge(row)
        assert type(reconstructed).__name__ == type(edm_obj).__name__
        assert reconstructed.name == edm_obj.name
        assert reconstructed.to_properties() == edm_obj.to_properties()
