"""Tests for energydb serialization (EDM ↔ DB row conversion).

After the UUID identity rewrite, every node row carries
``{"uuid", "node_type", "name", "data"}`` and every edge row carries
``{"uuid", "edge_type", "name", "data", "from_node_uuid", "to_node_uuid"}``.
Reconstruction populates ``Element.id`` from the row's uuid, and edge
endpoints come back as :class:`Reference` objects holding the endpoint
uuids directly — no path round-trip.
"""

from datetime import date
from zoneinfo import ZoneInfo

import energydatamodel as edm
import pytest
from energydatamodel.reference import Reference
from energydb.serialization import (
    _type_registry,
    reconstruct_edge,
    reconstruct_node,
    serialize_edge,
    serialize_node,
)
from shapely.geometry import Point, Polygon


def _node_row(serialized: dict) -> dict:
    """Simulate a DB row dict for a node: unwrap Jsonb so ``data`` is plain."""
    return {
        "uuid": serialized["uuid"],
        "node_type": serialized["node_type"],
        "name": serialized["name"],
        "data": serialized["data"].obj,
    }


def _edge_row(serialized: dict, *, from_uuid, to_uuid) -> dict:
    """Build a DB row dict for an edge; UUIDs come from the FK columns."""
    return {
        "uuid": serialized["uuid"],
        "edge_type": serialized["edge_type"],
        "name": serialized["name"],
        "data": serialized["data"].obj,
        "from_node_uuid": from_uuid,
        "to_node_uuid": to_uuid,
    }


class TestTypeRegistry:
    def test_all_common_types_present(self):
        type_map = _type_registry()
        expected = [
            "Portfolio",
            "Site",
            "EnergyCommunity",
            "WindTurbine",
            "WindFarm",
            "PVSystem",
            "SolarPowerArea",
            "Battery",
            "HydroPowerPlant",
            "HeatPump",
            "PVArray",
            "Building",
            "House",
            "BiddingZone",
            "SynchronousArea",
            "Country",
            "JunctionPoint",
            "Meter",
            "Transformer",
            "Line",
            "Link",
            "Interconnection",
            "Pipe",
        ]
        for t in expected:
            assert t in type_map, f"Missing type: {t}"

    def test_values_are_classes(self):
        for name, cls in _type_registry().items():
            assert isinstance(cls, type), f"{name} maps to non-class: {cls}"


class TestSerializeNode:
    def test_wind_turbine(self):
        t = edm.wind.WindTurbine(
            name="T01",
            capacity=3.5,
            hub_height=80,
            geometry=Point(3.0, 55.0),
        )
        row = serialize_node(t)
        assert row["uuid"] == t.id
        assert row["node_type"] == "WindTurbine"
        assert row["name"] == "T01"
        data = row["data"].obj
        assert data["capacity"] == 3.5
        assert data["hub_height"] == 80
        assert data["geometry"]["__geometry__"] is True
        assert data["geometry"]["type"] == "Point"
        # Coords are lists, not tuples, so the in-memory serialize output
        # compares equal to the JSONB read-back.
        assert data["geometry"]["coordinates"] == [3.0, 55.0]
        # Structural columns and ``id`` live as columns; not duplicated in data.
        assert "name" not in data
        assert "id" not in data
        assert "type" not in data

    def test_windfarm_excludes_members(self):
        t01 = edm.wind.WindTurbine(name="T01", capacity=3.5)
        farm = edm.wind.WindFarm(name="Lillgrund", capacity=110, members=[t01])
        row = serialize_node(farm)
        assert row["node_type"] == "WindFarm"
        assert row["name"] == "Lillgrund"
        data = row["data"].obj
        assert data["capacity"] == 110
        assert "members" not in data

    def test_portfolio_empty(self):
        p = edm.Portfolio(name="Europe")
        row = serialize_node(p)
        assert row["uuid"] == p.id
        assert row["node_type"] == "Portfolio"
        assert row["name"] == "Europe"
        assert row["data"].obj == {}

    def test_site_with_geometry(self):
        s = edm.Site(name="Offshore-1", geometry=Point(3.0, 55.0))
        data = serialize_node(s)["data"].obj
        assert data["geometry"]["coordinates"] == [3.0, 55.0]

    def test_battery(self):
        b = edm.battery.Battery(name="B01", storage_capacity=100, min_soc=0.1)
        data = serialize_node(b)["data"].obj
        assert data["storage_capacity"] == 100
        assert data["min_soc"] == 0.1

    def test_unset_fields_omitted(self):
        t = edm.wind.WindTurbine(name="T01", capacity=3.5)
        data = serialize_node(t)["data"].obj
        assert "geometry" not in data
        assert "commissioning_date" not in data

    def test_synchronous_area(self):
        nsa = edm.SynchronousArea(name="NSA", nominal_frequency=50.0)
        data = serialize_node(nsa)["data"].obj
        assert data["nominal_frequency"] == 50.0


class TestReconstructNode:
    def test_wind_turbine(self):
        original = edm.wind.WindTurbine(
            name="T01",
            capacity=3.5,
            hub_height=80,
            geometry=Point(3.0, 55.0),
        )
        obj = reconstruct_node(_node_row(serialize_node(original)))
        assert isinstance(obj, edm.wind.WindTurbine)
        assert obj.id == original.id
        assert obj.name == "T01"
        assert obj.capacity == 3.5
        assert obj.hub_height == 80
        assert isinstance(obj.geometry, Point)
        assert obj.latitude == 55.0
        assert obj.longitude == 3.0

    def test_portfolio(self):
        p = edm.Portfolio(name="Europe")
        obj = reconstruct_node(_node_row(serialize_node(p)))
        assert isinstance(obj, edm.Portfolio)
        assert obj.id == p.id
        assert obj.name == "Europe"
        assert obj.geometry is None

    def test_site(self):
        s = edm.Site(name="Sweden", geometry=Point(13.0, 55.0))
        obj = reconstruct_node(_node_row(serialize_node(s)))
        assert isinstance(obj, edm.Site)
        assert obj.name == "Sweden"
        assert obj.latitude == 55.0
        assert obj.longitude == 13.0

    def test_unknown_type_raises(self):
        from uuid import uuid4

        row = {"uuid": uuid4(), "node_type": "UnknownAsset", "name": "X", "data": {}}
        with pytest.raises(ValueError, match="Unknown node type"):
            reconstruct_node(row)

    def test_edge_type_raises(self):
        from uuid import uuid4

        row = {"uuid": uuid4(), "node_type": "Line", "name": "L1", "data": {}}
        with pytest.raises(TypeError, match="not a Node or Collection subclass"):
            reconstruct_node(row)

    def test_synchronous_area(self):
        nsa = edm.SynchronousArea(name="NSA", nominal_frequency=50.0)
        obj = reconstruct_node(_node_row(serialize_node(nsa)))
        assert isinstance(obj, edm.SynchronousArea)
        assert obj.nominal_frequency == 50.0


class TestSerializeReconstructRoundTrip:
    """Verify that serialize → reconstruct preserves data and identity."""

    @pytest.mark.parametrize(
        "edm_obj",
        [
            edm.wind.WindTurbine(name="T01", capacity=3.5, hub_height=80),
            edm.battery.Battery(name="B01", storage_capacity=100),
            edm.heatpump.HeatPump(name="HP01", capacity=10, cop=3.0, energy_source="air"),
            edm.Portfolio(name="Europe"),
            edm.Site(name="Sweden", geometry=Point(13.0, 55.0)),
            edm.BiddingZone(name="SE-SE1"),
            edm.SynchronousArea(name="NSA", nominal_frequency=50.0),
        ],
    )
    def test_round_trip(self, edm_obj):
        reconstructed = reconstruct_node(_node_row(serialize_node(edm_obj)))
        assert type(reconstructed).__name__ == type(edm_obj).__name__
        assert reconstructed.id == edm_obj.id
        assert reconstructed.name == edm_obj.name
        assert reconstructed.to_properties() == edm_obj.to_properties()


class TestSerializeEdge:
    def test_line(self):
        a = edm.grid.JunctionPoint(name="A")
        b = edm.grid.JunctionPoint(name="B")
        line = edm.grid.Line(name="L1", capacity=500, from_element=Reference(a), to_element=Reference(b))
        row = serialize_edge(line)
        assert row["uuid"] == line.id
        assert row["edge_type"] == "Line"
        assert row["name"] == "L1"
        data = row["data"].obj
        assert data["capacity"] == 500
        assert data["directed"] is True
        # FK columns hold the endpoints; not in `data`.
        assert "from_element" not in data
        assert "to_element" not in data
        assert "id" not in data

    def test_interconnection(self):
        a = edm.BiddingZone(name="A")
        b = edm.BiddingZone(name="B")
        ic = edm.grid.Interconnection(
            name="IC-1",
            capacity_forward=1000,
            capacity_backward=500,
            from_element=Reference(a),
            to_element=Reference(b),
        )
        data = serialize_edge(ic)["data"].obj
        assert data["capacity_forward"] == 1000
        assert data["capacity_backward"] == 500

    def test_pipe(self):
        a = edm.grid.JunctionPoint(name="A")
        b = edm.grid.JunctionPoint(name="B")
        pipe = edm.grid.Pipe(name="P1", capacity=200, medium="gas", from_element=Reference(a), to_element=Reference(b))
        data = serialize_edge(pipe)["data"].obj
        assert data["capacity"] == 200
        assert data["medium"] == "gas"

    def test_undirected(self):
        a = edm.grid.JunctionPoint(name="A")
        b = edm.grid.JunctionPoint(name="B")
        link = edm.grid.Link(
            name="LK1",
            capacity=100,
            directed=False,
            from_element=Reference(a),
            to_element=Reference(b),
        )
        data = serialize_edge(link)["data"].obj
        assert data["directed"] is False


class TestReconstructEdge:
    def test_line_with_uuid_endpoints(self):
        a = edm.grid.JunctionPoint(name="A")
        b = edm.grid.JunctionPoint(name="B")
        line = edm.grid.Line(name="L1", capacity=500, from_element=Reference(a), to_element=Reference(b))
        row = _edge_row(serialize_edge(line), from_uuid=a.id, to_uuid=b.id)
        obj = reconstruct_edge(row)
        assert isinstance(obj, edm.grid.Line)
        assert obj.id == line.id
        assert obj.name == "L1"
        assert obj.capacity == 500
        assert obj.directed is True
        assert isinstance(obj.from_element, Reference)
        assert obj.from_element.id == a.id
        assert isinstance(obj.to_element, Reference)
        assert obj.to_element.id == b.id

    def test_unknown_type_raises(self):
        from uuid import uuid4

        row = {
            "uuid": uuid4(),
            "edge_type": "UnknownEdge",
            "name": "X",
            "data": {},
            "from_node_uuid": uuid4(),
            "to_node_uuid": uuid4(),
        }
        with pytest.raises(ValueError, match="Unknown edge type"):
            reconstruct_edge(row)

    def test_node_type_raises(self):
        from uuid import uuid4

        row = {
            "uuid": uuid4(),
            "edge_type": "WindTurbine",
            "name": "T1",
            "data": {},
            "from_node_uuid": uuid4(),
            "to_node_uuid": uuid4(),
        }
        with pytest.raises(TypeError, match="not an Edge subclass"):
            reconstruct_edge(row)


class TestEdgeRoundTrip:
    @pytest.mark.parametrize(
        "edge_factory",
        [
            lambda a, b: edm.grid.Line(name="L1", capacity=500, from_element=Reference(a), to_element=Reference(b)),
            lambda a, b: edm.grid.Link(name="LK1", capacity=100, from_element=Reference(a), to_element=Reference(b)),
            lambda a, b: edm.grid.Pipe(
                name="P1", capacity=300, medium="gas", from_element=Reference(a), to_element=Reference(b)
            ),
            lambda a, b: edm.grid.Interconnection(
                name="IC1",
                capacity_forward=1000,
                capacity_backward=500,
                from_element=Reference(a),
                to_element=Reference(b),
            ),
        ],
    )
    def test_round_trip(self, edge_factory):
        a = edm.grid.JunctionPoint(name="A")
        b = edm.grid.JunctionPoint(name="B")
        edge = edge_factory(a, b)
        row = _edge_row(serialize_edge(edge), from_uuid=a.id, to_uuid=b.id)
        reconstructed = reconstruct_edge(row)
        assert type(reconstructed).__name__ == type(edge).__name__
        assert reconstructed.id == edge.id
        assert reconstructed.name == edge.name
        assert reconstructed.to_properties() == edge.to_properties()


# ---------------------------------------------------------------------------
# Tree round-trip via serialize → reconstruct → add_child
# ---------------------------------------------------------------------------


def _collect_nodes(obj, parent, out):
    """Depth-first walk — skip Edges; yield (obj, parent_obj)."""
    out.append((obj, parent))
    for child in obj.children():
        if isinstance(child, edm.Edge):
            continue
        _collect_nodes(child, obj, out)


def _collect_edges(obj, out):
    for child in obj.children():
        if isinstance(child, edm.Edge):
            out.append((child, obj))
        else:
            _collect_edges(child, out)


def _roundtrip_tree(root):
    """Serialize every node, reconstruct, and rebuild the tree via add_child.

    Returns the rebuilt root. UUIDs round-trip through ``uuid`` columns; edge
    endpoints round-trip through ``from_node_uuid`` / ``to_node_uuid``.
    """
    node_pairs: list = []
    _collect_nodes(root, None, node_pairs)

    rebuilt: dict = {}
    for obj, _parent in node_pairs:
        rebuilt[obj.id] = reconstruct_node(_node_row(serialize_node(obj)))

    rebuilt_root = rebuilt[root.id]
    for obj, parent in node_pairs:
        if parent is None:
            continue
        rebuilt[parent.id].add_child(rebuilt[obj.id])

    edge_pairs: list = []
    _collect_edges(root, edge_pairs)
    for edge, parent in edge_pairs:
        new_edge = reconstruct_edge(
            _edge_row(serialize_edge(edge), from_uuid=edge.from_element.id, to_uuid=edge.to_element.id)
        )
        rebuilt[parent.id].add_child(new_edge)

    return rebuilt_root


class TestComplexTreeRoundTrip:
    """End-to-end tree round-trip: serialize → reconstruct → add_child."""

    def test_portfolio_with_sites_and_assets(self):
        original = edm.Portfolio(
            name="Europe",
            members=[
                edm.Site(
                    name="Offshore-1",
                    geometry=Point(3.0, 55.0),
                    members=[
                        edm.wind.WindTurbine(name="T01", capacity=3.5, hub_height=80),
                        edm.wind.WindTurbine(name="T02", capacity=3.5, hub_height=90),
                    ],
                ),
                edm.Site(
                    name="Rooftop-1",
                    geometry=Point(4.5, 52.0),
                    members=[
                        edm.solar.PVSystem(
                            name="PV01",
                            capacity=10,
                            surface_tilt=25,
                            surface_azimuth=180,
                        ),
                        edm.battery.Battery(name="B01", storage_capacity=100),
                    ],
                ),
            ],
        )

        rebuilt = _roundtrip_tree(original)

        assert isinstance(rebuilt, edm.Portfolio)
        assert rebuilt.id == original.id
        assert rebuilt.name == "Europe"
        assert len(rebuilt.members) == 2

        site1, site2 = rebuilt.members
        assert isinstance(site1, edm.Site)
        assert site1.name == "Offshore-1"
        assert site1.latitude == 55.0 and site1.longitude == 3.0
        assert len(site1.members) == 2
        assert all(isinstance(m, edm.wind.WindTurbine) for m in site1.members)

        assert isinstance(site2, edm.Site)
        assert isinstance(site2.members[0], edm.solar.PVSystem)
        assert isinstance(site2.members[1], edm.battery.Battery)

    def test_windfarm_with_nested_turbines(self):
        original = edm.wind.WindFarm(
            name="Lillgrund",
            capacity=110,
            members=[
                edm.wind.WindTurbine(name="T01", capacity=2.3),
                edm.wind.WindTurbine(name="T02", capacity=2.3),
                edm.wind.WindTurbine(name="T03", capacity=2.3),
            ],
        )
        rebuilt = _roundtrip_tree(original)

        assert isinstance(rebuilt, edm.wind.WindFarm)
        assert rebuilt.id == original.id
        assert rebuilt.capacity == 110
        assert len(rebuilt.members) == 3
        for t in rebuilt.members:
            assert isinstance(t, edm.wind.WindTurbine)
            assert t.capacity == 2.3

    def test_mixed_collection_and_node_children(self):
        original = edm.Portfolio(
            name="Nordic",
            members=[
                edm.BiddingZone(name="SE4"),
                edm.SynchronousArea(name="Nordic-Sync", nominal_frequency=50.0),
                edm.Site(
                    name="Lillgrund-Site",
                    members=[
                        edm.wind.WindFarm(
                            name="Lillgrund",
                            capacity=110,
                            members=[edm.wind.WindTurbine(name="T01", capacity=2.3)],
                        ),
                        edm.weather.TemperatureSensor(name="TempSensor-1", height=10),
                    ],
                ),
            ],
        )
        rebuilt = _roundtrip_tree(original)

        assert isinstance(rebuilt, edm.Portfolio)
        assert [type(m).__name__ for m in rebuilt.members] == [
            "BiddingZone",
            "SynchronousArea",
            "Site",
        ]
        sync_area = rebuilt.members[1]
        assert sync_area.nominal_frequency == 50.0

        site = rebuilt.members[2]
        assert len(site.members) == 2
        wf = site.members[0]
        assert isinstance(wf, edm.wind.WindFarm)
        assert wf.members[0].name == "T01"
        sensor = site.members[1]
        assert isinstance(sensor, edm.weather.TemperatureSensor)
        assert sensor.height == 10

    def test_tree_with_grid_edges(self):
        bus_a = edm.grid.JunctionPoint(name="BusA")
        bus_b = edm.grid.JunctionPoint(name="BusB")
        line = edm.grid.Line(
            name="Cable-1",
            capacity=500,
            from_element=Reference(bus_a),
            to_element=Reference(bus_b),
        )
        original = edm.Portfolio(
            name="Europe",
            members=[edm.Site(name="Offshore-1", members=[bus_a, bus_b, line])],
        )

        rebuilt = _roundtrip_tree(original)

        site = rebuilt.members[0]
        types = [type(c).__name__ for c in site.children()]
        assert types.count("JunctionPoint") == 2
        assert types.count("Line") == 1

        rebuilt_line = next(c for c in site.children() if isinstance(c, edm.grid.Line))
        assert rebuilt_line.capacity == 500
        assert isinstance(rebuilt_line.from_element, Reference)
        assert rebuilt_line.from_element.id == bus_a.id
        assert rebuilt_line.to_element.id == bus_b.id

    def test_geometry_round_trip(self):
        """Point geometries (2D + 3D) survive GeoJSON round-trip."""
        original = edm.Portfolio(
            name="Europe",
            members=[
                edm.Site(name="A", geometry=Point(10.0, 60.0)),
                edm.Site(name="B", geometry=Point(11.0, 61.0, 50.0)),
            ],
        )
        rebuilt = _roundtrip_tree(original)

        a, b = rebuilt.members
        assert a.geometry.x == 10.0 and a.geometry.y == 60.0
        assert not a.geometry.has_z
        assert b.geometry.has_z
        assert b.geometry.z == 50.0

    def test_to_properties_preserved_for_every_node(self):
        original = edm.Portfolio(
            name="Europe",
            members=[
                edm.Site(
                    name="Offshore-1",
                    members=[
                        edm.wind.WindTurbine(
                            name="T01",
                            capacity=3.5,
                            hub_height=80,
                            rotor_diameter=120,
                        ),
                        edm.battery.Battery(
                            name="B01",
                            storage_capacity=1000,
                            max_charge=500,
                            max_discharge=500,
                        ),
                    ],
                ),
            ],
        )
        rebuilt = _roundtrip_tree(original)

        def flatten(obj, out):
            out.append(obj)
            for child in obj.children():
                if isinstance(child, edm.Edge):
                    continue
                flatten(child, out)

        orig_list, new_list = [], []
        flatten(original, orig_list)
        flatten(rebuilt, new_list)

        assert len(orig_list) == len(new_list)
        for o, n in zip(orig_list, new_list, strict=True):
            assert type(o) is type(n)
            assert o.id == n.id
            assert o.name == n.name
            assert o.to_properties() == n.to_properties()

    def test_deeply_nested_tree(self):
        original = edm.Portfolio(
            name="Global",
            members=[
                edm.Country(
                    name="SE",
                    members=[
                        edm.BiddingZone(
                            name="SE3",
                            members=[
                                edm.Site(
                                    name="LillgrundSite",
                                    members=[
                                        edm.wind.WindFarm(
                                            name="Lillgrund",
                                            capacity=110,
                                            members=[
                                                edm.wind.WindTurbine(name="T01", capacity=2.3, hub_height=65),
                                                edm.wind.WindTurbine(name="T02", capacity=2.3, hub_height=65),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        )
        rebuilt = _roundtrip_tree(original)

        country = rebuilt.members[0]
        bz = country.members[0]
        site = bz.members[0]
        wf = site.members[0]
        assert isinstance(country, edm.Country)
        assert isinstance(bz, edm.BiddingZone)
        assert isinstance(site, edm.Site)
        assert isinstance(wf, edm.wind.WindFarm)
        assert wf.capacity == 110
        assert len(wf.members) == 2

    def test_multiple_edge_types_in_one_tree(self):
        a = edm.grid.JunctionPoint(name="A")
        b = edm.grid.JunctionPoint(name="B")
        c = edm.grid.JunctionPoint(name="C")
        d = edm.grid.JunctionPoint(name="D")
        trafo = edm.grid.Transformer(name="CD-Tx", capacity=200, voltage_hv=220.0, voltage_lv=110.0)
        line = edm.grid.Line(
            name="AB-Line",
            capacity=500,
            from_element=Reference(a),
            to_element=Reference(b),
        )
        link = edm.grid.Link(
            name="BC-Link",
            capacity=100,
            directed=False,
            from_element=Reference(b),
            to_element=Reference(c),
        )
        pipe = edm.grid.Pipe(
            name="AD-Pipe",
            capacity=300,
            medium="gas",
            from_element=Reference(a),
            to_element=Reference(d),
        )
        original = edm.Portfolio(name="Grid", members=[a, b, c, d, trafo, line, link, pipe])

        rebuilt = _roundtrip_tree(original)
        edges = [c for c in rebuilt.children() if isinstance(c, edm.Edge)]
        nodes = [c for c in rebuilt.children() if isinstance(c, edm.Node)]
        assert len(edges) == 3
        assert any(isinstance(n, edm.grid.Transformer) for n in nodes)

        by_name = {e.name: e for e in edges}
        assert isinstance(by_name["AB-Line"], edm.grid.Line)
        assert by_name["AB-Line"].directed is True
        assert isinstance(by_name["BC-Link"], edm.grid.Link)
        assert by_name["BC-Link"].directed is False
        assert isinstance(by_name["AD-Pipe"], edm.grid.Pipe)
        assert by_name["AD-Pipe"].medium == "gas"

        rebuilt_trafo = next(n for n in nodes if isinstance(n, edm.grid.Transformer))
        assert rebuilt_trafo.voltage_hv == 220.0
        assert rebuilt_trafo.voltage_lv == 110.0

    def test_edges_across_subtrees(self):
        bus_a = edm.grid.JunctionPoint(name="BusA")
        bus_b = edm.grid.JunctionPoint(name="BusB")
        cross = edm.grid.Line(
            name="CrossLink",
            capacity=800,
            from_element=Reference(bus_a),
            to_element=Reference(bus_b),
        )
        original = edm.Portfolio(
            name="Nordic",
            members=[
                edm.Site(name="SiteA", members=[bus_a]),
                edm.Site(name="SiteB", members=[bus_b, cross]),
            ],
        )
        rebuilt = _roundtrip_tree(original)

        site_b = rebuilt.members[1]
        line = next(c for c in site_b.children() if isinstance(c, edm.grid.Line))
        assert line.from_element.id == bus_a.id
        assert line.to_element.id == bus_b.id
        assert line.capacity == 800

    def test_energy_community_buildings_houses(self):
        original = edm.EnergyCommunity(
            name="EC-1",
            members=[
                edm.building.Building(
                    name="BuildingA",
                    type="commercial",
                    members=[
                        edm.building.House(
                            name="Unit-1",
                            type="apartment",
                            members=[
                                edm.heatpump.HeatPump(name="HP1", capacity=10, cop=3.5, energy_source="air"),
                                edm.solar.PVArray(name="PVA1", capacity=5, surface_tilt=30),
                                edm.battery.Battery(name="Bat1", storage_capacity=20),
                            ],
                        ),
                    ],
                ),
            ],
        )
        rebuilt = _roundtrip_tree(original)

        assert isinstance(rebuilt, edm.EnergyCommunity)
        building = rebuilt.members[0]
        house = building.members[0]
        assert isinstance(building, edm.building.Building)
        assert building.type == "commercial"
        assert isinstance(house, edm.building.House)
        assert house.type == "apartment"
        assert len(house.members) == 3

    def test_interconnection_between_countries(self):
        de = edm.Country(name="DE")
        fr = edm.Country(name="FR")
        ic = edm.grid.Interconnection(
            name="DE-FR",
            capacity_forward=1500,
            capacity_backward=1200,
            from_element=Reference(de),
            to_element=Reference(fr),
        )
        original = edm.Portfolio(name="EU", members=[de, fr, ic])
        rebuilt = _roundtrip_tree(original)

        rebuilt_ic = next(c for c in rebuilt.children() if isinstance(c, edm.grid.Interconnection))
        assert rebuilt_ic.capacity_forward == 1500
        assert rebuilt_ic.capacity_backward == 1200
        assert rebuilt_ic.from_element.id == de.id
        assert rebuilt_ic.to_element.id == fr.id

    def test_round_trip_idempotence(self):
        tree = edm.Portfolio(
            name="Europe",
            members=[
                edm.Site(
                    name="Offshore-1",
                    geometry=Point(3.0, 55.0),
                    members=[
                        edm.wind.WindTurbine(name="T01", capacity=3.5, hub_height=80),
                        edm.battery.Battery(name="B01", storage_capacity=100, max_charge=50),
                    ],
                ),
            ],
        )

        def all_nodes(obj, out):
            out.append(obj)
            for child in obj.children():
                if isinstance(child, edm.Edge):
                    continue
                all_nodes(child, out)

        orig_nodes: list = []
        all_nodes(tree, orig_nodes)
        first_pass = [_node_row(serialize_node(n)) for n in orig_nodes]

        rebuilt = _roundtrip_tree(tree)
        rebuilt_nodes: list = []
        all_nodes(rebuilt, rebuilt_nodes)
        second_pass = [_node_row(serialize_node(n)) for n in rebuilt_nodes]

        assert first_pass == second_pass


# ---------------------------------------------------------------------------
# Previously-broken fields now round-trip
# ---------------------------------------------------------------------------


class TestPreviouslyDroppedFields:
    """Fields that silently dropped pre-refactor now round-trip correctly."""

    def test_polygon_geometry(self):
        poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
        site = edm.Site(name="Area", geometry=poly)
        rebuilt = reconstruct_node(_node_row(serialize_node(site)))
        assert isinstance(rebuilt.geometry, Polygon)
        assert rebuilt.geometry.equals(poly)

    def test_zoneinfo_timezone(self):
        site = edm.Site(name="Stockholm", tz=ZoneInfo("Europe/Stockholm"))
        rebuilt = reconstruct_node(_node_row(serialize_node(site)))
        assert isinstance(rebuilt.tz, ZoneInfo)
        assert str(rebuilt.tz) == "Europe/Stockholm"

    def test_commissioning_date(self):
        t = edm.wind.WindTurbine(
            name="T01",
            capacity=3.5,
            commissioning_date=date(2020, 1, 15),
        )
        rebuilt = reconstruct_node(_node_row(serialize_node(t)))
        assert rebuilt.commissioning_date == date(2020, 1, 15)

    def test_sensor_height(self):
        s = edm.weather.TemperatureSensor(name="Temp-80m", height=80.0)
        rebuilt = reconstruct_node(_node_row(serialize_node(s)))
        assert isinstance(rebuilt, edm.weather.TemperatureSensor)
        assert rebuilt.height == 80.0


# ---------------------------------------------------------------------------
# Exhaustive EDM registry coverage
# ---------------------------------------------------------------------------


_ABSTRACT_OR_UNION = {
    "Node",
    "Edge",
    "Collection",
    "Area",
    "Asset",
    "EdgeAsset",
    "NodeAsset",
    "GridNode",
    "Sensor",
}


def _concrete_classes(kind: str):
    registry = _type_registry()
    out = []
    for name, cls in sorted(registry.items()):
        if name in _ABSTRACT_OR_UNION:
            continue
        if (
            kind == "edge"
            and issubclass(cls, edm.Edge)
            or kind == "collection"
            and issubclass(cls, edm.Collection)
            and not issubclass(cls, edm.Edge)
            or kind == "node"
            and issubclass(cls, edm.Node)
            and not issubclass(cls, edm.Edge)
        ):
            out.append((name, cls))
    return out


class TestAllEDMClassesRoundTrip:
    """Every concrete class in the EDM registry survives node-level round trip."""

    @pytest.mark.parametrize(
        "name,cls",
        _concrete_classes("node") + _concrete_classes("collection"),
        ids=lambda v: v if isinstance(v, str) else v.__name__,
    )
    def test_node_class(self, name, cls):
        obj = cls(name=f"{name}-instance")
        rebuilt = reconstruct_node(_node_row(serialize_node(obj)))
        assert type(rebuilt) is cls
        assert rebuilt.id == obj.id
        assert rebuilt.name == obj.name
        assert rebuilt.to_properties() == obj.to_properties()

    @pytest.mark.parametrize(
        "name,cls",
        _concrete_classes("edge"),
        ids=lambda v: v if isinstance(v, str) else v.__name__,
    )
    def test_edge_class(self, name, cls):
        a = edm.grid.JunctionPoint(name="A")
        b = edm.grid.JunctionPoint(name="B")
        obj = cls(name=f"{name}-instance", from_element=Reference(a), to_element=Reference(b))
        row = _edge_row(serialize_edge(obj), from_uuid=a.id, to_uuid=b.id)
        rebuilt = reconstruct_edge(row)
        assert type(rebuilt) is cls
        assert rebuilt.id == obj.id
        assert rebuilt.name == obj.name
        assert rebuilt.to_properties() == obj.to_properties()
