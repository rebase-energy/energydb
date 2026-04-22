"""Tests for energydb serialization (EDM ↔ DB row conversion).

Row shape: ``{"node_type": str, "name": str, "data": dict}`` for nodes (plus
``parent_id`` / DB-only columns, ignored by reconstruction). Edges add
``from_node_path`` and ``to_node_path`` for Reference resolution.
"""

from datetime import date
from zoneinfo import ZoneInfo

import pytest
from shapely.geometry import Point, Polygon

import energydatamodel as edm
from energydatamodel.reference import Reference

from energydb.serialization import (
    _type_registry,
    reconstruct_edge,
    reconstruct_node,
    serialize_edge,
    serialize_node,
)


def _node_row(serialized: dict) -> dict:
    """Simulate a DB row dict: unwrap Jsonb so ``data`` is a plain dict."""
    return {
        "node_type": serialized["node_type"],
        "name": serialized["name"],
        "data": serialized["data"].obj,
    }


def _edge_row(serialized: dict, *, from_path: str, to_path: str) -> dict:
    return {
        "edge_type": serialized["edge_type"],
        "name": serialized["name"],
        "data": serialized["data"].obj,
        "from_node_path": from_path,
        "to_node_path": to_path,
    }


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
        row = serialize_node(t)
        assert row["node_type"] == "WindTurbine"
        assert row["name"] == "T01"
        data = row["data"].obj
        assert data["capacity"] == 3.5
        assert data["hub_height"] == 80
        # Geometry carried as GeoJSON under the ``__geometry__`` tag.
        assert data["geometry"]["__geometry__"] is True
        assert data["geometry"]["type"] == "Point"
        assert data["geometry"]["coordinates"] == (3.0, 55.0)
        # ``name`` and ``type`` live at top level, not in ``data``.
        assert "name" not in data
        assert "type" not in data

    def test_windfarm_excludes_members(self):
        t01 = edm.WindTurbine(name="T01", capacity=3.5)
        farm = edm.WindFarm(name="Lillgrund", capacity=110, members=[t01])
        row = serialize_node(farm)
        assert row["node_type"] == "WindFarm"
        assert row["name"] == "Lillgrund"
        data = row["data"].obj
        assert data["capacity"] == 110
        assert "members" not in data  # children excluded

    def test_portfolio_empty(self):
        p = edm.Portfolio(name="Europe")
        row = serialize_node(p)
        assert row["node_type"] == "Portfolio"
        assert row["name"] == "Europe"
        assert row["data"].obj == {}

    def test_site_with_geometry(self):
        s = edm.Site(name="Offshore-1", geometry=Point(3.0, 55.0))
        data = serialize_node(s)["data"].obj
        assert data["geometry"]["coordinates"] == (3.0, 55.0)

    def test_battery(self):
        b = edm.Battery(name="B01", storage_capacity=100, min_soc=0.1)
        data = serialize_node(b)["data"].obj
        assert data["storage_capacity"] == 100
        assert data["min_soc"] == 0.1

    def test_unset_fields_omitted(self):
        t = edm.WindTurbine(name="T01", capacity=3.5)
        data = serialize_node(t)["data"].obj
        # None values aren't serialized — no geometry, no commissioning_date.
        assert "geometry" not in data
        assert "commissioning_date" not in data

    def test_synchronous_area(self):
        nsa = edm.SynchronousArea(name="NSA", nominal_frequency=50.0)
        data = serialize_node(nsa)["data"].obj
        assert data["nominal_frequency"] == 50.0


class TestReconstructNode:
    def test_wind_turbine(self):
        row = {
            "node_type": "WindTurbine",
            "name": "T01",
            "data": {
                "capacity": 3.5, "hub_height": 80,
                "geometry": {"__geometry__": True, "type": "Point", "coordinates": [3.0, 55.0]},
            },
        }
        obj = reconstruct_node(row)
        assert isinstance(obj, edm.WindTurbine)
        assert obj.name == "T01"
        assert obj.capacity == 3.5
        assert obj.hub_height == 80
        assert isinstance(obj.geometry, Point)
        assert obj.lat == 55.0
        assert obj.lon == 3.0

    def test_portfolio(self):
        row = {"node_type": "Portfolio", "name": "Europe", "data": {}}
        obj = reconstruct_node(row)
        assert isinstance(obj, edm.Portfolio)
        assert obj.name == "Europe"
        assert obj.geometry is None

    def test_site(self):
        row = {
            "node_type": "Site",
            "name": "Sweden",
            "data": {
                "geometry": {"__geometry__": True, "type": "Point", "coordinates": [13.0, 55.0]},
            },
        }
        obj = reconstruct_node(row)
        assert isinstance(obj, edm.Site)
        assert obj.name == "Sweden"
        assert obj.lat == 55.0
        assert obj.lon == 13.0

    def test_unknown_type_raises(self):
        row = {"node_type": "UnknownAsset", "name": "X", "data": {}}
        with pytest.raises(ValueError, match="Unknown node type"):
            reconstruct_node(row)

    def test_edge_type_raises(self):
        row = {"node_type": "Line", "name": "L1", "data": {}}
        with pytest.raises(TypeError, match="not a Node or Collection subclass"):
            reconstruct_node(row)

    def test_synchronous_area(self):
        row = {
            "node_type": "SynchronousArea",
            "name": "NSA",
            "data": {"nominal_frequency": 50.0},
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
        reconstructed = reconstruct_node(_node_row(serialize_node(edm_obj)))
        assert type(reconstructed).__name__ == type(edm_obj).__name__
        assert reconstructed.name == edm_obj.name
        assert reconstructed.to_properties() == edm_obj.to_properties()


class TestSerializeEdge:
    def test_line(self):
        line = edm.Line(name="L1", capacity=500)
        row = serialize_edge(line)
        assert row["edge_type"] == "Line"
        assert row["name"] == "L1"
        data = row["data"].obj
        assert data["capacity"] == 500
        assert data["directed"] is True
        # from_entity/to_entity are excluded from data — they become FK columns.
        assert "from_entity" not in data
        assert "to_entity" not in data

    def test_interconnection(self):
        ic = edm.Interconnection(
            name="IC-1",
            capacity_forward=1000,
            capacity_backward=500,
        )
        data = serialize_edge(ic)["data"].obj
        assert data["capacity_forward"] == 1000
        assert data["capacity_backward"] == 500

    def test_pipe(self):
        pipe = edm.Pipe(name="P1", capacity=200, medium="gas")
        data = serialize_edge(pipe)["data"].obj
        assert data["capacity"] == 200
        assert data["medium"] == "gas"

    def test_undirected(self):
        link = edm.Link(name="LK1", capacity=100, directed=False)
        data = serialize_edge(link)["data"].obj
        assert data["directed"] is False


class TestReconstructEdge:
    def test_line_with_refs(self):
        row = {
            "edge_type": "Line",
            "name": "L1",
            "data": {"capacity": 500, "directed": True},
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
            "data": {},
            "from_node_path": "A",
            "to_node_path": "B",
        }
        with pytest.raises(ValueError, match="Unknown edge type"):
            reconstruct_edge(row)

    def test_node_type_raises(self):
        row = {
            "edge_type": "WindTurbine",
            "name": "T1",
            "data": {},
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
        row = _edge_row(serialize_edge(edm_obj), from_path="A/B", to_path="C/D")
        reconstructed = reconstruct_edge(row)
        assert type(reconstructed).__name__ == type(edm_obj).__name__
        assert reconstructed.name == edm_obj.name
        assert reconstructed.to_properties() == edm_obj.to_properties()


# ---------------------------------------------------------------------------
# Complex tree round-trip
# ---------------------------------------------------------------------------
#
# Simulates the full ``write(tree)`` -> ``get_tree()`` cycle in memory: every
# node is serialized, the dicts stand in for DB rows, each is reconstructed,
# and the tree is rebuilt via ``add_child`` — the same path
# :meth:`EnergyDataClient.get_tree` uses. Edges are handled in a second pass
# so ``from_entity``/``to_entity`` Reference paths resolve against the rebuilt
# subtree, mirroring the two-pass write.


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


def _build_path_map(root):
    """Walk the tree and return ``{id(node): "root/…/name"}`` for every node."""
    paths: dict[int, str] = {}

    def _walk(obj, parent_path):
        path = f"{parent_path}/{obj.name}" if parent_path else obj.name
        paths[id(obj)] = path
        for child in obj.children():
            if isinstance(child, edm.Edge):
                continue
            _walk(child, path)

    _walk(root, "")
    return paths


def _roundtrip_tree(root):
    """Serialize every node, reconstruct, and rebuild the tree via add_child.

    Returns the rebuilt root. Mirrors what ``client._write_tree`` /
    ``client.get_tree`` do, minus the DB.
    """
    node_pairs: list = []
    _collect_nodes(root, None, node_pairs)

    rebuilt: dict[int, edm.Element] = {}
    for obj, _parent in node_pairs:
        rebuilt[id(obj)] = reconstruct_node(_node_row(serialize_node(obj)))

    rebuilt_root = rebuilt[id(root)]
    for obj, parent in node_pairs:
        if parent is None:
            continue
        rebuilt[id(parent)].add_child(rebuilt[id(obj)])

    # Pass 2: edges — use each edge's current ``from_entity.target`` path
    # string as the stand-in for what the DB would return after resolving FKs.
    edge_pairs: list = []
    _collect_edges(root, edge_pairs)
    for edge, parent in edge_pairs:
        from_ref = edge.from_entity
        to_ref = edge.to_entity
        from_path = from_ref.target if isinstance(from_ref, Reference) else from_ref
        to_path = to_ref.target if isinstance(to_ref, Reference) else to_ref
        new_edge = reconstruct_edge(
            _edge_row(serialize_edge(edge), from_path=from_path, to_path=to_path)
        )
        rebuilt[id(parent)].add_child(new_edge)

    return rebuilt_root


class TestComplexTreeRoundTrip:
    """End-to-end tree round-trip: serialize → reconstruct → add_child."""

    def test_portfolio_with_sites_and_assets(self):
        original = edm.Portfolio(name="Europe", members=[
            edm.Site(name="Offshore-1", geometry=Point(3.0, 55.0), members=[
                edm.WindTurbine(name="T01", capacity=3.5, hub_height=80),
                edm.WindTurbine(name="T02", capacity=3.5, hub_height=90),
            ]),
            edm.Site(name="Rooftop-1", geometry=Point(4.5, 52.0), members=[
                edm.PVSystem(
                    name="PV01", capacity=10,
                    surface_tilt=25, surface_azimuth=180,
                ),
                edm.Battery(name="B01", storage_capacity=100),
            ]),
        ])

        rebuilt = _roundtrip_tree(original)

        assert isinstance(rebuilt, edm.Portfolio)
        assert rebuilt.name == "Europe"
        assert len(rebuilt.members) == 2

        site1, site2 = rebuilt.members
        assert isinstance(site1, edm.Site)
        assert site1.name == "Offshore-1"
        assert site1.lat == 55.0 and site1.lon == 3.0
        assert len(site1.members) == 2
        assert all(isinstance(m, edm.WindTurbine) for m in site1.members)
        assert site1.members[0].capacity == 3.5
        assert site1.members[0].hub_height == 80
        assert site1.members[1].hub_height == 90

        assert isinstance(site2, edm.Site)
        assert isinstance(site2.members[0], edm.PVSystem)
        assert site2.members[0].surface_tilt == 25
        assert isinstance(site2.members[1], edm.Battery)
        assert site2.members[1].storage_capacity == 100

    def test_windfarm_with_nested_turbines(self):
        original = edm.WindFarm(name="Lillgrund", capacity=110, members=[
            edm.WindTurbine(name="T01", capacity=2.3),
            edm.WindTurbine(name="T02", capacity=2.3),
            edm.WindTurbine(name="T03", capacity=2.3),
        ])
        rebuilt = _roundtrip_tree(original)

        assert isinstance(rebuilt, edm.WindFarm)
        assert rebuilt.capacity == 110
        assert len(rebuilt.members) == 3
        for t in rebuilt.members:
            assert isinstance(t, edm.WindTurbine)
            assert t.capacity == 2.3

    def test_mixed_collection_and_node_children(self):
        original = edm.Portfolio(name="Nordic", members=[
            edm.BiddingZone(name="SE4"),
            edm.SynchronousArea(name="Nordic-Sync", nominal_frequency=50.0),
            edm.Site(name="Lillgrund-Site", members=[
                edm.WindFarm(name="Lillgrund", capacity=110, members=[
                    edm.WindTurbine(name="T01", capacity=2.3),
                ]),
                edm.TemperatureSensor(name="TempSensor-1", height=10),
            ]),
        ])
        rebuilt = _roundtrip_tree(original)

        assert isinstance(rebuilt, edm.Portfolio)
        assert [type(m).__name__ for m in rebuilt.members] == [
            "BiddingZone", "SynchronousArea", "Site",
        ]
        sync_area = rebuilt.members[1]
        assert sync_area.nominal_frequency == 50.0

        site = rebuilt.members[2]
        assert len(site.members) == 2
        wf = site.members[0]
        assert isinstance(wf, edm.WindFarm)
        assert wf.members[0].name == "T01"
        sensor = site.members[1]
        assert isinstance(sensor, edm.TemperatureSensor)
        # Sensor.height now round-trips through ``data``.
        assert sensor.height == 10

    def test_tree_with_grid_edges(self):
        bus_a = edm.JunctionPoint(name="BusA")
        bus_b = edm.JunctionPoint(name="BusB")
        line = edm.Line(
            name="Cable-1", capacity=500,
            from_entity=Reference("Europe/Offshore-1/BusA"),
            to_entity=Reference("Europe/Offshore-1/BusB"),
        )
        original = edm.Portfolio(name="Europe", members=[
            edm.Site(name="Offshore-1", members=[bus_a, bus_b, line]),
        ])

        rebuilt = _roundtrip_tree(original)

        site = rebuilt.members[0]
        types = [type(c).__name__ for c in site.children()]
        assert types.count("JunctionPoint") == 2
        assert types.count("Line") == 1

        rebuilt_line = next(c for c in site.children() if isinstance(c, edm.Line))
        assert rebuilt_line.capacity == 500
        assert isinstance(rebuilt_line.from_entity, Reference)
        assert rebuilt_line.from_entity.target == "Europe/Offshore-1/BusA"
        assert rebuilt_line.to_entity.target == "Europe/Offshore-1/BusB"

    def test_geometry_round_trip(self):
        """Point geometries (2D + 3D) survive GeoJSON round-trip."""
        original = edm.Portfolio(name="Europe", members=[
            edm.Site(name="A", geometry=Point(10.0, 60.0)),
            edm.Site(name="B", geometry=Point(11.0, 61.0, 50.0)),  # 3D
        ])
        rebuilt = _roundtrip_tree(original)

        a, b = rebuilt.members
        assert a.geometry.x == 10.0 and a.geometry.y == 60.0
        assert not a.geometry.has_z
        assert b.geometry.has_z
        assert b.geometry.z == 50.0

    def test_to_properties_preserved_for_every_node(self):
        original = edm.Portfolio(name="Europe", members=[
            edm.Site(name="Offshore-1", members=[
                edm.WindTurbine(
                    name="T01", capacity=3.5, hub_height=80, rotor_diameter=120,
                ),
                edm.Battery(
                    name="B01", storage_capacity=1000, max_charge=500, max_discharge=500,
                ),
            ]),
        ])
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
        for o, n in zip(orig_list, new_list):
            assert type(o) is type(n)
            assert o.name == n.name
            assert o.to_properties() == n.to_properties()

    # ------------------------------------------------------------------
    # Deeper / broader coverage
    # ------------------------------------------------------------------

    def test_deeply_nested_tree(self):
        original = edm.Portfolio(name="Global", members=[
            edm.Country(name="SE", members=[
                edm.BiddingZone(name="SE3", members=[
                    edm.Site(name="LillgrundSite", members=[
                        edm.WindFarm(name="Lillgrund", capacity=110, members=[
                            edm.WindTurbine(name="T01", capacity=2.3, hub_height=65),
                            edm.WindTurbine(name="T02", capacity=2.3, hub_height=65),
                        ]),
                    ]),
                ]),
            ]),
        ])
        rebuilt = _roundtrip_tree(original)

        country = rebuilt.members[0]
        bz = country.members[0]
        site = bz.members[0]
        wf = site.members[0]
        assert isinstance(country, edm.Country)
        assert isinstance(bz, edm.BiddingZone)
        assert isinstance(site, edm.Site)
        assert isinstance(wf, edm.WindFarm)
        assert wf.capacity == 110
        assert len(wf.members) == 2

    def test_multiple_edge_types_in_one_tree(self):
        a = edm.JunctionPoint(name="A")
        b = edm.JunctionPoint(name="B")
        c = edm.JunctionPoint(name="C")
        d = edm.JunctionPoint(name="D")
        line = edm.Line(
            name="AB-Line", capacity=500, directed=True,
            from_entity=Reference("Grid/A"),
            to_entity=Reference("Grid/B"),
        )
        link = edm.Link(
            name="BC-Link", capacity=100, directed=False,
            from_entity=Reference("Grid/B"),
            to_entity=Reference("Grid/C"),
        )
        trafo = edm.Transformer(
            name="CD-Tx", capacity=200, directed=True,
            from_entity=Reference("Grid/C"),
            to_entity=Reference("Grid/D"),
        )
        pipe = edm.Pipe(
            name="AD-Pipe", capacity=300, medium="gas", directed=True,
            from_entity=Reference("Grid/A"),
            to_entity=Reference("Grid/D"),
        )
        original = edm.Portfolio(name="Grid", members=[a, b, c, d, line, link, trafo, pipe])

        rebuilt = _roundtrip_tree(original)
        edges = [c for c in rebuilt.children() if isinstance(c, edm.Edge)]
        assert len(edges) == 4

        by_name = {e.name: e for e in edges}
        assert isinstance(by_name["AB-Line"], edm.Line)
        assert by_name["AB-Line"].directed is True
        assert isinstance(by_name["BC-Link"], edm.Link)
        assert by_name["BC-Link"].directed is False
        assert isinstance(by_name["CD-Tx"], edm.Transformer)
        assert isinstance(by_name["AD-Pipe"], edm.Pipe)
        assert by_name["AD-Pipe"].medium == "gas"

    def test_edges_across_subtrees(self):
        original = edm.Portfolio(name="Nordic", members=[
            edm.Site(name="SiteA", members=[edm.JunctionPoint(name="BusA")]),
            edm.Site(name="SiteB", members=[
                edm.JunctionPoint(name="BusB"),
                edm.Line(
                    name="CrossLink", capacity=800,
                    from_entity=Reference("Nordic/SiteA/BusA"),
                    to_entity=Reference("Nordic/SiteB/BusB"),
                ),
            ]),
        ])
        rebuilt = _roundtrip_tree(original)

        site_b = rebuilt.members[1]
        line = next(c for c in site_b.children() if isinstance(c, edm.Line))
        assert line.from_entity.target == "Nordic/SiteA/BusA"
        assert line.to_entity.target == "Nordic/SiteB/BusB"
        assert line.capacity == 800

    def test_energy_community_buildings_houses(self):
        original = edm.EnergyCommunity(name="EC-1", members=[
            edm.Building(name="BuildingA", type="commercial", members=[
                edm.House(name="Unit-1", type="apartment", members=[
                    edm.HeatPump(name="HP1", capacity=10, cop=3.5, energy_source="air"),
                    edm.PVArray(name="PVA1", capacity=5, surface_tilt=30),
                    edm.Battery(name="Bat1", storage_capacity=20),
                ]),
            ]),
        ])
        rebuilt = _roundtrip_tree(original)

        assert isinstance(rebuilt, edm.EnergyCommunity)
        building = rebuilt.members[0]
        house = building.members[0]
        assert isinstance(building, edm.Building)
        assert building.type == "commercial"
        assert isinstance(house, edm.House)
        assert house.type == "apartment"
        assert len(house.members) == 3
        hp, pva, bat = house.members
        assert hp.cop == 3.5
        assert pva.surface_tilt == 30
        assert bat.storage_capacity == 20

    def test_interconnection_between_countries(self):
        original = edm.Portfolio(name="EU", members=[
            edm.Country(name="DE"),
            edm.Country(name="FR"),
            edm.Interconnection(
                name="DE-FR", capacity_forward=1500, capacity_backward=1200,
                from_entity=Reference("EU/DE"),
                to_entity=Reference("EU/FR"),
            ),
        ])
        rebuilt = _roundtrip_tree(original)

        ic = next(c for c in rebuilt.children() if isinstance(c, edm.Interconnection))
        assert ic.capacity_forward == 1500
        assert ic.capacity_backward == 1200
        assert ic.from_entity.target == "EU/DE"
        assert ic.to_entity.target == "EU/FR"

    def test_round_trip_idempotence(self):
        tree = edm.Portfolio(name="Europe", members=[
            edm.Site(name="Offshore-1", geometry=Point(3.0, 55.0), members=[
                edm.WindTurbine(name="T01", capacity=3.5, hub_height=80),
                edm.Battery(name="B01", storage_capacity=100, max_charge=50),
            ]),
        ])

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

    def test_ancestor_path_round_trip(self):
        """Build edge rows using tree-computed ancestor paths.

        Mirrors what the real DB does: ``from_node_id`` / ``to_node_id`` are
        resolved via ``resolve_path`` back to slash-joined strings before
        :func:`reconstruct_edge` runs.
        """
        a = edm.JunctionPoint(name="BusA")
        b = edm.JunctionPoint(name="BusB")
        line = edm.Line(name="Cable", capacity=400)
        line.from_entity = a
        line.to_entity = b
        original = edm.Portfolio(name="EU", members=[
            edm.Site(name="Offshore", members=[a, b, line]),
        ])

        path_map = _build_path_map(original)
        assert path_map[id(a)] == "EU/Offshore/BusA"
        assert path_map[id(b)] == "EU/Offshore/BusB"

        node_list: list = []
        _collect_nodes(original, None, node_list)
        rebuilt: dict[int, edm.Element] = {}
        for obj, _parent in node_list:
            rebuilt[id(obj)] = reconstruct_node(_node_row(serialize_node(obj)))
        for obj, parent in node_list:
            if parent is not None:
                rebuilt[id(parent)].add_child(rebuilt[id(obj)])

        new_line = reconstruct_edge(
            _edge_row(
                serialize_edge(line),
                from_path=path_map[id(line.from_entity)],
                to_path=path_map[id(line.to_entity)],
            )
        )
        rebuilt[id(original.members[0])].add_child(new_line)

        rebuilt_root = rebuilt[id(original)]
        site = rebuilt_root.members[0]
        rebuilt_line = next(c for c in site.children() if isinstance(c, edm.Line))
        assert rebuilt_line.from_entity.target == "EU/Offshore/BusA"
        assert rebuilt_line.to_entity.target == "EU/Offshore/BusB"
        assert rebuilt_line.capacity == 400


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
        t = edm.WindTurbine(
            name="T01", capacity=3.5, commissioning_date=date(2020, 1, 15),
        )
        rebuilt = reconstruct_node(_node_row(serialize_node(t)))
        assert rebuilt.commissioning_date == date(2020, 1, 15)

    def test_sensor_height(self):
        s = edm.TemperatureSensor(name="Temp-80m", height=80.0)
        rebuilt = reconstruct_node(_node_row(serialize_node(s)))
        assert isinstance(rebuilt, edm.TemperatureSensor)
        assert rebuilt.height == 80.0


# ---------------------------------------------------------------------------
# Exhaustive EDM registry coverage
# ---------------------------------------------------------------------------
#
# Round-trips every concrete class EDM auto-registers. Abstract bases and
# tagged-union parents are skipped since they cannot be instantiated in
# isolation (no ``type`` discriminator). For each class we only check
# identity-of-type + name + ``to_properties()`` equality — the per-class
# property coverage lives in the dedicated tests above.


_ABSTRACT_OR_UNION = {
    "Node", "Edge", "Collection",
    "Area", "Asset", "EdgeAsset", "NodeAsset",
    "GridNode", "Sensor",
}


def _concrete_classes(kind: str):
    registry = _type_registry()
    out = []
    for name, cls in sorted(registry.items()):
        if name in _ABSTRACT_OR_UNION:
            continue
        if kind == "edge" and issubclass(cls, edm.Edge):
            out.append((name, cls))
        elif kind == "collection" and issubclass(cls, edm.Collection) and not issubclass(cls, edm.Edge):
            out.append((name, cls))
        elif kind == "node" and issubclass(cls, edm.Node) and not issubclass(cls, edm.Edge):
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
        assert rebuilt.name == obj.name
        assert rebuilt.to_properties() == obj.to_properties()

    @pytest.mark.parametrize(
        "name,cls",
        _concrete_classes("edge"),
        ids=lambda v: v if isinstance(v, str) else v.__name__,
    )
    def test_edge_class(self, name, cls):
        obj = cls(name=f"{name}-instance")
        row = _edge_row(serialize_edge(obj), from_path="X/A", to_path="X/B")
        rebuilt = reconstruct_edge(row)
        assert type(rebuilt) is cls
        assert rebuilt.name == obj.name
        assert rebuilt.to_properties() == obj.to_properties()
