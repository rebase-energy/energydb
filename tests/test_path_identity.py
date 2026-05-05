"""Correctness tests for the UUID identity model + path-based fluent CLI.

Live integration tests — skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL``
aren't set. UUID identity makes most of the formerly-tricky path issues
trivial; the fluent CLI's path resolution still needs to handle special
characters and disambiguation.

* Duplicate names under different parents resolve independently.
* Names containing ``/`` survive round-trip.
* Edges are addressed by ``uuid`` or by the ``(from_path, to_path,
  edge_type)`` triple.
* ``move_to`` keeps the node's identity (and series) intact.
* ``UNIQUE (parent_uuid, name)`` rejects same-name siblings of different
  node_type at the DB layer.
* ``List(Utf8)`` manifest paths route correctly, including special chars.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from uuid import UUID

import energydb as edb
import polars as pl
import pytest
from energydatamodel.reference import Reference
from energydb import Client

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set — skipping path-identity tests",
        allow_module_level=True,
    )


@pytest.fixture
def client():
    c = Client()
    c.delete()
    c.create()
    yield c
    c.delete()
    c.close()


def _hours(n: int) -> list[datetime]:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    return [base + timedelta(hours=i) for i in range(n)]


# ---------------------------------------------------------------------------
# Duplicate names under different parents
# ---------------------------------------------------------------------------


def test_duplicate_names_under_different_parents_resolve_independently(client):
    """Two `T01` turbines under different sites are distinct nodes."""
    tree = edb.Portfolio(
        name="MyP",
        members=[
            edb.Site(
                name="SiteA",
                members=[edb.wind.WindTurbine(name="T01", capacity=3.5)],
            ),
            edb.Site(
                name="SiteB",
                members=[edb.wind.WindTurbine(name="T01", capacity=4.0)],
            ),
        ],
    )
    client.register_tree(tree)

    a = client.get_node("MyP", "SiteA", "T01").get()
    b = client.get_node("MyP", "SiteB", "T01").get()
    assert a.capacity == 3.5
    assert b.capacity == 4.0
    assert a.id != b.id


# ---------------------------------------------------------------------------
# Names with special characters
# ---------------------------------------------------------------------------


def test_names_with_slashes_round_trip(client):
    """A node name containing '/' must survive write → read."""
    tree = edb.Portfolio(
        name="P",
        members=[edb.Site(name="Distribution/12kV")],
    )
    client.register_tree(tree)
    site = client.get_node("P", "Distribution/12kV").get()
    assert site.name == "Distribution/12kV"


def test_names_with_dots_and_unicode(client):
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.Site(name="N.O.R.D"),
            edb.Site(name="Lillgrund Vindkraftspark"),
            edb.Site(name="北京"),
        ],
    )
    client.register_tree(tree)
    for nm in ("N.O.R.D", "Lillgrund Vindkraftspark", "北京"):
        assert client.get_node("P", nm).get().name == nm


# ---------------------------------------------------------------------------
# Edges addressed by uuid or by (from_path, to_path, type) triple
# ---------------------------------------------------------------------------


def test_edge_addressed_by_uuid_or_triple(client):
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
    client.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b]))

    line = edb.grid.Line(name="Cable-1", capacity=500, from_element=Reference(bus_a), to_element=Reference(bus_b))
    edge_uuid = client.create_edge(line)
    assert isinstance(edge_uuid, UUID)

    by_triple = client.get_edge(("Grid", "BusA"), ("Grid", "BusB"), type="Line").get()
    assert isinstance(by_triple, edb.grid.Line)
    assert by_triple.capacity == 500
    assert by_triple.id == edge_uuid

    by_uuid = client.get_edge(uuid=edge_uuid).get()
    assert by_uuid.capacity == 500


def test_two_edges_of_different_types_between_same_endpoints(client):
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
    client.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b]))

    line = edb.grid.Line(name="Cable", capacity=500, from_element=Reference(bus_a), to_element=Reference(bus_b))
    pipe = edb.grid.Pipe(
        name="GasPipe", capacity=200, medium="gas", from_element=Reference(bus_a), to_element=Reference(bus_b)
    )
    line_uuid = client.create_edge(line)
    pipe_uuid = client.create_edge(pipe)
    assert line_uuid != pipe_uuid
    assert isinstance(client.get_edge(("Grid", "BusA"), ("Grid", "BusB"), type="Line").get(), edb.grid.Line)
    assert isinstance(client.get_edge(("Grid", "BusA"), ("Grid", "BusB"), type="Pipe").get(), edb.grid.Pipe)


# ---------------------------------------------------------------------------
# move_to: identity + series survive
# ---------------------------------------------------------------------------


def test_move_to_preserves_uuid_and_series(client):
    turbine = edb.wind.WindTurbine(name="T01", capacity=3.5)
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.Site(name="OldSite", members=[turbine]),
            edb.Site(name="NewSite"),
        ],
    )
    client.register_tree(tree)

    scope = client.get_node("P", "OldSite", "T01")
    sid = scope.register_series(
        name="capacity",
        canonical_unit="MW",
        data_type="actual",
        timeseries_type="FLAT",
        retention="forever",
    )
    df = pl.DataFrame({"valid_time": _hours(2), "value": [3.5, 3.5]})
    scope.write(df, data_type="actual", name="capacity")

    scope.move_to(client.get_node("P", "NewSite"))

    with pytest.raises(ValueError):
        client.get_node("P", "OldSite", "T01").get()
    moved = client.get_node("P", "NewSite", "T01").get()
    assert moved.capacity == 3.5
    # UUID survives the move.
    assert moved.id == turbine.id

    out = client.get_node("P", "NewSite", "T01").read(data_type="actual", name="capacity", output="polars")
    assert out["value"].to_list() == [3.5, 3.5]
    assert out["series_id"].unique().to_list() == [sid]


def test_move_to_collision_raises(client):
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.Site(name="A", members=[edb.wind.WindTurbine(name="T01", capacity=3.5)]),
            edb.Site(name="B", members=[edb.wind.WindTurbine(name="T01", capacity=4.0)]),
        ],
    )
    client.register_tree(tree)

    import psycopg

    with pytest.raises(psycopg.errors.UniqueViolation):
        client.get_node("P", "A", "T01").move_to(client.get_node("P", "B"))


def test_move_to_self_rejected(client):
    """Moving a node into itself must raise — that would orphan it from the tree."""
    client.register_tree(edb.Portfolio(name="P", members=[edb.Site(name="S")]))
    with pytest.raises(ValueError, match="into itself"):
        client.get_node("P", "S").move_to(client.get_node("P", "S"))


def test_move_to_descendant_rejected(client):
    """Moving a node into one of its descendants must raise — would create a
    cycle in the parent chain."""
    client.register_tree(
        edb.Portfolio(
            name="P",
            members=[edb.Site(name="S", members=[edb.wind.WindTurbine(name="T", capacity=3.5)])],
        )
    )
    with pytest.raises(ValueError, match="own subtree"):
        client.get_node("P", "S").move_to(client.get_node("P", "S", "T"))


# ---------------------------------------------------------------------------
# Tightened (parent_uuid, name) unique constraint
# ---------------------------------------------------------------------------


def test_same_name_different_type_rejected_under_one_parent(client):
    import psycopg

    client.register_tree(edb.Portfolio(name="P"))
    client.register_tree(edb.wind.WindTurbine(name="X", capacity=3.5), under=("P",))

    # Same name + different type under the same parent → rejected by the
    # ``UNIQUE (parent_uuid, name)`` constraint (different uuid for Battery,
    # so ON CONFLICT (uuid) doesn't fire; the unique key collision surfaces).
    with pytest.raises(psycopg.errors.UniqueViolation):
        client.register_tree(edb.battery.Battery(name="X", storage_capacity=10), under=("P",))


# ---------------------------------------------------------------------------
# Manifest with List(Utf8) paths
# ---------------------------------------------------------------------------


def test_manifest_with_list_utf8_paths(client):
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.wind.WindTurbine(
                name="T01",
                capacity=3.5,
                timeseries=[edb.TimeSeriesDescriptor(name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
            ),
            edb.wind.WindTurbine(
                name="Distribution/12kV",  # name with slash
                capacity=2.0,
                timeseries=[edb.TimeSeriesDescriptor(name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
            ),
        ],
    )
    client.register_tree(tree)

    write_df = pl.DataFrame(
        {
            "path": [["P", "T01"]] * 2 + [["P", "Distribution/12kV"]] * 2,
            "data_type": ["actual"] * 4,
            "name": ["power"] * 4,
            "valid_time": _hours(2) * 2,
            "value": [1.0, 2.0, 10.0, 20.0],
        }
    )
    client.write(write_df)

    manifest = pl.DataFrame(
        {
            "path": [["P", "T01"], ["P", "Distribution/12kV"]],
            "data_type": ["actual", "actual"],
            "name": ["power", "power"],
        }
    )
    out = client.read(manifest, output="polars")
    assert set(out["node"].unique().to_list()) == {"T01", "Distribution/12kV"}
    paths = sorted([tuple(p) for p in out["path"].to_list()])
    assert ("P", "Distribution/12kV") in paths
    assert ("P", "T01") in paths
