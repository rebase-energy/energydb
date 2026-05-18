"""Correctness tests for the UUID identity model + path-based fluent CLI.

Live integration tests — skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL``
aren't set. UUID identity makes most of the formerly-tricky path issues
trivial; the fluent CLI's path resolution still needs to handle special
characters and disambiguation.

* Duplicate names under different parents resolve independently.
* Names with dots and unicode round-trip cleanly (``/`` is reserved as
  the path separator and is rejected by name validation — see
  ``test_name_validation.py``).
* Edges are addressed by ``uuid`` or by the ``(from_path, to_path,
  edge_type)`` triple.
* ``move_to`` keeps the node's identity (and series) intact.
* ``UNIQUE (parent_uuid, name)`` rejects same-name siblings of different
  node_type at the DB layer.
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

    out = client.get_node("P", "NewSite", "T01").read(data_type="actual", name="capacity")
    assert out["value"].to_list() == [3.5, 3.5]
    # series_id is no longer surfaced on the public result; the per-call sid
    # we registered above is still valid internally — confirmed by data integrity.
    _ = sid


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
