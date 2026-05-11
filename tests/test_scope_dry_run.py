"""Dry-run integration tests for NodeScope and EdgeScope mutators.

Each mutator with ``dry_run=True`` returns a :class:`TreeDiff` describing
the change and rolls the transaction back — the DB is unchanged after.

Skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL`` are not set.
"""

from __future__ import annotations

import os

import energydb as edb
import pytest
from energydatamodel.reference import Reference
from energydb import Client, TreeDiff

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set — skipping dry_run tests",
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


@pytest.fixture
def populated(client):
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.Site(
                name="S",
                members=[edb.wind.WindTurbine(name="T", capacity=3.5)],
            ),
            edb.Site(name="B"),
        ],
    )
    client.register_tree(tree)
    return client


# ---------------------------------------------------------------------------
# NodeScope.rename
# ---------------------------------------------------------------------------


def test_rename_dry_run_returns_diff_and_writes_nothing(populated):
    diff = populated.get_node("P", "S", "T").rename("T-new", dry_run=True)
    assert isinstance(diff, TreeDiff)
    assert len(diff.node_changes) == 1
    change = diff.node_changes[0]
    assert change.renamed
    assert change.old.name == "T"
    assert change.new.name == "T-new"

    # DB unchanged.
    assert populated.get_node("P", "S", "T").get().name == "T"
    with pytest.raises(ValueError):
        populated.get_node("P", "S", "T-new").get()


def test_rename_no_dry_run_persists(populated):
    out = populated.get_node("P", "S", "T").rename("T-new")
    assert out is None
    assert populated.get_node("P", "S", "T-new").get().name == "T-new"


# ---------------------------------------------------------------------------
# NodeScope.update
# ---------------------------------------------------------------------------


def test_update_dry_run_returns_diff_and_writes_nothing(populated):
    diff = populated.get_node("P", "S", "T").update({"capacity": 4.0}, dry_run=True)
    assert isinstance(diff, TreeDiff)
    assert len(diff.node_changes) == 1
    change = diff.node_changes[0]
    assert change.data_changed
    assert change.new.data["capacity"] == 4.0

    # DB unchanged.
    assert populated.get_node("P", "S", "T").get().capacity == 3.5


def test_update_replace_data_dry_run(populated):
    diff = populated.get_node("P", "S", "T").update({"capacity": 4.0}, replace_data=True, dry_run=True)
    assert isinstance(diff, TreeDiff)
    change = diff.node_changes[0]
    # With replace_data, only the new key survives.
    assert change.new.data == {"capacity": 4.0}


# ---------------------------------------------------------------------------
# NodeScope.delete
# ---------------------------------------------------------------------------


def test_delete_dry_run_returns_diff_and_writes_nothing(populated):
    diff = populated.get_node("P", "S", "T").delete(dry_run=True)
    assert isinstance(diff, TreeDiff)
    assert len(diff.node_changes) == 1
    change = diff.node_changes[0]
    assert change.kind == "delete"
    assert change.old.name == "T"
    assert change.new is None

    # DB unchanged.
    assert populated.get_node("P", "S", "T").get().name == "T"


# ---------------------------------------------------------------------------
# NodeScope.move_to
# ---------------------------------------------------------------------------


def test_move_to_dry_run_returns_diff_and_writes_nothing(populated):
    diff = populated.get_node("P", "S", "T").move_to(["P", "B"], dry_run=True)
    assert isinstance(diff, TreeDiff)
    assert len(diff.node_changes) == 1
    change = diff.node_changes[0]
    assert change.moved
    assert change.old.parent_uuid != change.new.parent_uuid

    # DB unchanged.
    assert populated.get_node("P", "S", "T").get().name == "T"
    with pytest.raises(ValueError):
        populated.get_node("P", "B", "T").get()


# ---------------------------------------------------------------------------
# EdgeScope.rename / update / move_to / delete
# ---------------------------------------------------------------------------


@pytest.fixture
def populated_with_edge(client):
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
    bus_c = edb.grid.JunctionPoint(name="BusC")
    client.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b, bus_c]))
    line = edb.grid.Line(name="L1", capacity=500, from_element=Reference(bus_a), to_element=Reference(bus_b))
    edge_uuid = client.create_edge(line)
    return client, edge_uuid, bus_a, bus_b, bus_c


def test_edge_rename_dry_run(populated_with_edge):
    client, edge_uuid, *_ = populated_with_edge
    diff = client.get_edge(uuid=edge_uuid).rename("L1-new", dry_run=True)
    assert isinstance(diff, TreeDiff)
    assert len(diff.edge_changes) == 1
    change = diff.edge_changes[0]
    assert change.old.name == "L1"
    assert change.new.name == "L1-new"

    # DB unchanged.
    assert client.get_edge(uuid=edge_uuid).get().name == "L1"


def test_edge_rename_persists(populated_with_edge):
    client, edge_uuid, *_ = populated_with_edge
    out = client.get_edge(uuid=edge_uuid).rename("L1-new")
    assert out is None
    assert client.get_edge(uuid=edge_uuid).get().name == "L1-new"


def test_edge_update_dry_run(populated_with_edge):
    client, edge_uuid, *_ = populated_with_edge
    diff = client.get_edge(uuid=edge_uuid).update({"capacity": 750}, dry_run=True)
    assert isinstance(diff, TreeDiff)
    assert diff.edge_changes[0].data_changed


def test_edge_delete_dry_run(populated_with_edge):
    client, edge_uuid, *_ = populated_with_edge
    diff = client.get_edge(uuid=edge_uuid).delete(dry_run=True)
    assert isinstance(diff, TreeDiff)
    assert diff.edge_changes[0].kind == "delete"
    # DB unchanged.
    assert client.get_edge(uuid=edge_uuid).get().name == "L1"


def test_edge_move_to_dry_run(populated_with_edge):
    client, edge_uuid, _, _, bus_c = populated_with_edge
    diff = client.get_edge(uuid=edge_uuid).move_to(
        from_node=["Grid", "BusA"],
        to_node=["Grid", "BusC"],
        dry_run=True,
    )
    assert isinstance(diff, TreeDiff)
    change = diff.edge_changes[0]
    assert change.endpoints_changed
    assert change.new.to_node_uuid == bus_c.id


def test_edge_move_to_persists_and_preserves_uuid(populated_with_edge):
    client, edge_uuid, _, _, bus_c = populated_with_edge
    out = client.get_edge(uuid=edge_uuid).move_to(from_node=["Grid", "BusA"], to_node=["Grid", "BusC"])
    assert out is None
    edge = client.get_edge(uuid=edge_uuid).get()
    assert edge.to_element.id == bus_c.id


def test_edge_move_to_same_endpoints_raises(populated_with_edge):
    client, edge_uuid, *_ = populated_with_edge
    with pytest.raises(ValueError, match="distinct"):
        client.get_edge(uuid=edge_uuid).move_to(from_node=["Grid", "BusA"], to_node=["Grid", "BusA"])
