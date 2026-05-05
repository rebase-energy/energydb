"""Integration tests for ``register_tree`` modes — additive vs replace_subtree,
allow_delete, dry_run, type-change rejection, cross-tree edge rejection,
silent rename/move detection.

Skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL`` are not set.
"""

from __future__ import annotations

import os
from uuid import UUID

import energydb as edb
import pytest
from energydatamodel.reference import Reference
from energydb import Client, TreeDiff

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set — skipping register_tree mode tests",
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


# ---------------------------------------------------------------------------
# Additive mode (default)
# ---------------------------------------------------------------------------


def test_additive_is_idempotent(client):
    """register_tree → register_tree (same content) → no-op."""
    tree = edb.Portfolio(
        name="P",
        members=[edb.Site(name="S", members=[edb.wind.WindTurbine(name="T", capacity=3.5)])],
    )
    client.register_tree(tree)
    # Second call should not raise; tree is unchanged.
    root_uuid = client.register_tree(tree)
    assert isinstance(root_uuid, UUID)


def test_additive_does_not_delete_orphans(client):
    """Removing a node from the EDM tree → DB row stays under additive."""
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.Site(name="A", members=[edb.wind.WindTurbine(name="T1", capacity=3.5)]),
            edb.Site(name="B"),
        ],
    )
    client.register_tree(tree)

    smaller = edb.Portfolio(
        id=tree.id,
        name="P",
        members=[
            edb.Site(id=tree.members[0].id, name="A"),  # T1 removed from EDM
        ],
    )
    client.register_tree(smaller)

    # T1 should still exist on the DB side.
    rebuilt = client.get_tree("P")
    site_a = next(m for m in rebuilt.members if m.name == "A")
    assert any(m.name == "T1" for m in site_a.members)


# ---------------------------------------------------------------------------
# replace_subtree mode
# ---------------------------------------------------------------------------


def test_replace_subtree_requires_allow_delete(client):
    tree = edb.Portfolio(
        name="P",
        members=[edb.Site(name="A", members=[edb.wind.WindTurbine(name="T1", capacity=3.5)])],
    )
    client.register_tree(tree)

    smaller = edb.Portfolio(
        id=tree.id,
        name="P",
        members=[edb.Site(id=tree.members[0].id, name="A")],  # T1 dropped
    )
    with pytest.raises(ValueError, match="allow_delete=True"):
        client.register_tree(smaller, mode="replace_subtree")


def test_replace_subtree_with_allow_delete_removes_orphans(client):
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.Site(name="A", members=[edb.wind.WindTurbine(name="T1", capacity=3.5)]),
            edb.Site(name="B"),
        ],
    )
    client.register_tree(tree)

    smaller = edb.Portfolio(
        id=tree.id,
        name="P",
        members=[edb.Site(id=tree.members[0].id, name="A")],  # T1 + Site B dropped
    )
    client.register_tree(smaller, mode="replace_subtree", allow_delete=True)

    rebuilt = client.get_tree("P")
    site_names = {m.name for m in rebuilt.members}
    assert site_names == {"A"}
    assert rebuilt.members[0].members == []


def test_replace_subtree_silent_rename(client):
    """A renamed node (same uuid, new name) should not require allow_delete."""
    tree = edb.Portfolio(
        name="P",
        members=[edb.Site(name="OldName", members=[edb.wind.WindTurbine(name="T", capacity=3.5)])],
    )
    client.register_tree(tree)

    site = tree.members[0]
    site.name = "NewName"
    # No allow_delete required — uuid match means it's a rename, not a delete.
    client.register_tree(tree, mode="replace_subtree")

    with pytest.raises(ValueError):
        client.get_node("P", "OldName").get()
    moved = client.get_node("P", "NewName").get()
    assert moved.id == site.id


def test_replace_subtree_silent_move(client):
    """A moved node (same uuid, new parent) doesn't require allow_delete."""
    turbine = edb.wind.WindTurbine(name="T", capacity=3.5)
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.Site(name="A", members=[turbine]),
            edb.Site(name="B"),
        ],
    )
    client.register_tree(tree)

    # Move turbine from A to B in the in-memory tree.
    tree.members[0].members.remove(turbine)
    tree.members[1].members.append(turbine)

    client.register_tree(tree, mode="replace_subtree")

    moved = client.get_node("P", "B", "T").get()
    assert moved.id == turbine.id
    with pytest.raises(ValueError):
        client.get_node("P", "A", "T").get()


def test_replace_subtree_property_edit_in_place(client):
    """Same uuid, changed property → UPDATE, not delete + insert."""
    turbine = edb.wind.WindTurbine(name="T", capacity=3.5)
    tree = edb.Portfolio(name="P", members=[edb.Site(name="S", members=[turbine])])
    client.register_tree(tree)

    turbine.capacity = 4.0
    client.register_tree(tree, mode="replace_subtree")

    rebuilt = client.get_node("P", "S", "T").get()
    assert rebuilt.id == turbine.id
    assert rebuilt.capacity == 4.0


# ---------------------------------------------------------------------------
# dry_run
# ---------------------------------------------------------------------------


def test_dry_run_returns_diff_and_writes_nothing(client):
    tree = edb.Portfolio(
        name="P",
        members=[edb.Site(name="S", members=[edb.wind.WindTurbine(name="T", capacity=3.5)])],
    )

    diff = client.register_tree(tree, dry_run=True)
    assert isinstance(diff, TreeDiff)
    assert len(diff.node_inserts) == 3  # P, S, T

    # Nothing was actually written.
    with pytest.raises(ValueError):
        client.get_node("P").get()


def test_dry_run_with_replace_subtree(client):
    tree = edb.Portfolio(
        name="P",
        members=[edb.Site(name="S", members=[edb.wind.WindTurbine(name="T", capacity=3.5)])],
    )
    client.register_tree(tree)

    smaller = edb.Portfolio(id=tree.id, name="P", members=[edb.Site(id=tree.members[0].id, name="S")])
    diff = client.register_tree(smaller, mode="replace_subtree", allow_delete=True, dry_run=True)
    assert isinstance(diff, TreeDiff)
    assert len(diff.node_deletes) == 1
    assert diff.node_deletes[0].display_name == "T"

    # T still exists (dry run did not commit).
    assert client.get_node("P", "S", "T").get().capacity == 3.5


def test_replace_subtree_noop_roundtrip_with_geometries_has_no_changes(client):
    """Read-modify-write with no mutation must produce an empty diff, even when
    nodes carry shapely geometries. Regression for spurious data-edit entries
    caused by tuple coords in serialize output vs list coords from JSONB."""
    from shapely.geometry import Point, Polygon

    tree = edb.Portfolio(
        name="P",
        members=[
            edb.Site(
                name="Offshore",
                geometry=Point(3.0, 55.0),
                members=[
                    edb.wind.WindTurbine(name="T01", capacity=3.5, geometry=Point(3.02, 55.01)),
                ],
            ),
            edb.BiddingZone(name="SE4", geometry=Polygon([(12.5, 55.0), (16.5, 55.0), (16.5, 58.0), (12.5, 58.0)])),
        ],
    )
    client.register_tree(tree)

    # Read back, then immediately register with no mutation. Diff must be empty.
    fetched = client.get_tree("P")
    diff = client.register_tree(fetched, mode="replace_subtree", allow_delete=True, dry_run=True)
    assert isinstance(diff, TreeDiff)
    assert not diff.has_changes, (
        f"No-op round-trip should produce no changes, got: nodes={diff.node_changes}, edges={diff.edge_changes}"
    )


# ---------------------------------------------------------------------------
# Cross-tree edge rejection
# ---------------------------------------------------------------------------


def test_cross_tree_edge_rejected(client):
    """An edge whose endpoint UUID is not in the tree raises."""
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
    # Edge points at bus_b but bus_b is NOT in the tree being registered.
    bad = edb.Portfolio(
        name="Bad",
        members=[
            bus_a,
            edb.grid.Line(name="X", capacity=10, from_element=Reference(bus_a), to_element=Reference(bus_b)),
        ],
    )
    with pytest.raises(ValueError, match="not in the tree"):
        client.register_tree(bad)


# ---------------------------------------------------------------------------
# Type change rejection
# ---------------------------------------------------------------------------


def test_type_change_rejected(client):
    """Same uuid, different type → raises."""
    turbine = edb.wind.WindTurbine(name="X", capacity=3.5)
    client.register_tree(edb.Portfolio(name="P", members=[turbine]))

    # Build a different EDM type with the SAME uuid.
    battery = edb.battery.Battery(id=turbine.id, name="X", storage_capacity=10)
    bad = edb.Portfolio(name="P", members=[battery])
    with pytest.raises(ValueError, match="immutable"):
        client.register_tree(bad)
