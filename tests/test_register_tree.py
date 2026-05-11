"""Integration tests for ``register_tree`` — create-only semantics, dry_run,
type-change rejection, cross-tree edge rejection, "already exists" guard.

Modifications to existing rows go through scope mutators or
:meth:`Client.transaction`; ``register_tree`` does not upsert.

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
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set — skipping register_tree tests",
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
# Create-only semantics
# ---------------------------------------------------------------------------


def test_register_creates_tree(client):
    tree = edb.Portfolio(
        name="P",
        members=[edb.Site(name="S", members=[edb.wind.WindTurbine(name="T", capacity=3.5)])],
    )
    root_uuid = client.register_tree(tree)
    assert isinstance(root_uuid, UUID)
    rebuilt = client.get_tree("P")
    assert rebuilt.id == tree.id
    assert rebuilt.members[0].members[0].name == "T"


def test_register_existing_uuid_raises(client):
    """Re-registering the same tree should raise — register_tree is create-only."""
    tree = edb.Portfolio(
        name="P",
        members=[edb.Site(name="S", members=[edb.wind.WindTurbine(name="T", capacity=3.5)])],
    )
    client.register_tree(tree)
    with pytest.raises(ValueError, match="create-only"):
        client.register_tree(tree)


def test_register_partial_overlap_raises(client):
    """If even one UUID in the payload already exists, the whole call fails."""
    site = edb.Site(name="S", members=[edb.wind.WindTurbine(name="T", capacity=3.5)])
    tree = edb.Portfolio(name="P", members=[site])
    client.register_tree(tree)

    # New portfolio that reuses the existing site's uuid.
    new_tree = edb.Portfolio(
        name="P2",
        members=[edb.Site(id=site.id, name="S", members=[edb.wind.WindTurbine(name="T2", capacity=4.0)])],
    )
    with pytest.raises(ValueError, match="create-only"):
        client.register_tree(new_tree)


def test_register_new_subtree_under_existing_parent(client):
    """Registering a fresh subtree under an existing parent is fine."""
    tree = edb.Portfolio(name="P", members=[edb.Site(name="A")])
    client.register_tree(tree)

    new_subtree = edb.Site(name="B", members=[edb.wind.WindTurbine(name="T", capacity=3.5)])
    client.register_tree(new_subtree, under=["P"])

    rebuilt = client.get_tree("P")
    names = {m.name for m in rebuilt.members}
    assert names == {"A", "B"}


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

    with pytest.raises(ValueError):
        client.get_node("P").get()


# ---------------------------------------------------------------------------
# Cross-tree edge rejection
# ---------------------------------------------------------------------------


def test_cross_tree_edge_rejected(client):
    """An edge whose endpoint UUID is not in the tree raises."""
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
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
# Type change rejection — would only fire if a duplicate UUID survived the
# create-only guard. Kept as a defense-in-depth check.
# ---------------------------------------------------------------------------


def test_type_change_rejected_via_create_only(client):
    """Same uuid, different type → caught by the create-only guard first."""
    turbine = edb.wind.WindTurbine(name="X", capacity=3.5)
    client.register_tree(edb.Portfolio(name="P", members=[turbine]))

    battery = edb.battery.Battery(id=turbine.id, name="X", storage_capacity=10)
    bad = edb.Portfolio(name="P2", members=[battery])
    with pytest.raises(ValueError, match="create-only"):
        client.register_tree(bad)
