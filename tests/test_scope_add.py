"""Integration tests for ``NodeScope.add(...)``.

Sugar over ``register_tree(under=<scope>)``: same create-only semantics,
returns a :class:`NodeScope` on success or :class:`TreeDiff` for ``dry_run``,
participates in ``client.transaction()``.

Skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL`` are not set.
"""

from __future__ import annotations

import os

import energydb as edb
import pytest
from energydb import Client, NodeScope, TreeDiff

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set: skipping NodeScope.add tests",
        allow_module_level=True,
    )


@pytest.fixture
def populated():
    c = Client()
    c.delete()
    c.create()
    tree = edb.Portfolio(
        name="P",
        members=[edb.Site(name="S", members=[edb.wind.WindTurbine(name="T1", capacity=3.5)])],
    )
    c.register_tree(tree)
    yield c
    c.delete()
    c.close()


# ---------------------------------------------------------------------------
# Single-child add
# ---------------------------------------------------------------------------


def test_add_single_child_returns_scope_and_persists(populated):
    new = populated.get_node("P", "S").add(edb.wind.WindTurbine(name="T2", capacity=4.0))

    assert isinstance(new, NodeScope)
    fetched = populated.get_node("P", "S", "T2").get()
    assert fetched.name == "T2"
    assert fetched.capacity == 4.0


def test_add_returns_scope_pointing_at_new_node(populated):
    """The returned scope should resolve to the added node, and be chain-friendly."""
    turbine = edb.wind.WindTurbine(name="T2", capacity=3.5)
    new = populated.get_node("P", "S").add(turbine)
    fetched = new.get()
    assert fetched.id == turbine.id


def test_add_supports_chaining_with_update(populated):
    populated.get_node("P", "S").add(edb.wind.WindTurbine(name="T2", capacity=3.5)).update({"capacity": 4.2})
    assert populated.get_node("P", "S", "T2").get().capacity == 4.2


# ---------------------------------------------------------------------------
# Subtree add
# ---------------------------------------------------------------------------


def test_add_subtree(populated):
    subtree = edb.Site(
        name="S2",
        members=[
            edb.wind.WindTurbine(name="T3", capacity=4.0),
            edb.wind.WindTurbine(name="T4", capacity=4.5),
        ],
    )
    populated.get_node("P").add(subtree)

    assert populated.get_node("P", "S2", "T3").get().capacity == 4.0
    assert populated.get_node("P", "S2", "T4").get().capacity == 4.5


# ---------------------------------------------------------------------------
# dry_run
# ---------------------------------------------------------------------------


def test_add_dry_run_returns_diff_and_writes_nothing(populated):
    turbine = edb.wind.WindTurbine(name="T2", capacity=4.0)
    diff = populated.get_node("P", "S").add(turbine, dry_run=True)

    assert isinstance(diff, TreeDiff)
    assert len(diff.node_inserts) == 1
    assert diff.node_inserts[0].display_name == "T2"

    with pytest.raises(ValueError):
        populated.get_node("P", "S", "T2").get()


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


def test_add_existing_uuid_raises(populated):
    """Re-adding a node with a UUID already in the DB raises (create-only)."""
    existing = populated.get_node("P", "S", "T1").get()
    with pytest.raises(ValueError, match="create-only"):
        populated.get_node("P", "S").add(edb.wind.WindTurbine(id=existing.id, name="T1-clone", capacity=3.5))


def test_add_under_nonexistent_parent_raises(populated):
    with pytest.raises(ValueError):
        populated.get_node("does-not-exist").add(edb.wind.WindTurbine(name="T2", capacity=3.5))


# ---------------------------------------------------------------------------
# Transaction integration
# ---------------------------------------------------------------------------


def test_add_inside_transaction_atomic_commit(populated):
    with populated.transaction() as txn:
        new = txn.get_node("P", "S").add(edb.wind.WindTurbine(name="T2", capacity=4.0))
        assert isinstance(new, NodeScope)
        diff = txn.preview()
        assert any(c.display_name == "T2" for c in diff.node_inserts)
        txn.commit()

    assert populated.get_node("P", "S", "T2").get().capacity == 4.0


def test_add_inside_transaction_rolls_back_without_commit(populated):
    with pytest.raises(RuntimeError, match="without .commit"), populated.transaction() as txn:
        txn.get_node("P", "S").add(edb.wind.WindTurbine(name="T2", capacity=4.0))

    with pytest.raises(ValueError):
        populated.get_node("P", "S", "T2").get()


def test_add_dry_run_inside_txn_raises(populated):
    with pytest.raises(ValueError, match="dry_run"), populated.transaction() as txn:
        txn.get_node("P", "S").add(edb.wind.WindTurbine(name="T2", capacity=4.0), dry_run=True)
        txn.commit()
