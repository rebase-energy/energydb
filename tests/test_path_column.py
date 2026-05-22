"""Tests for the materialized ``energydb.node.path`` column.

Verify that the column is populated on insert, kept consistent on rename
and move (self + descendants in one atomic UPDATE), and that the unique /
non-empty constraints fire as designed.

Live integration tests — skipped if ``TIMEDB_PG_DSN`` /
``TIMEDB_CH_URL`` aren't set.
"""

from __future__ import annotations

import os
from uuid import uuid4

import energydb as edb
import psycopg
import pytest
from energydb import Client

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set — skipping path-column tests",
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


def _raw_dsn() -> str:
    return os.environ["TIMEDB_PG_DSN"]


def _fetch_paths() -> dict[str, str]:
    """Return ``{name: path}`` for every row currently in ``energydb.node``."""
    with psycopg.connect(_raw_dsn()) as conn:
        rows = conn.execute("SELECT name, path FROM energydb.node").fetchall()
    return dict(rows)


def _fetch_row(name: str) -> tuple[str, str, object]:
    """Return ``(name, path, updated_at)`` for the (unique-by-name) row."""
    with psycopg.connect(_raw_dsn()) as conn:
        row = conn.execute(
            "SELECT name, path, updated_at FROM energydb.node WHERE name = %s",
            (name,),
        ).fetchone()
    assert row is not None, f"no row with name={name!r}"
    return row


# ---------------------------------------------------------------------------
# create_node: path is materialized from the ancestor chain
# ---------------------------------------------------------------------------


def test_create_node_sets_path(client):
    tree = edb.Portfolio(
        name="A",
        members=[edb.Site(name="B", members=[edb.wind.WindTurbine(name="C", capacity=3.5)])],
    )
    client.register_tree(tree)
    paths = _fetch_paths()
    assert paths["A"] == "A"
    assert paths["B"] == "A/B"
    assert paths["C"] == "A/B/C"


# ---------------------------------------------------------------------------
# rename: subtree UPDATE
# ---------------------------------------------------------------------------


def test_rename_leaf_does_not_touch_others(client):
    client.register_tree(
        edb.Portfolio(
            name="A",
            members=[edb.Site(name="B", members=[edb.wind.WindTurbine(name="C", capacity=3.5)])],
        )
    )
    _, _, a_before = _fetch_row("A")
    _, _, b_before = _fetch_row("B")

    client.get_node("A", "B", "C").rename("C2")

    paths = _fetch_paths()
    assert paths["C2"] == "A/B/C2"
    # Self changed; ancestors did not.
    _, _, a_after = _fetch_row("A")
    _, _, b_after = _fetch_row("B")
    assert a_after == a_before
    assert b_after == b_before


def test_rename_internal_updates_descendants(client):
    client.register_tree(
        edb.Portfolio(
            name="A",
            members=[edb.Site(name="B", members=[edb.wind.WindTurbine(name="C", capacity=3.5)])],
        )
    )
    client.get_node("A", "B").rename("B2")
    paths = _fetch_paths()
    assert paths["A"] == "A"
    assert paths["B2"] == "A/B2"
    assert paths["C"] == "A/B2/C"


def test_rename_collision_rolls_back(client):
    client.register_tree(
        edb.Portfolio(
            name="A",
            members=[
                edb.Site(name="B", members=[edb.wind.WindTurbine(name="X", capacity=3.5)]),
                edb.Site(name="B2"),
            ],
        )
    )
    before = _fetch_paths()
    with pytest.raises(psycopg.errors.UniqueViolation):
        client.get_node("A", "B").rename("B2")
    after = _fetch_paths()
    assert after == before


def test_rename_root_updates_descendants(client):
    client.register_tree(
        edb.Portfolio(
            name="A",
            members=[edb.Site(name="B", members=[edb.wind.WindTurbine(name="C", capacity=3.5)])],
        )
    )
    client.get_node("A").rename("A2")
    paths = _fetch_paths()
    assert paths["A2"] == "A2"
    assert paths["B"] == "A2/B"
    assert paths["C"] == "A2/B/C"


# ---------------------------------------------------------------------------
# move_to: subtree UPDATE
# ---------------------------------------------------------------------------


def test_move_subtree_rewrites_paths(client):
    client.register_tree(
        edb.Portfolio(
            name="A",
            members=[edb.Site(name="X", members=[edb.wind.WindTurbine(name="Y", capacity=3.5)])],
        )
    )
    client.register_tree(edb.Portfolio(name="B"))

    client.get_node("A", "X").move_to(client.get_node("B"))

    paths = _fetch_paths()
    assert paths["X"] == "B/X"
    assert paths["Y"] == "B/X/Y"


def test_move_with_descendant_collision_rolls_back(client):
    client.register_tree(
        edb.Portfolio(
            name="A",
            members=[edb.Site(name="X", members=[edb.wind.WindTurbine(name="Y", capacity=3.5)])],
        )
    )
    client.register_tree(
        edb.Portfolio(
            name="B",
            members=[edb.Site(name="X", members=[edb.wind.WindTurbine(name="Y", capacity=4.0)])],
        )
    )
    before = _fetch_paths()
    with pytest.raises(psycopg.errors.UniqueViolation):
        client.get_node("A", "X").move_to(client.get_node("B"))
    after = _fetch_paths()
    assert after == before


def test_move_into_descendant_rejects(client):
    client.register_tree(
        edb.Portfolio(
            name="A",
            members=[edb.Site(name="X", members=[edb.wind.WindTurbine(name="Y", capacity=3.5)])],
        )
    )
    with pytest.raises(ValueError, match="own subtree"):
        client.get_node("A", "X").move_to(client.get_node("A", "X", "Y"))


# ---------------------------------------------------------------------------
# delete: CASCADE removes descendants and their path rows
# ---------------------------------------------------------------------------


def test_delete_cascades(client):
    client.register_tree(
        edb.Portfolio(
            name="A",
            members=[edb.Site(name="B", members=[edb.wind.WindTurbine(name="C", capacity=3.5)])],
        )
    )
    client.get_node("A").delete()
    paths = _fetch_paths()
    assert paths == {}


# ---------------------------------------------------------------------------
# UNIQUE(path) at the DB layer
# ---------------------------------------------------------------------------


def test_path_unique_constraint_holds(client):
    """Raw INSERT of two rows with the same ``path`` raises ``UniqueViolation``."""
    with psycopg.connect(_raw_dsn()) as conn:
        conn.execute(
            "INSERT INTO energydb.node (uuid, node_type, name, path, data) VALUES (%s, %s, %s, %s, %s)",
            (uuid4(), "Site", "Solo1", "DupPath", "{}"),
        )
    # Separate connection so the first INSERT is already committed; the second
    # INSERT's transaction can be rolled back independently when it raises.
    with (
        psycopg.connect(_raw_dsn()) as conn,
        pytest.raises(psycopg.errors.UniqueViolation),
    ):
        conn.execute(
            "INSERT INTO energydb.node (uuid, node_type, name, path, data) VALUES (%s, %s, %s, %s, %s)",
            (uuid4(), "Site", "Solo2", "DupPath", "{}"),
        )
