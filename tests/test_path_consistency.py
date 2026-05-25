"""Thorough consistency tests for the materialized ``energydb.node.path`` column.

Complements ``test_path_column.py`` (which covers the headline cases) with
the edge cases that are most likely to surface drift between the
authoritative adjacency list (``parent_uuid``) and the denormalized
``path`` mirror:

* Prefix-name collisions — names where one is a string prefix of another
  (``A`` vs ``AB``) stress the ``substring`` arithmetic and the
  ``LIKE old_path || '/%%'`` pattern in :meth:`NodeScope.rename` /
  :meth:`NodeScope.move_to`. A wrong predicate would silently corrupt the
  sibling subtree.
* Deep trees (5+ levels) — verifies the single-UPDATE subtree rewrite
  propagates through more than a couple of levels.
* Multi-tree isolation — operations on one root must leave every row of
  every other top-level tree byte-identical, including ``updated_at``.
* parent_uuid ↔ path invariant — after any mixed sequence of inserts,
  renames, moves, ``add()``-s, and deletes, every row's ``path`` must
  still equal the chain of names walked via ``parent_uuid`` from a root.
* Transactions — path updates participate in the PG transaction
  (rollback restores them) and are visible to subsequent operations
  inside the same txn (the next ``register_tree`` / ``get_node`` on the
  same connection sees the new ``path``).

Live integration tests — skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL``
aren't set.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import energydb as edb
import psycopg
import pytest
from energydb import Client

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set — skipping path-consistency tests",
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
# Helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NodeRow:
    uuid: UUID
    name: str
    parent_uuid: UUID | None
    path: str
    updated_at: Any


def _raw_dsn() -> str:
    return os.environ["TIMEDB_PG_DSN"]


def _fetch_rows() -> dict[UUID, NodeRow]:
    """Return ``{uuid: NodeRow}`` for every row in ``energydb.node``."""
    with psycopg.connect(_raw_dsn()) as conn:
        rows = conn.execute("SELECT uuid, name, parent_uuid, path, updated_at FROM energydb.node").fetchall()
    return {r[0]: NodeRow(uuid=r[0], name=r[1], parent_uuid=r[2], path=r[3], updated_at=r[4]) for r in rows}


def _path_via_parents(rows: dict[UUID, NodeRow], row: NodeRow) -> str:
    """Reconstruct ``row.path`` by walking ``parent_uuid`` to the root."""
    names: list[str] = []
    cur: NodeRow | None = row
    while cur is not None:
        names.append(cur.name)
        cur = rows.get(cur.parent_uuid) if cur.parent_uuid is not None else None
    return "/".join(reversed(names))


def _assert_invariant() -> dict[UUID, NodeRow]:
    """Every row's ``path`` must equal the chain of names walked via ``parent_uuid``.

    Returns the fetched rows so callers can layer further assertions on top
    without re-querying.
    """
    rows = _fetch_rows()
    for row in rows.values():
        expected = _path_via_parents(rows, row)
        assert row.path == expected, f"path drift for uuid={row.uuid}: db_path={row.path!r}, reconstructed={expected!r}"
    return rows


def _by_path(rows: dict[UUID, NodeRow]) -> dict[str, NodeRow]:
    """Index rows by their (unique) ``path``."""
    by_path: dict[str, NodeRow] = {}
    for row in rows.values():
        by_path[row.path] = row
    return by_path


def _site_with_leaves(name: str, leaves: list[str]) -> Any:
    return edb.Site(
        name=name,
        members=[edb.wind.WindTurbine(name=ln, capacity=3.5) for ln in leaves],
    )


# ---------------------------------------------------------------------------
# Prefix-name collisions
# ---------------------------------------------------------------------------


def test_rename_does_not_affect_prefix_sibling(client):
    """Rename ``A`` → ``Z`` must not touch sibling ``AB`` (``A`` is a prefix of ``AB``)."""
    client.register_tree(
        edb.Portfolio(
            name="P",
            members=[
                edb.Site(name="A"),
                edb.Site(name="AB"),
                edb.Site(name="ABC"),
            ],
        )
    )
    before = _by_path(_fetch_rows())
    ab_before = before["P/AB"]
    abc_before = before["P/ABC"]

    client.get_node("P", "A").rename("Z")

    rows = _assert_invariant()
    by_path = _by_path(rows)
    assert "P/Z" in by_path
    assert "P/A" not in by_path
    # Prefix siblings are byte-identical, including updated_at.
    assert by_path["P/AB"] == ab_before
    assert by_path["P/ABC"] == abc_before


def test_rename_does_not_affect_prefix_descendants(client):
    """Rename ``A`` must rewrite ``A/X`` but leave ``AB/X`` untouched."""
    client.register_tree(
        edb.Portfolio(
            name="P",
            members=[
                _site_with_leaves("A", ["X"]),
                _site_with_leaves("AB", ["X"]),
            ],
        )
    )
    before = _by_path(_fetch_rows())
    ab_x_before = before["P/AB/X"]

    client.get_node("P", "A").rename("Z")

    rows = _assert_invariant()
    by_path = _by_path(rows)
    assert "P/Z/X" in by_path
    assert "P/A/X" not in by_path
    assert by_path["P/AB/X"] == ab_x_before


def test_move_does_not_affect_prefix_sibling(client):
    """Moving ``A`` to a new parent must not perturb its prefix sibling ``AB``."""
    client.register_tree(
        edb.Portfolio(
            name="P",
            members=[
                _site_with_leaves("A", ["X"]),
                _site_with_leaves("AB", ["X"]),
                edb.Site(name="Dest"),
            ],
        )
    )
    before = _by_path(_fetch_rows())
    ab_before = before["P/AB"]
    ab_x_before = before["P/AB/X"]

    client.get_node("P", "A").move_to(client.get_node("P", "Dest"))

    rows = _assert_invariant()
    by_path = _by_path(rows)
    assert "P/Dest/A" in by_path
    assert "P/Dest/A/X" in by_path
    assert "P/A" not in by_path and "P/A/X" not in by_path
    assert by_path["P/AB"] == ab_before
    assert by_path["P/AB/X"] == ab_x_before


def test_rename_into_prefix_sibling_collides_and_rolls_back(client):
    """Renaming ``A`` → ``AB`` when ``AB`` exists as a sibling must roll back atomically."""
    client.register_tree(
        edb.Portfolio(
            name="P",
            members=[
                _site_with_leaves("A", ["X"]),
                _site_with_leaves("AB", ["Y"]),
            ],
        )
    )
    before = _fetch_rows()
    with pytest.raises(psycopg.errors.UniqueViolation):
        client.get_node("P", "A").rename("AB")
    after = _fetch_rows()
    assert after == before


def test_deep_prefix_rename_only_touches_intended_subtree(client):
    """A subtree headed by ``B1`` and a sibling headed by ``B11`` are isolated.

    Rename ``B1`` → ``BX`` rewrites paths under ``A/B1/...`` only; the
    ``A/B11/...`` subtree (which shares ``B1`` as a string prefix) must
    remain byte-identical.
    """
    client.register_tree(
        edb.Portfolio(
            name="A",
            members=[
                edb.Site(
                    name="B1",
                    members=[edb.wind.WindTurbine(name="C1", capacity=1.0)],
                ),
                edb.Site(
                    name="B11",
                    members=[edb.wind.WindTurbine(name="C1", capacity=2.0)],
                ),
            ],
        )
    )
    before = _by_path(_fetch_rows())
    b11_before = before["A/B11"]
    b11_c1_before = before["A/B11/C1"]

    client.get_node("A", "B1").rename("BX")

    rows = _assert_invariant()
    by_path = _by_path(rows)
    assert "A/BX/C1" in by_path
    assert "A/B1/C1" not in by_path
    assert by_path["A/B11"] == b11_before
    assert by_path["A/B11/C1"] == b11_c1_before


# ---------------------------------------------------------------------------
# Deep trees
# ---------------------------------------------------------------------------


def _chain(depth: int) -> Any:
    """Build a linear chain ``L0 → L1 → … → L{depth-1}`` of Sites.

    Wrapping ``Site`` for every non-leaf, ``WindTurbine`` for the leaf —
    keeps the schema valid without changing the path semantics.
    """
    assert depth >= 2
    leaf = edb.wind.WindTurbine(name=f"L{depth - 1}", capacity=3.5)
    node: Any = leaf
    for i in range(depth - 2, 0, -1):
        node = edb.Site(name=f"L{i}", members=[node])
    return edb.Portfolio(name="L0", members=[node])


def test_deep_tree_create_sets_full_path(client):
    """Six-level chain — every level's path is the slash-join of its ancestors."""
    client.register_tree(_chain(6))
    by_path = _by_path(_assert_invariant())
    for i in range(6):
        expected = "/".join(f"L{j}" for j in range(i + 1))
        assert expected in by_path, f"missing path {expected!r}"


def test_deep_tree_rename_mid_rewrites_all_descendants(client):
    """Renaming a mid-level node in a 6-level chain rewrites every descendant path."""
    client.register_tree(_chain(6))
    client.get_node("L0", "L1", "L2").rename("M2")

    by_path = _by_path(_assert_invariant())
    # L0/L1 untouched.
    assert "L0/L1" in by_path
    # Renamed row + every descendant rewritten.
    assert "L0/L1/M2" in by_path
    assert "L0/L1/M2/L3/L4/L5" in by_path
    # Old chain gone end-to-end.
    assert "L0/L1/L2" not in by_path
    assert "L0/L1/L2/L3/L4/L5" not in by_path


def test_deep_tree_move_mid_propagates_new_prefix(client):
    """Move the mid-level subtree of one 6-deep chain under a second 6-deep chain."""
    client.register_tree(_chain(6))
    # Second tree with disjoint names so the move can't collide.
    other = edb.Portfolio(
        name="X0",
        members=[edb.Site(name="X1", members=[edb.Site(name="X2")])],
    )
    client.register_tree(other)

    client.get_node("L0", "L1", "L2").move_to(client.get_node("X0", "X1", "X2"))

    by_path = _by_path(_assert_invariant())
    # Old subtree gone from L0 side.
    assert "L0/L1/L2" not in by_path
    assert "L0/L1/L2/L3" not in by_path
    # New subtree fully rewritten under X2.
    assert "X0/X1/X2/L2" in by_path
    assert "X0/X1/X2/L2/L3/L4/L5" in by_path


# ---------------------------------------------------------------------------
# Multi-tree isolation
# ---------------------------------------------------------------------------


def _two_parallel_trees(client) -> None:
    client.register_tree(
        edb.Portfolio(
            name="P1",
            members=[
                _site_with_leaves("S", ["T1", "T2"]),
                edb.Site(name="Dest"),
            ],
        )
    )
    client.register_tree(
        edb.Portfolio(
            name="P2",
            members=[
                _site_with_leaves("S", ["T1", "T2"]),
                edb.Site(name="Dest"),
            ],
        )
    )


def _p2_rows(rows: dict[UUID, NodeRow]) -> dict[str, NodeRow]:
    return {p: r for p, r in _by_path(rows).items() if p.split("/")[0] == "P2"}


def test_rename_in_one_tree_leaves_other_tree_byte_identical(client):
    _two_parallel_trees(client)
    p2_before = _p2_rows(_fetch_rows())

    client.get_node("P1", "S").rename("Z")

    rows = _assert_invariant()
    assert "P1/Z/T1" in _by_path(rows)
    assert _p2_rows(rows) == p2_before


def test_move_in_one_tree_leaves_other_tree_byte_identical(client):
    _two_parallel_trees(client)
    p2_before = _p2_rows(_fetch_rows())

    client.get_node("P1", "S").move_to(client.get_node("P1", "Dest"))

    rows = _assert_invariant()
    by_path = _by_path(rows)
    assert "P1/Dest/S/T1" in by_path
    assert "P1/S" not in by_path
    assert _p2_rows(rows) == p2_before


def test_delete_subtree_leaves_other_paths_intact(client):
    """Deleting one subtree must leave every surviving row's path correct
    (CASCADE drops descendants of the deleted node only)."""
    _two_parallel_trees(client)
    before = _by_path(_fetch_rows())
    survivors = {p: row for p, row in before.items() if not p.startswith("P1/S")}

    client.get_node("P1", "S").delete()

    rows = _assert_invariant()
    by_path = _by_path(rows)
    # P1/S and its descendants gone.
    assert not any(p.startswith("P1/S") for p in by_path)
    # Everything else byte-identical (no spurious UPDATE side-effects).
    assert {p: by_path[p] for p in survivors} == survivors


# ---------------------------------------------------------------------------
# Mixed-sequence parent_uuid ↔ path invariant
# ---------------------------------------------------------------------------


def test_invariant_after_mixed_sequence_of_ops(client):
    """Insert → rename → move → add() → rename → delete → invariant still holds."""
    client.register_tree(
        edb.Portfolio(
            name="P",
            members=[
                _site_with_leaves("A", ["T1", "T2"]),
                edb.Site(name="B"),
                edb.Site(name="C"),
            ],
        )
    )
    # 1. rename a leaf
    client.get_node("P", "A", "T1").rename("T1x")
    _assert_invariant()
    # 2. move a subtree
    client.get_node("P", "A").move_to(client.get_node("P", "B"))
    _assert_invariant()
    # 3. add a child under the moved subtree
    client.get_node("P", "B", "A").add(edb.wind.WindTurbine(name="T3", capacity=5.0))
    by_path = _by_path(_assert_invariant())
    assert "P/B/A/T3" in by_path
    # 4. rename a deep node
    client.get_node("P", "B", "A").rename("AA")
    by_path = _by_path(_assert_invariant())
    assert "P/B/AA/T1x" in by_path
    assert "P/B/AA/T3" in by_path
    # 5. delete an intermediate subtree
    client.get_node("P", "B", "AA").delete()
    by_path = _by_path(_assert_invariant())
    assert not any(p.startswith("P/B/AA") for p in by_path)
    assert "P/B" in by_path and "P/C" in by_path


def test_invariant_under_rename_chain(client):
    """Renaming the same internal node several times keeps every descendant aligned."""
    client.register_tree(
        edb.Portfolio(
            name="P",
            members=[_site_with_leaves("S", ["T1", "T2"])],
        )
    )
    current = "S"
    for new in ("S1", "S2", "S3"):
        client.get_node("P", current).rename(new)
        _assert_invariant()
        current = new
    by_path = _by_path(_fetch_rows())
    assert "P/S3/T1" in by_path
    assert "P/S3/T2" in by_path
    for stale in ("P/S", "P/S1", "P/S2"):
        assert stale not in by_path


def test_invariant_under_move_back_and_forth(client):
    """Moving a subtree A → B → A leaves paths consistent with the original layout."""
    client.register_tree(
        edb.Portfolio(
            name="P",
            members=[
                _site_with_leaves("Home", ["T1"]),
                edb.Site(name="Tmp"),
            ],
        )
    )
    client.get_node("P", "Home", "T1").move_to(client.get_node("P", "Tmp"))
    _assert_invariant()
    client.get_node("P", "Tmp", "T1").move_to(client.get_node("P", "Home"))
    by_path = _by_path(_assert_invariant())
    assert "P/Home/T1" in by_path
    assert "P/Tmp/T1" not in by_path


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


def test_rename_visible_within_txn_before_commit(client):
    """Mid-transaction reads see the txn's own ``path`` rewrites."""
    client.register_tree(
        edb.Portfolio(
            name="P",
            members=[_site_with_leaves("S", ["T1"])],
        )
    )
    with client.transaction() as txn:
        txn.get_node("P", "S").rename("S2")
        # Resolve via the *new* path — only works if path was rewritten on the
        # txn's connection. .get() forces a SELECT against the open txn.
        assert txn.get_node("P", "S2", "T1").get().name == "T1"
        with pytest.raises(ValueError):
            txn.get_node("P", "S", "T1").get()
        txn.commit()

    by_path = _by_path(_assert_invariant())
    assert "P/S2/T1" in by_path
    assert "P/S/T1" not in by_path


def test_rename_rolled_back_on_exception_restores_paths(client):
    """Rename inside a txn that exits via exception is rolled back — paths revert."""
    client.register_tree(
        edb.Portfolio(
            name="P",
            members=[_site_with_leaves("S", ["T1", "T2"])],
        )
    )
    before = _fetch_rows()

    class _Sentinel(RuntimeError):
        pass

    with pytest.raises(_Sentinel), client.transaction() as txn:
        txn.get_node("P", "S").rename("Renamed")
        # Even from the same txn, the new path resolves cleanly here.
        assert txn.get_node("P", "Renamed", "T1").get().name == "T1"
        raise _Sentinel("force rollback")

    after = _fetch_rows()
    assert after == before


def test_move_inside_txn_committed_atomically(client):
    """``move_to`` inside a transaction commits atomically and leaves the invariant intact."""
    client.register_tree(
        edb.Portfolio(
            name="P",
            members=[
                _site_with_leaves("A", ["T1", "T2"]),
                edb.Site(name="B"),
            ],
        )
    )

    with client.transaction() as txn:
        txn.get_node("P", "A").move_to(txn.get_node("P", "B"))
        # Visible inside the txn.
        assert txn.get_node("P", "B", "A", "T1").get().name == "T1"
        txn.commit()

    by_path = _by_path(_assert_invariant())
    assert "P/B/A/T1" in by_path
    assert "P/B/A/T2" in by_path
    assert "P/A" not in by_path


def test_rename_then_add_child_inside_txn_uses_new_parent_path(client):
    """``add()`` inside a txn following a ``rename`` materializes the child's
    path against the *renamed* parent — proves the create_node SELECT sees
    the txn's own write."""
    client.register_tree(edb.Portfolio(name="P", members=[edb.Site(name="S")]))
    with client.transaction() as txn:
        txn.get_node("P", "S").rename("S2")
        txn.get_node("P", "S2").add(edb.wind.WindTurbine(name="Tnew", capacity=4.0))
        txn.commit()

    by_path = _by_path(_assert_invariant())
    assert "P/S2/Tnew" in by_path
    assert "P/S" not in by_path
