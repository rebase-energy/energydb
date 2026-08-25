"""Integration tests for ``client.transaction()``.

Verifies:
- Explicit ``.commit()`` applies all changes atomically.
- Exit without ``.commit()`` raises and rolls back.
- Exception inside the block rolls back without masking the original.
- ``preview()`` aggregates the change log so far, including mid-txn
  ``register_tree`` inserts.
- Mid-transaction reads see the txn's own uncommitted writes (proves the
  txn really uses a single connection).
- ``dry_run`` inside a transaction is rejected.

Skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL`` are not set.
"""

from __future__ import annotations

import os

import energydb as edb
import pytest
from energydb import Client, TreeDiff

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set: skipping transaction tests",
        allow_module_level=True,
    )


@pytest.fixture
def populated():
    c = Client()
    c.delete()
    c.create()
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.Site(
                name="S",
                members=[
                    edb.wind.WindTurbine(name="T1", capacity=3.5),
                    edb.wind.WindTurbine(name="T2", capacity=3.5),
                ],
            ),
            edb.Site(name="B"),
        ],
    )
    c.register_tree(tree)
    yield c
    c.delete()
    c.close()


# ---------------------------------------------------------------------------
# Explicit commit
# ---------------------------------------------------------------------------


def test_commit_applies_changes(populated):
    with populated.transaction() as txn:
        txn.get_node("P", "S", "T1").rename("T1-A")
        txn.get_node("P", "S", "T2").update({"capacity": 4.0})
        txn.commit()

    assert populated.get_node("P", "S", "T1-A").get().name == "T1-A"
    assert populated.get_node("P", "S", "T2").get().capacity == 4.0


# ---------------------------------------------------------------------------
# Missing commit → rollback + raise
# ---------------------------------------------------------------------------


def test_missing_commit_rolls_back_and_raises(populated):
    with pytest.raises(RuntimeError, match="without .commit"), populated.transaction() as txn:
        txn.get_node("P", "S", "T1").rename("T1-X")

    # T1 still exists; rename did not persist.
    assert populated.get_node("P", "S", "T1").get().name == "T1"
    with pytest.raises(ValueError):
        populated.get_node("P", "S", "T1-X").get()


# ---------------------------------------------------------------------------
# Exception inside block → rollback, original error propagates
# ---------------------------------------------------------------------------


def test_exception_rolls_back_without_masking(populated):
    class Boom(Exception):
        pass

    with pytest.raises(Boom), populated.transaction() as txn:
        txn.get_node("P", "S", "T1").rename("T1-X")
        raise Boom("user error")

    assert populated.get_node("P", "S", "T1").get().name == "T1"


# ---------------------------------------------------------------------------
# preview()
# ---------------------------------------------------------------------------


def test_preview_includes_all_changes(populated):
    with populated.transaction() as txn:
        txn.get_node("P", "S", "T1").rename("T1-A")
        txn.get_node("P", "S", "T2").update({"capacity": 4.0})
        diff = txn.preview()
        assert isinstance(diff, TreeDiff)
        assert len(diff.node_changes) == 2
        txn.commit()


def test_preview_includes_register_tree(populated):
    """register_tree inside a txn should populate the change log."""
    with populated.transaction() as txn:
        new_subtree = edb.Site(
            name="C",
            members=[edb.wind.WindTurbine(name="T3", capacity=5.0)],
        )
        txn.register_tree(new_subtree, under=["P"])
        diff = txn.preview()
        # 2 inserts: Site "C" + WindTurbine "T3".
        assert len(diff.node_inserts) >= 2
        txn.commit()

    rebuilt = populated.get_tree("P")
    names = {m.name for m in rebuilt.members}
    assert "C" in names


# ---------------------------------------------------------------------------
# Mid-transaction reads see uncommitted writes (proves single connection)
# ---------------------------------------------------------------------------


def test_mid_txn_read_sees_uncommitted_writes(populated):
    with populated.transaction() as txn:
        txn.get_node("P", "S", "T1").rename("T1-A")
        # Re-fetch via txn: must reflect the rename.
        node = txn.get_node("P", "S", "T1-A").get()
        assert node.name == "T1-A"
        txn.commit()


# ---------------------------------------------------------------------------
# dry_run inside a transaction is rejected
# ---------------------------------------------------------------------------


def test_dry_run_inside_txn_raises(populated):
    with pytest.raises(ValueError, match="dry_run"), populated.transaction() as txn:
        txn.get_node("P", "S", "T1").rename("T1-A", dry_run=True)
        txn.commit()


# ---------------------------------------------------------------------------
# Double commit is rejected
# ---------------------------------------------------------------------------


def test_double_commit_raises(populated):
    with pytest.raises(RuntimeError, match="already committed"), populated.transaction() as txn:
        txn.get_node("P", "S", "T1").rename("T1-A")
        txn.commit()
        txn.commit()


# ---------------------------------------------------------------------------
# Time-series I/O is rejected on txn-bound scopes
# ---------------------------------------------------------------------------


def test_node_scope_write_inside_txn_raises(populated):
    import polars as pl

    df = pl.DataFrame({"valid_time": [], "value": []})
    with pytest.raises(RuntimeError, match="not supported inside a transaction"), populated.transaction() as txn:
        txn.get_node("P", "S", "T1").write(df, data_type="actual", name="power")
        txn.commit()


def test_node_scope_read_inside_txn_raises(populated):
    with pytest.raises(RuntimeError, match="not supported inside a transaction"), populated.transaction() as txn:
        txn.get_node("P", "S", "T1").read()
        txn.commit()


def test_node_scope_read_relative_inside_txn_raises(populated):
    with pytest.raises(RuntimeError, match="not supported inside a transaction"), populated.transaction() as txn:
        txn.get_node("P", "S", "T1").read_relative(data_type="actual", name="power")
        txn.commit()
