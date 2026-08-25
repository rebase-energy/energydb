"""Wildcard-safety regression: node names containing ``_`` must not bleed into
sibling subtrees through the LIKE prefix queries on ``node.path``.

The bug class (before the ESCAPE fix):
    SELECT ... WHERE n.path LIKE r.path || '/%'
where ``r.path`` could contain ``_`` (legal in names, e.g. ``Site_A``,
``bench_root_200``). ``_`` is a LIKE metacharacter matching any single
character, so the pattern ``Site_A/%`` also matches paths under a sibling
named ``SiteXA``. Reads return cross-subtree rows; the two ``UPDATE``
statements in ``rename`` / ``move_to`` rewrite paths in unrelated subtrees
(silent data corruption).

Fixture is one tree with two siblings that lexically alias under a buggy
LIKE, ``Site_A`` and ``SiteXA``, each with leaf children. Every test
goes through the public ``energydb`` API the same way a user would.

LIKE-on-path sites covered (5 of 7 directly; the remaining 2 share the
same SQL pattern after the fix; see the in-test comments):
  paths.resolve_subtree_uuids       → client.query_nodes(within=...)
  client.get_tree                   → client.get_tree(...)
  scope.NodeScope.descendants       → client.get_node(...).descendants()
  scope.NodeScope.move_to cycle     → client.get_node(...).move_to(...)
  scope.rename + move_to UPDATE     → client.get_node(...).rename(...) / .move_to(...)
"""

from __future__ import annotations

import os

import energydb as edb
import psycopg
import pytest
from energydb import Client

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set: skipping wildcard-safety tests",
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
def tree(client):
    """``root/{Site_A, SiteXA}`` each with two turbines.

    ``Site_A`` contains a LIKE metacharacter; ``SiteXA`` lexically aliases
    it under the buggy un-escaped ``LIKE 'Site_A/%'`` (the ``_`` matches the
    ``X``). All seven LIKE-on-path sites must distinguish the two subtrees.
    """
    client.register_tree(
        edb.Portfolio(
            name="root",
            members=[
                edb.Site(
                    name="Site_A",
                    members=[
                        edb.wind.WindTurbine(name="T01", capacity=3.5),
                        edb.wind.WindTurbine(name="T02", capacity=3.5),
                    ],
                ),
                edb.Site(
                    name="SiteXA",
                    members=[
                        edb.wind.WindTurbine(name="T03", capacity=3.5),
                        edb.wind.WindTurbine(name="T04", capacity=3.5),
                    ],
                ),
            ],
        )
    )
    return client


def _fetch_paths() -> dict[str, str]:
    """Public-API users wouldn't reach for this, but inspecting raw paths is
    the most direct way to assert that no cross-subtree corruption happened
    on rename/move. Read-only."""
    with psycopg.connect(os.environ["TIMEDB_PG_DSN"]) as conn:
        rows = conn.execute("SELECT name, path FROM energydb.node").fetchall()
    return dict(rows)


# ---------------------------------------------------------------------------
# query_nodes(within=…) → paths.resolve_subtree_uuids
# ---------------------------------------------------------------------------


def test_query_nodes_within_excludes_lex_alias(tree):
    nodes = tree.query_nodes(within="root/Site_A")
    names = {n.name for n in nodes}
    # Site_A itself + its two turbines. SiteXA and T03/T04 are siblings'
    # subtree: buggy LIKE would include them via _ matching X.
    assert names == {"Site_A", "T01", "T02"}


# ---------------------------------------------------------------------------
# get_tree(…) → client.get_tree
# ---------------------------------------------------------------------------


def test_get_tree_excludes_lex_alias(tree):
    rebuilt = tree.get_tree("root", "Site_A")

    # Walk the reconstructed EDM tree and collect every name.
    def _walk(node):
        yield node.name
        for child in getattr(node, "members", None) or ():
            yield from _walk(child)

    names = set(_walk(rebuilt))
    assert names == {"Site_A", "T01", "T02"}


# ---------------------------------------------------------------------------
# NodeScope.descendants() → scope.descendants subtree LIKE
# ---------------------------------------------------------------------------


def test_descendants_excludes_lex_alias(tree):
    nodes = tree.get_node("root", "Site_A").descendants()
    names = {n["name"] for n in nodes}
    # descendants() excludes self by design: just the two turbines.
    assert names == {"T01", "T02"}


# ---------------------------------------------------------------------------
# move_to cycle check → scope.move_to cycle-check LIKE
# ---------------------------------------------------------------------------


def test_move_to_cycle_check_does_not_false_positive_on_lex_alias(tree):
    # Move Site_A under a node inside the SiteXA subtree (specifically T03).
    # Buggy code would compute the cycle check as:
    #     T03.path = 'root/SiteXA/T03' LIKE 'root/Site_A/%'   (_ matches X)
    # and reject the move as creating a cycle. Fixed code escapes _ and
    # the match correctly fails: the move proceeds.
    tree.get_node("root", "Site_A").move_to(tree.get_node("root", "SiteXA", "T03"))

    paths = _fetch_paths()
    assert paths["Site_A"] == "root/SiteXA/T03/Site_A"
    assert paths["T01"] == "root/SiteXA/T03/Site_A/T01"
    assert paths["T02"] == "root/SiteXA/T03/Site_A/T02"


# ---------------------------------------------------------------------------
# rename UPDATE → silent corruption of the SiteXA subtree
# ---------------------------------------------------------------------------


def test_rename_does_not_corrupt_lex_alias_subtree(tree):
    before = _fetch_paths()
    sitexa_before = {k: v for k, v in before.items() if k in ("SiteXA", "T03", "T04")}

    tree.get_node("root", "Site_A").rename("Site_B")

    after = _fetch_paths()
    # Site_A's subtree is rewritten under its new name.
    assert after["Site_B"] == "root/Site_B"
    assert after["T01"] == "root/Site_B/T01"
    assert after["T02"] == "root/Site_B/T02"
    # SiteXA's subtree is byte-identical to before.
    sitexa_after = {k: v for k, v in after.items() if k in ("SiteXA", "T03", "T04")}
    assert sitexa_after == sitexa_before


# ---------------------------------------------------------------------------
# move_to UPDATE → silent corruption of the SiteXA subtree
# ---------------------------------------------------------------------------


def test_move_does_not_corrupt_lex_alias_subtree(tree):
    tree.register_tree(edb.Portfolio(name="other_root"))

    before = _fetch_paths()
    sitexa_before = {k: v for k, v in before.items() if k in ("SiteXA", "T03", "T04")}

    tree.get_node("root", "Site_A").move_to(tree.get_node("other_root"))

    after = _fetch_paths()
    assert after["Site_A"] == "other_root/Site_A"
    assert after["T01"] == "other_root/Site_A/T01"
    assert after["T02"] == "other_root/Site_A/T02"
    # SiteXA's subtree is untouched.
    sitexa_after = {k: v for k, v in after.items() if k in ("SiteXA", "T03", "T04")}
    assert sitexa_after == sitexa_before
