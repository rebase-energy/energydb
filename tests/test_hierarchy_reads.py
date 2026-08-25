"""Contract tests for the single-round-trip hierarchy reads.

get / get_raw / children / descendants / path / get_tree / query_nodes /
query_edges / get_subtree_raw resolve the scope inside the main statement
(no separate ``resolve_node_uuid`` query) and run on autocommit connections.
These tests pin the behavioral contracts that must survive that collapse:

* not-found semantics per addressing form (path-addressed raises the
  resolve-style ValueError, uuid-addressed returns empty/None);
* empty results distinguishable from missing roots;
* result ordering and type filters;
* txn-bound scopes read through the transaction's connection and see its
  uncommitted mutations.

Live integration tests: skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL``
aren't set.
"""

from __future__ import annotations

import os
from uuid import uuid4

import energydb as edb
import pytest
from energydatamodel.reference import Reference
from energydb import Client

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set: skipping hierarchy-read tests",
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
    """P → S → (T1, T2) plus grid nodes with an edge, series on T1."""
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.Site(
                name="S",
                members=[
                    edb.wind.WindTurbine(
                        name="T1",
                        capacity=3.5,
                        timeseries=[edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
                    ),
                    edb.wind.WindTurbine(name="T2", capacity=3.5),
                ],
            )
        ],
    )
    client.register_tree(tree)
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
    client.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b]))
    line = edb.grid.Line(name="L1", capacity=500, from_element=Reference(bus_a), to_element=Reference(bus_b))
    client.create_edge(line)
    return client, tree, (bus_a, bus_b, line)


# ---------------------------------------------------------------------------
# not-found contracts per addressing form
# ---------------------------------------------------------------------------


def test_path_addressed_missing_raises(populated):
    client, _tree, _grid = populated
    for call in (
        lambda s: s.get(),
        lambda s: s.get_raw(),
        lambda s: s.children(),
        lambda s: s.descendants(),
        lambda s: s.path(),
    ):
        with pytest.raises(ValueError, match="Node not found: P/nope"):
            call(client.get_node("P", "nope"))


def test_relative_path_missing_raises_with_start(populated):
    client, tree, _grid = populated
    root_scope = client.get_node(uuid=tree.id)
    with pytest.raises(ValueError, match="relative to"):
        root_scope.get_node("nope").get()


def test_uuid_addressed_missing_is_empty(populated):
    client, _tree, _grid = populated
    ghost = client.get_node(uuid=uuid4())
    assert ghost.get_raw() is None
    assert ghost.children() == []
    assert ghost.descendants() == []
    with pytest.raises(ValueError, match="Node not found: uuid="):
        ghost.get()
    with pytest.raises(ValueError, match="Node not found: uuid="):
        ghost.path()


# ---------------------------------------------------------------------------
# children / descendants: emptiness, ordering, type filter
# ---------------------------------------------------------------------------


def test_children_of_leaf_is_empty_not_error(populated):
    client, _tree, _grid = populated
    assert client.get_node("P/S/T1").children() == []
    assert client.get_node("P/S/T1").descendants() == []


def test_children_ordering_and_type_filter(populated):
    client, _tree, _grid = populated
    kids = client.get_node("P/S").children()
    assert [k["name"] for k in kids] == ["T1", "T2"]
    assert [k["name"] for k in client.get_node("P/S").children(type="WindTurbine")] == ["T1", "T2"]
    assert client.get_node("P/S").children(type="Battery") == []


def test_descendants_excludes_root_and_filters(populated):
    client, tree, _grid = populated
    names = {d["name"] for d in client.get_node("P").descendants()}
    assert names == {"S", "T1", "T2"}
    turbines = client.get_node(uuid=tree.id).descendants(type="WindTurbine")
    assert {d["name"] for d in turbines} == {"T1", "T2"}


def test_get_and_path_by_all_addressings(populated):
    client, tree, _grid = populated
    assert client.get_node("P/S/T1").get().name == "T1"
    t1_uuid = client.get_node("P/S/T1").get_raw()["uuid"]
    assert client.get_node(uuid=t1_uuid).get().name == "T1"
    assert client.get_node(uuid=tree.id).get_node("S/T1").get().name == "T1"
    assert client.get_node(uuid=t1_uuid).path() == ("P", "S", "T1")


# ---------------------------------------------------------------------------
# get_tree / get_subtree_raw
# ---------------------------------------------------------------------------


def test_get_tree_missing_raises_both_forms(populated):
    client, _tree, _grid = populated
    with pytest.raises(ValueError, match="Node not found: nope"):
        client.get_tree("nope")
    with pytest.raises(ValueError, match="Node not found: uuid="):
        client.get_tree(uuid=uuid4())


def test_get_tree_with_series(populated):
    client, _tree, _grid = populated
    rebuilt = client.get_tree("P", include_series=True)
    t1 = next(n for n in rebuilt.children()[0].children() if n.name == "T1")
    assert any(ts.name == "power" for ts in t1.timeseries or [])
    t2 = next(n for n in rebuilt.children()[0].children() if n.name == "T2")
    assert not (t2.timeseries or [])


def test_get_subtree_raw_includes_root_and_missing_is_empty(populated):
    client, tree, _grid = populated
    rows = client.get_subtree_raw(tree.id)
    assert [r["path"] for r in rows] == ["P", "P/S", "P/S/T1", "P/S/T2"]
    assert client.get_subtree_raw(uuid4()) == []


# ---------------------------------------------------------------------------
# query_nodes / query_edges within=
# ---------------------------------------------------------------------------


def test_query_nodes_within_contracts(populated):
    client, tree, _grid = populated
    assert {n.name for n in client.query_nodes(within="P")} == {"P", "S", "T1", "T2"}
    # subtree exists but the filter matches nothing → empty, no error
    assert client.query_nodes(type="Battery", within="P") == []
    # missing path raises; missing uuid is empty, by contract
    with pytest.raises(ValueError, match="Node not found: nope"):
        client.query_nodes(within="nope")
    assert client.query_nodes(within=uuid4()) == []
    assert {n.name for n in client.query_nodes(within=tree.id)} == {"P", "S", "T1", "T2"}


def test_query_edges_within_contracts(populated):
    client, _tree, (_a, _b, line) = populated
    # both endpoints inside the subtree → the edge appears exactly once
    edges = client.query_edges(within="Grid")
    assert [e.id for e in edges] == [line.id]
    assert client.query_edges(type="Line", within="Grid")[0].id == line.id
    assert client.query_edges(type="nonexistent", within="Grid") == []
    with pytest.raises(ValueError, match="Node not found: nope"):
        client.query_edges(within="nope")
    assert client.query_edges(within=uuid4()) == []
    # a subtree containing no endpoints → empty
    assert client.query_edges(within="P") == []


# ---------------------------------------------------------------------------
# edge get: triple + uuid forms
# ---------------------------------------------------------------------------


def test_edge_get_by_triple_and_uuid(populated):
    client, _tree, (_a, _b, line) = populated
    by_triple = client.get_edge("Grid/BusA", "Grid/BusB", type="Line").get()
    assert by_triple.id == line.id
    assert client.get_edge(uuid=line.id).get().id == line.id
    with pytest.raises(ValueError, match="Edge not found: type='nonexistent'"):
        client.get_edge("Grid/BusA", "Grid/BusB", type="nonexistent").get()
    with pytest.raises(ValueError, match="Node not found|resolve"):
        client.get_edge("Grid/BusA", "Grid/nope", type="Line").get()
    with pytest.raises(ValueError, match="Edge not found: uuid="):
        client.get_edge(uuid=uuid4()).get()


# ---------------------------------------------------------------------------
# txn-bound scopes read the transaction's uncommitted state
# ---------------------------------------------------------------------------


def test_txn_hierarchy_reads_see_uncommitted(populated):
    client, _tree, _grid = populated
    with client.transaction() as txn:
        txn.register_tree(edb.Site(name="S2"), under=["P"])
        # the txn scope sees the uncommitted node ...
        assert {c["name"] for c in txn.get_node("P").children()} == {"S", "S2"}
        assert "S2" in {d["name"] for d in txn.get_node("P").descendants()}
        assert txn.get_node("P/S2").get_raw() is not None
        # ... while a plain (autocommit) scope does not
        assert {c["name"] for c in client.get_node("P").children()} == {"S"}
        txn.commit()
    assert {c["name"] for c in client.get_node("P").children()} == {"S", "S2"}
