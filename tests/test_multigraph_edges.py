"""Multigraph edges: parallel circuits between the same endpoint pair.

``edge_uniq`` is ``(edge_type, from_node_uuid, to_node_uuid, name)`` with
``NULLS NOT DISTINCT``, so a corridor can carry six circuits that differ only
by name — the shape a real transmission network has and a simple graph cannot
represent. The interesting half is not the constraint but the *addressing*:
every triple-addressed path either resolves a unique edge or raises
:class:`~energydb.errors.AmbiguousEdgeError`; none of them silently picks one.

Live integration tests — skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL``
aren't set.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import energydatamodel as edm
import energydb as edb
import polars as pl
import psycopg
import pytest
from energydatamodel.reference import Reference
from energydb import Client
from energydb.errors import AlreadyExistsError, AmbiguousEdgeError, EdgeNotFoundError

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set — skipping multigraph edge tests",
        allow_module_level=True,
    )

BASE = datetime(2026, 1, 1, tzinfo=UTC)

# The corridor from the issue: six parallel circuits NO2-420 → NO1-420.
CIRCUITS = ("circuit-1", "circuit-2", "circuit-3", "circuit-4", "circuit-5", "circuit-6")
FROM_PATH = "Grid/NO2-420"
TO_PATH = "Grid/NO1-420"


def _values(n: int = 3, offset: float = 0.0) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "valid_time": [BASE + timedelta(hours=i) for i in range(n)],
            "value": [offset + i for i in range(n)],
        }
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
def corridor(client):
    """Two buses and six parallel ``Line`` circuits between them."""
    bus_a = edb.grid.JunctionPoint(name="NO2-420")
    bus_b = edb.grid.JunctionPoint(name="NO1-420")
    client.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b]))
    uuids = {}
    for i, circuit in enumerate(CIRCUITS):
        uuids[circuit] = client.create_edge(
            edb.grid.Line(
                name=circuit,
                capacity=400 + i,
                from_element=Reference(bus_a),
                to_element=Reference(bus_b),
            )
        )
    return uuids


def _register_and_write(client, *, name: str, offset: float) -> None:
    """One FLAT series named ``flow`` on the ``name`` circuit, with data."""
    scope = client.get_edge(FROM_PATH, TO_PATH, type="Line", name=name)
    scope.register_series(
        name="flow",
        canonical_unit="MW",
        data_type="actual",
        timeseries_type="FLAT",
    )
    scope.write(_values(offset=offset), data_type="actual", name="flow")


# ---------------------------------------------------------------------------
# The constraint itself
# ---------------------------------------------------------------------------


def test_parallel_named_edges_are_accepted(client, corridor):
    """Six circuits on one endpoint pair — the whole point."""
    assert len(set(corridor.values())) == len(CIRCUITS)
    lines = client.query_edges(type="Line")
    assert sorted(line.name for line in lines) == sorted(CIRCUITS)


def test_duplicate_quadruple_is_rejected(client, corridor):
    """Same (type, from, to, name) as an existing edge, new uuid → conflict."""
    bus_a = client.get_node("Grid", "NO2-420").get_raw()
    bus_b = client.get_node("Grid", "NO1-420").get_raw()
    with pytest.raises(AlreadyExistsError, match="already exists"):
        client.create_edge(
            edb.grid.Line(
                name="circuit-1",
                capacity=999,
                from_element=Reference(bus_a["uuid"]),
                to_element=Reference(bus_b["uuid"]),
            )
        )


def test_two_unnamed_edges_are_still_rejected(client):
    """``NULLS NOT DISTINCT``: at most one *unnamed* edge per (type, from, to).

    Plain NULL semantics would allow unlimited unnamed duplicates — strictly
    worse than the simple-graph constraint this replaced.
    """
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
    client.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b]))

    client.create_edge(edm.Edge(from_element=Reference(bus_a), to_element=Reference(bus_b)))
    with pytest.raises(AlreadyExistsError, match="distinct"):
        client.create_edge(edm.Edge(from_element=Reference(bus_a), to_element=Reference(bus_b)))


def test_an_unnamed_edge_coexists_with_named_parallels(client, corridor):
    """The unnamed edge is one more parallel edge, not a conflicting one."""
    bus_a = client.get_node("Grid", "NO2-420").get_raw()
    bus_b = client.get_node("Grid", "NO1-420").get_raw()
    client.create_edge(
        edb.grid.Line(
            capacity=100,
            from_element=Reference(bus_a["uuid"]),
            to_element=Reference(bus_b["uuid"]),
        )
    )
    assert len(client.query_edges(type="Line")) == len(CIRCUITS) + 1


def test_same_quadruple_in_two_namespaces_coexists(client):
    """``namespace`` needs no seat in the key: the endpoint uuids differ.

    Two tenants each register ``Grid/BusA → Grid/BusB`` with an identically
    named ``Line``. The *paths* collide, the node uuids do not, so the two
    edges are distinct rows.
    """
    for ns in ("tenant-a", "tenant-b"):
        view = client.namespace(ns)
        bus_a = edb.grid.JunctionPoint(name="BusA")
        bus_b = edb.grid.JunctionPoint(name="BusB")
        view.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b]))
        view.create_edge(
            edb.grid.Line(name="circuit-1", capacity=400, from_element=Reference(bus_a), to_element=Reference(bus_b))
        )

    with psycopg.connect(os.environ["TIMEDB_PG_DSN"]) as conn:
        rows = conn.execute(
            f"SELECT namespace, name FROM {_P}edge WHERE edge_type = 'Line' ORDER BY namespace"
        ).fetchall()
    assert rows == [("tenant-a", "circuit-1"), ("tenant-b", "circuit-1")]


# ---------------------------------------------------------------------------
# Addressing
# ---------------------------------------------------------------------------


def test_unique_triple_still_resolves(client):
    """The pre-multigraph corpus is untouched: one edge on a triple resolves."""
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
    client.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b]))
    edge_uuid = client.create_edge(
        edb.grid.Line(name="only", capacity=400, from_element=Reference(bus_a), to_element=Reference(bus_b))
    )

    assert client.get_edge("Grid/BusA", "Grid/BusB", type="Line").get().id == edge_uuid


def test_ambiguous_triple_raises_with_every_candidate(client, corridor):
    with pytest.raises(AmbiguousEdgeError) as excinfo:
        client.get_edge(FROM_PATH, TO_PATH, type="Line").get()

    err = excinfo.value
    assert err.from_path == FROM_PATH
    assert err.to_path == TO_PATH
    assert err.edge_type == "Line"
    assert [m["name"] for m in err.matches] == list(CIRCUITS)
    assert {m["uuid"] for m in err.matches} == set(corridor.values())
    # The fix is in the message.
    assert "name=" in str(err)


def test_ambiguous_triple_is_a_validation_error(client, corridor):
    """Ambiguity is a caller-side addressing bug, not a missing entity."""
    assert issubclass(AmbiguousEdgeError, edb.ValidationError)
    with pytest.raises(ValueError):  # noqa: PT011 -- the ValueError compat base is the point
        client.get_edge(FROM_PATH, TO_PATH, type="Line").get_raw()


def test_quadruple_resolves_each_circuit(client, corridor):
    for i, circuit in enumerate(CIRCUITS):
        line = client.get_edge(FROM_PATH, TO_PATH, type="Line", name=circuit).get()
        assert line.id == corridor[circuit]
        assert line.capacity == 400 + i


def test_unknown_name_raises_not_found_carrying_the_name(client, corridor):
    with pytest.raises(EdgeNotFoundError) as excinfo:
        client.get_edge(FROM_PATH, TO_PATH, type="Line", name="circuit-7").get()
    assert excinfo.value.name == "circuit-7"
    assert excinfo.value.edge_type == "Line"


def test_name_and_uuid_are_mutually_exclusive(client, corridor):
    with pytest.raises(edb.ValidationError, match="not both"):
        client.get_edge(uuid=corridor["circuit-1"], name="circuit-1")


def test_scope_repr_shows_the_name(client, corridor):
    scope = client.get_edge(FROM_PATH, TO_PATH, type="Line", name="circuit-2")
    assert "name='circuit-2'" in repr(scope)
    assert "name=" not in repr(client.get_edge(FROM_PATH, TO_PATH, type="Line"))


# ---------------------------------------------------------------------------
# Manifest routing
# ---------------------------------------------------------------------------


def _manifest(*names: str | None, with_edge_name: bool = True) -> pl.DataFrame:
    data: dict[str, list] = {
        "from_path": [FROM_PATH] * len(names),
        "to_path": [TO_PATH] * len(names),
        "edge_type": ["Line"] * len(names),
        "data_type": ["actual"] * len(names),
        "name": ["flow"] * len(names),
    }
    if with_edge_name:
        data["edge_name"] = list(names)
    return pl.DataFrame(data, schema_overrides={"edge_name": pl.Utf8} if with_edge_name else None)


def test_edge_name_column_routes_to_the_right_circuit(client, corridor):
    _register_and_write(client, name="circuit-1", offset=10.0)
    _register_and_write(client, name="circuit-2", offset=20.0)

    out = client.read(_manifest("circuit-1", "circuit-2"))
    assert set(out.columns) >= {"from_path", "to_path", "edge_type", "edge_name", "data_type", "name", "value"}
    by_circuit = {
        circuit: sorted(sub["value"].to_list())
        for circuit, sub in ((c, out.filter(pl.col("edge_name") == c)) for c in ("circuit-1", "circuit-2"))
    }
    assert by_circuit == {"circuit-1": [10.0, 11.0, 12.0], "circuit-2": [20.0, 21.0, 22.0]}


def test_null_edge_name_routes_to_the_unnamed_edge(client, corridor):
    """Null in the column means "the unnamed edge", not "any edge"."""
    bus_a = client.get_node("Grid", "NO2-420").get_raw()
    bus_b = client.get_node("Grid", "NO1-420").get_raw()
    client.create_edge(
        edb.grid.Line(
            capacity=100,
            from_element=Reference(bus_a["uuid"]),
            to_element=Reference(bus_b["uuid"]),
        )
    )
    unnamed = client.get_edge(FROM_PATH, TO_PATH, type="Line", name=None)  # still ambiguous without a name
    with pytest.raises(AmbiguousEdgeError):
        unnamed.get_raw()

    _register_and_write(client, name="circuit-1", offset=10.0)
    # The unnamed edge, addressed by uuid, gets its own series + data.
    unnamed_uuid = next(e.id for e in client.query_edges(type="Line") if e.name is None)
    scope = client.get_edge(uuid=unnamed_uuid)
    scope.register_series(name="flow", canonical_unit="MW", data_type="actual", timeseries_type="FLAT")
    scope.write(_values(offset=99.0), data_type="actual", name="flow")

    out = client.read(_manifest("circuit-1", None))
    assert out.filter(pl.col("edge_name").is_null())["value"].to_list() == [99.0, 100.0, 101.0]
    assert out.filter(pl.col("edge_name") == "circuit-1")["value"].to_list() == [10.0, 11.0, 12.0]


def test_manifest_without_edge_name_on_parallel_edges_raises(client, corridor):
    _register_and_write(client, name="circuit-1", offset=10.0)

    with pytest.raises(AmbiguousEdgeError) as excinfo:
        client.read(_manifest("circuit-1", with_edge_name=False))
    assert [m["name"] for m in excinfo.value.matches] == list(CIRCUITS)
    assert "edge_name" in str(excinfo.value)


def test_ambiguity_is_caught_even_when_one_sibling_has_the_series(client, corridor):
    """Only ``circuit-1`` carries ``flow``, but the *address* is still ambiguous.

    The resolver LEFT-JOINs series precisely so a nameless triple can't quietly
    resolve to "the only circuit that happens to have this series".
    """
    _register_and_write(client, name="circuit-1", offset=10.0)
    with pytest.raises(AmbiguousEdgeError):
        client.read(_manifest("circuit-1", with_edge_name=False), on_missing="skip")


def test_edge_name_without_the_triple_is_a_manifest_error(client):
    manifest = pl.DataFrame(
        {
            "edge_name": ["circuit-1"],
            "data_type": ["actual"],
            "name": ["flow"],
        }
    )
    with pytest.raises(edb.ManifestError, match="edge_name"):
        client.read(manifest)


def test_write_and_read_round_trip_across_parallel_circuits(client, corridor):
    """A manifest write routed by ``edge_name`` lands on the right circuits."""
    for circuit in CIRCUITS:
        client.get_edge(FROM_PATH, TO_PATH, type="Line", name=circuit).register_series(
            name="flow",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
        )

    frames = []
    for i, circuit in enumerate(CIRCUITS):
        frames.append(
            _values(offset=100.0 * (i + 1)).with_columns(
                pl.lit(FROM_PATH).alias("from_path"),
                pl.lit(TO_PATH).alias("to_path"),
                pl.lit("Line").alias("edge_type"),
                pl.lit(circuit).alias("edge_name"),
                pl.lit("actual").alias("data_type"),
                pl.lit("flow").alias("name"),
            )
        )
    client.write(pl.concat(frames))

    out = client.read(_manifest(*CIRCUITS))
    for i, circuit in enumerate(CIRCUITS):
        got = out.filter(pl.col("edge_name") == circuit).sort("valid_time")["value"].to_list()
        assert got == [100.0 * (i + 1) + j for j in range(3)]


# ---------------------------------------------------------------------------
# Result schema
# ---------------------------------------------------------------------------


def test_edge_series_key_has_six_fields(client, corridor):
    _register_and_write(client, name="circuit-1", offset=10.0)
    _register_and_write(client, name="circuit-2", offset=20.0)

    out = client.read(_manifest("circuit-1", "circuit-2"), output="by_path")
    assert all(isinstance(k, edb.EdgeSeriesKey) and len(k) == 6 for k in out)
    key = edb.EdgeSeriesKey(
        from_path=FROM_PATH,
        to_path=TO_PATH,
        edge_type="Line",
        edge_name="circuit-2",
        data_type="actual",
        name="flow",
    )
    assert out[key]["value"].to_list() == [20.0, 21.0, 22.0]


def test_by_path_keys_do_not_collide_across_parallel_circuits(client, corridor):
    """Without ``edge_name`` in the key these two would overwrite each other."""
    _register_and_write(client, name="circuit-1", offset=10.0)
    _register_and_write(client, name="circuit-2", offset=20.0)

    out = client.read(_manifest("circuit-1", "circuit-2"), output="by_path")
    assert len(out) == 2
    assert {k.edge_name for k in out} == {"circuit-1", "circuit-2"}


def test_find_filters_by_edge_name(client, corridor):
    _register_and_write(client, name="circuit-1", offset=10.0)
    _register_and_write(client, name="circuit-2", offset=20.0)

    out = client.read(_manifest("circuit-1", "circuit-2"), output="by_path")
    matches = edb.find(out, edge_name="circuit-2")
    assert len(matches) == 1
    assert matches[0][1]["value"].to_list() == [20.0, 21.0, 22.0]


def test_edge_scope_read_carries_edge_name(client, corridor):
    """A scope read is edge-routed too: the column is always present."""
    _register_and_write(client, name="circuit-3", offset=30.0)
    out = client.get_edge(FROM_PATH, TO_PATH, type="Line", name="circuit-3").read()
    assert out["edge_name"].unique().to_list() == ["circuit-3"]


def test_scope_read_on_an_ambiguous_triple_raises(client, corridor):
    """The read path resolves the edge too — it must not pick one either."""
    _register_and_write(client, name="circuit-1", offset=10.0)
    with pytest.raises(AmbiguousEdgeError):
        client.get_edge(FROM_PATH, TO_PATH, type="Line").read(data_type="actual", name="flow")


def test_resolve_then_read_from_meta_keeps_the_edge_name(client, corridor):
    """The resolve/read split (authorize-before-read) carries ``edge_name``."""
    _register_and_write(client, name="circuit-2", offset=20.0)
    scope = client.get_edge(FROM_PATH, TO_PATH, type="Line", name="circuit-2")
    meta = scope.resolve()
    assert meta["edge_name"].to_list() == ["circuit-2"]

    out = scope.read_from_meta(meta, output="by_path")
    assert [k.edge_name for k in out] == ["circuit-2"]


def test_edge_read_output_feeds_back_in_as_a_manifest(client, corridor):
    """The output columns *are* the routing columns — including ``edge_name``."""
    _register_and_write(client, name="circuit-1", offset=10.0)
    first = client.read(_manifest("circuit-1"))
    again = client.read(first.select(["from_path", "to_path", "edge_type", "edge_name", "data_type", "name"]).unique())
    assert sorted(again["value"].to_list()) == sorted(first["value"].to_list())


# ---------------------------------------------------------------------------
# Mutations
# ---------------------------------------------------------------------------


def test_rename_onto_an_occupied_quadruple_raises(client, corridor):
    with pytest.raises(AlreadyExistsError, match="distinct"):
        client.get_edge(uuid=corridor["circuit-1"]).rename("circuit-2")


def test_rename_to_a_free_name_succeeds(client, corridor):
    client.get_edge(uuid=corridor["circuit-1"]).rename("circuit-1a")
    assert client.get_edge(FROM_PATH, TO_PATH, type="Line", name="circuit-1a").get().id == corridor["circuit-1"]


def test_move_to_onto_an_occupied_quadruple_raises(client, corridor):
    """A third bus with a same-named circuit; moving onto it collides."""
    bus_c = edb.grid.JunctionPoint(name="NO3-420")
    client.register_tree(edb.Portfolio(name="Grid2", members=[bus_c]))
    bus_a = client.get_node("Grid", "NO2-420").get_raw()
    client.create_edge(
        edb.grid.Line(
            name="circuit-1",
            capacity=400,
            from_element=Reference(bus_a["uuid"]),
            to_element=Reference(bus_c),
        )
    )

    with pytest.raises(AlreadyExistsError, match="distinct"):
        client.get_edge(uuid=corridor["circuit-1"]).move_to(
            from_node=("Grid", "NO2-420"),
            to_node=("Grid2", "NO3-420"),
        )


def test_deleting_one_circuit_leaves_its_siblings(client, corridor):
    client.get_edge(FROM_PATH, TO_PATH, type="Line", name="circuit-4").delete()

    remaining = sorted(line.name for line in client.query_edges(type="Line"))
    assert remaining == [c for c in CIRCUITS if c != "circuit-4"]
    assert client.get_edge(FROM_PATH, TO_PATH, type="Line", name="circuit-5").get().id == corridor["circuit-5"]


# ---------------------------------------------------------------------------
# register_tree
# ---------------------------------------------------------------------------


def test_register_tree_loads_parallel_circuits(client):
    """The ``lines.csv`` shape: many endpoint pairs, several circuits each.

    The load must drop nothing — that is the 12% of the public European
    transmission network a simple graph cannot hold.
    """
    pairs = 100
    circuits_per_pair = 2
    buses = [edb.grid.JunctionPoint(name=f"Bus{i}") for i in range(pairs * 2)]
    lines = [
        edb.grid.Line(
            name=f"circuit-{c}",
            capacity=400,
            from_element=Reference(buses[2 * p]),
            to_element=Reference(buses[2 * p + 1]),
        )
        for p in range(pairs)
        for c in range(circuits_per_pair)
    ]
    client.register_tree(edb.Portfolio(name="Net", members=[*buses, *lines]))

    assert len(client.query_edges(type="Line")) == pairs * circuits_per_pair
    # Spot-check that both circuits of one pair are addressable.
    for c in range(circuits_per_pair):
        line = client.get_edge("Net/Bus0", "Net/Bus1", type="Line", name=f"circuit-{c}").get()
        assert line.name == f"circuit-{c}"


def test_register_tree_rejects_a_duplicate_quadruple_in_one_payload(client):
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
    tree = edb.Portfolio(
        name="Net",
        members=[
            bus_a,
            bus_b,
            edb.grid.Line(name="dup", capacity=1, from_element=Reference(bus_a), to_element=Reference(bus_b)),
            edb.grid.Line(name="dup", capacity=2, from_element=Reference(bus_a), to_element=Reference(bus_b)),
        ],
    )
    with pytest.raises(AlreadyExistsError, match="distinct"):
        client.register_tree(tree)


# ---------------------------------------------------------------------------
# Engine-parallel read path
# ---------------------------------------------------------------------------


def test_engine_read_of_parallel_circuits_matches_sequential(client, corridor, monkeypatch):
    """The engine predicate resolves the whole corridor; the trim keeps it exact.

    Skips if ClickHouse can't reach PostgreSQL from its own network vantage
    (local docker without ``ENERGYDB_CH_PG_HOST``), same as the other engine
    tests. Strict mode so a broken engine raises instead of silently falling
    back to the sequential path, which would trivially pass.
    """
    import energydb._io as _io
    from energydb._ch_meta_engine import CH_ENGINE_TABLE
    from polars.testing import assert_frame_equal

    _register_and_write(client, name="circuit-1", offset=10.0)
    _register_and_write(client, name="circuit-2", offset=20.0)

    try:
        client.setup_ch_meta_engine()
        client.td._ch.command(f"SELECT count() FROM {CH_ENGINE_TABLE}")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"CH meta engine not usable in this env: {exc}")

    client._async._engine_unavailable = True
    sequential = client.read(_manifest("circuit-1", "circuit-2"))

    monkeypatch.setattr(_io, "_ENGINE_STRICT", True)
    client._async._engine_unavailable = False
    engine = client.read(_manifest("circuit-1", "circuit-2"))

    assert_frame_equal(engine, sequential)
    assert set(engine["edge_name"].to_list()) == {"circuit-1", "circuit-2"}


def test_unprovisioned_engine_table_degrades_to_the_sequential_read(client, corridor):
    """A missing engine table is a configuration state, not a failure."""
    from energydb._ch_meta_engine import DROP_ENGINE_TABLE

    _register_and_write(client, name="circuit-1", offset=10.0)
    expected = client.read(_manifest("circuit-1"))

    client.td._ch.command(DROP_ENGINE_TABLE)
    client._async._engine_unavailable = False
    degraded = client.read(_manifest("circuit-1"))

    assert degraded["value"].to_list() == expected["value"].to_list()
    assert degraded["edge_name"].to_list() == expected["edge_name"].to_list()
    client.setup_ch_meta_engine()


# ---------------------------------------------------------------------------
# series_meta view
# ---------------------------------------------------------------------------


def test_series_meta_view_exposes_edge_name(client, corridor):
    _register_and_write(client, name="circuit-1", offset=10.0)
    with psycopg.connect(os.environ["TIMEDB_PG_DSN"]) as conn:
        rows = conn.execute(f"SELECT edge_name FROM {_P}series_meta WHERE edge_uuid IS NOT NULL").fetchall()
    assert rows == [("circuit-1",)]


def _schema_prefix() -> str:
    from energydb.models import SQL_SCHEMA_PREFIX

    return SQL_SCHEMA_PREFIX


_P = _schema_prefix()


def test_uuid_and_name_addressing_agree(client, corridor):
    """Sanity: the two addressings resolve to the same row for every circuit."""
    for circuit, edge_uuid in corridor.items():
        assert isinstance(edge_uuid, UUID)
        by_name = client.get_edge(FROM_PATH, TO_PATH, type="Line", name=circuit).get_raw()
        by_uuid = client.get_edge(uuid=edge_uuid).get_raw()
        assert by_name == by_uuid


def test_unrelated_uuid_is_still_a_not_found(client):
    assert client.get_edge(uuid=uuid4()).get_raw() is None
