"""Tests for ``register_tree`` and edge support on ``Client``.

Mocked-pool unit tests for the tree-walking, plus a small set of live
integration tests when ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL`` are set.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import energydb as edb
import polars as pl
import pytest
from energydatamodel.reference import Reference
from energydb import Client
from energydb.client import AsyncClient
from energydb.models import SQL_SCHEMA_PREFIX as P

# ---------------------------------------------------------------------------
# Unit tests: mocked pool / td
# ---------------------------------------------------------------------------


def _simple_df(n: int = 3) -> pl.DataFrame:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    return pl.DataFrame(
        {
            "valid_time": [base + timedelta(hours=i) for i in range(n)],
            "value": [float(i) for i in range(n)],
        }
    )


def _make_conn(*, fetchall=None, execute_side_effect=None) -> MagicMock:
    """An async-aware mock connection for the ``await (await execute).fetch*()`` path.

    ``commit``/``rollback`` are awaitable; ``execute`` returns a cursor whose
    ``fetchall``/``fetchone`` are awaitable. Pass ``execute_side_effect`` to
    dispatch cursors per-SQL (e.g. path resolution vs existence checks).

    ``conn.cursor()`` is an async context manager yielding a cursor whose
    ``executemany`` is awaitable, as the batched ``register_tree`` insert
    path needs.
    The cursor is exposed as ``conn._batch_cursor`` for assertions.
    """
    conn = MagicMock()
    conn.commit = AsyncMock()
    conn.rollback = AsyncMock()
    if execute_side_effect is not None:
        conn.execute = AsyncMock(side_effect=execute_side_effect)
    else:
        cursor = MagicMock()
        cursor.fetchall = AsyncMock(return_value=fetchall if fetchall is not None else [])
        cursor.fetchone = AsyncMock(return_value=None)
        conn.execute = AsyncMock(return_value=cursor)
    batch_cursor = MagicMock()
    batch_cursor.executemany = AsyncMock()
    cursor_cm = conn.cursor.return_value
    cursor_cm.__aenter__ = AsyncMock(return_value=batch_cursor)
    cursor_cm.__aexit__ = AsyncMock(return_value=None)
    pipeline_cm = conn.pipeline.return_value
    pipeline_cm.__aenter__ = AsyncMock(return_value=None)
    pipeline_cm.__aexit__ = AsyncMock(return_value=None)
    conn._batch_cursor = batch_cursor
    return conn


def _target_table(sql: str) -> str:
    """The relation an ``INSERT INTO`` targets, schema prefix stripped.

    energydb's SQL is schema-qualified, so the target reads ``energydb.node``
    under ``ENERGYDB_SCHEMA=energydb`` and plain ``node`` by default. These
    assertions are about *which* table, not how it is spelled."""
    return sql.split("INSERT INTO ")[1].split(" ")[0].removeprefix(P)


def _batches(conn: MagicMock) -> dict[str, list[tuple]]:
    """The rows passed to each batched INSERT, keyed by target table."""
    out: dict[str, list[tuple]] = {}
    for call in conn._batch_cursor.executemany.call_args_list:
        sql, rows = call.args
        out[_target_table(sql)] = list(rows)
    return out


def _async_pool(conn: MagicMock) -> MagicMock:
    """A mock pool whose ``connection()`` is an async context manager yielding ``conn``."""
    pool = MagicMock()
    cm = pool.connection.return_value
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=None)
    return pool


def _mock_client(monkeypatch, conn: MagicMock | None = None) -> AsyncClient:
    """Build an AsyncClient with an async-aware mock pool and td."""
    monkeypatch.setattr(AsyncClient, "__init__", lambda self: None)
    client = AsyncClient()  # type: ignore[call-arg]
    client._pool = _async_pool(conn if conn is not None else _make_conn())
    client.td = MagicMock()
    return client


class TestRegisterSeriesShapeDefault:
    """``series_mod.register_series`` derives the retention default from the
    series shape when the caller passes ``retention=None``."""

    @staticmethod
    def _mock_conn(returned_sid: int = 1):
        cursor = MagicMock()
        cursor.fetchone = AsyncMock(return_value=(returned_sid,))
        conn = MagicMock()
        conn.execute = AsyncMock(return_value=cursor)
        return conn

    def test_flat_defaults_to_forever(self):
        from uuid import uuid4

        from energydb import series as series_mod

        conn = self._mock_conn()
        asyncio.run(
            series_mod.register_series(
                conn,
                owner_col="node_uuid",
                owner_uuid=uuid4(),
                data_type="actual",
                name="power",
                canonical_unit="MW",
                timeseries_type="FLAT",
            )
        )
        params = conn.execute.call_args.args[1]
        # retention is the 7th positional INSERT param (0-indexed 6).
        assert params[6] == "forever"

    def test_overlapping_defaults_to_medium(self):
        from uuid import uuid4

        from energydb import series as series_mod

        conn = self._mock_conn()
        asyncio.run(
            series_mod.register_series(
                conn,
                owner_col="node_uuid",
                owner_uuid=uuid4(),
                data_type="forecast",
                name="power",
                canonical_unit="MW",
                timeseries_type="OVERLAPPING",
            )
        )
        params = conn.execute.call_args.args[1]
        assert params[6] == "medium"

    def test_explicit_retention_wins_over_default(self):
        from uuid import uuid4

        from energydb import series as series_mod

        conn = self._mock_conn()
        asyncio.run(
            series_mod.register_series(
                conn,
                owner_col="node_uuid",
                owner_uuid=uuid4(),
                data_type="actual",
                name="power",
                canonical_unit="MW",
                timeseries_type="FLAT",
                retention="short",
            )
        )
        params = conn.execute.call_args.args[1]
        assert params[6] == "short"


class TestRegisterTree:
    def test_batches_every_node_with_correct_parent_and_path(self, monkeypatch):
        """``register_tree`` inserts all nodes in ONE batched statement, in DFS
        order (parent before child), with client-side materialized paths."""
        client = _mock_client(monkeypatch)
        conn = _make_conn()
        client._pool = _async_pool(conn)

        portfolio = edb.Portfolio(name="P")
        s1 = edb.Site(name="S1")
        t1 = edb.wind.WindTurbine(name="T1", capacity=3.5)
        t2 = edb.wind.WindTurbine(name="T2", capacity=3.5)
        s1.members = [t1, t2]
        s2 = edb.Site(name="S2")
        b1 = edb.battery.Battery(name="B1", storage_capacity=10)
        s2.members = [b1]
        portfolio.members = [s1, s2]

        root_uuid = asyncio.run(client.register_tree(portfolio))

        assert root_uuid == portfolio.id
        batches = _batches(conn)
        assert "edge" not in batches  # no edges in this tree
        # (uuid, node_type, name, parent_uuid, path, data)
        assert [(r[2], r[3], r[4]) for r in batches["node"]] == [
            ("P", None, "P"),
            ("S1", portfolio.id, "P/S1"),
            ("T1", s1.id, "P/S1/T1"),
            ("T2", s1.id, "P/S1/T2"),
            ("S2", portfolio.id, "P/S2"),
            ("B1", s2.id, "P/S2/B1"),
        ]
        client.td.write.assert_not_called()

    def test_metadata_only_tree_registers_series_no_data(self, monkeypatch):
        """Metadata-only TimeSeries land in the series batch; td.write is never called."""
        client = _mock_client(monkeypatch)
        conn = _make_conn()
        client._pool = _async_pool(conn)

        tree = edb.wind.WindTurbine(
            name="T",
            capacity=3.5,
            timeseries=[edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
        )
        asyncio.run(client.register_tree(tree))
        client.td.write.assert_not_called()
        series = _batches(conn)["series"]
        # (node_uuid, edge_uuid, data_type, name, canonical_unit, timeseries_type, retention, description)
        assert series == [(tree.id, None, "actual", "power", "MW", "FLAT", "forever", None)]

    def test_inline_data_rejected(self, monkeypatch):
        """register_tree raises if any TimeSeries has non-empty data; nothing is inserted."""
        client = _mock_client(monkeypatch)
        conn = _make_conn()
        client._pool = _async_pool(conn)

        tree = edb.wind.WindTurbine(
            name="T",
            capacity=3.5,
            timeseries=[edb.TimeSeries(_simple_df(2), name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
        )
        with pytest.raises(ValueError, match="register_tree"):
            asyncio.run(client.register_tree(tree))
        conn._batch_cursor.executemany.assert_not_called()

    def test_under_resolves_parent_path(self, monkeypatch):
        """``under=`` resolves to a parent_uuid; the grafted rows carry that
        parent_uuid and paths prefixed with the parent's materialized path."""
        from uuid import uuid4

        client = _mock_client(monkeypatch)

        parent_uuid = uuid4()

        # Different SQL queries fire during register_tree_under:
        #   1. resolve_node_uuid("Region/Site") (path = %s)      → returns 1-col row
        #   2. the create-only existence pre-check                → returns []
        #   3. graft parent path lookup (path FROM node ... uuid) → returns its path
        # Use side_effect to dispatch based on the SQL substring.
        def execute_side_effect(sql, *args, **kwargs):
            res = MagicMock()
            if "WHERE path = %s" in sql:
                # resolve_node_uuid materialized-path lookup uses fetchone.
                res.fetchone = AsyncMock(return_value=(parent_uuid,))
                res.fetchall = AsyncMock(return_value=[])
            elif f"SELECT path FROM {P}node WHERE uuid" in sql:
                res.fetchone = AsyncMock(return_value=("Region/Site",))
                res.fetchall = AsyncMock(return_value=[])
            else:
                # New target uuids: not yet in DB (existence pre-check).
                res.fetchone = AsyncMock(return_value=None)
                res.fetchall = AsyncMock(return_value=[])
            return res

        conn = _make_conn(execute_side_effect=execute_side_effect)
        client._pool = _async_pool(conn)

        turbine = edb.wind.WindTurbine(name="T", capacity=3.5)
        asyncio.run(client.register_tree(turbine, under=("Region", "Site")))
        rows = _batches(conn)["node"]
        assert [(r[0], r[3], r[4]) for r in rows] == [(turbine.id, parent_uuid, "Region/Site/T")]


class TestTwoPassWalk:
    def test_edges_batched_after_nodes(self, monkeypatch):
        client = _mock_client(monkeypatch)
        conn = _make_conn()
        client._pool = _async_pool(conn)

        bus_a = edb.grid.JunctionPoint(name="BusA")
        bus_b = edb.grid.JunctionPoint(name="BusB")
        line = edb.grid.Line(name="L1", capacity=500, from_element=Reference(bus_a), to_element=Reference(bus_b))
        tree = edb.Portfolio(name="Grid", members=[bus_a, bus_b, line])
        asyncio.run(client.register_tree(tree))

        batches = _batches(conn)
        node_names = [r[2] for r in batches["node"]]
        assert node_names == ["Grid", "BusA", "BusB"]  # the edge is not a node row
        # (uuid, edge_type, name, from_node_uuid, to_node_uuid, data)
        assert [(r[0], r[2], r[3], r[4]) for r in batches["edge"]] == [(line.id, "L1", bus_a.id, bus_b.id)]
        # nodes are inserted before edges (FK on the endpoints)
        calls = conn._batch_cursor.executemany.call_args_list
        tables = [_target_table(c.args[0]) for c in calls]
        assert tables.index("node") < tables.index("edge")


class TestBatchedSeriesRows:
    """Payload-internal duplicate handling in ``_collect_series_rows`` mirrors
    the DB-driven contract of ``register_series``."""

    def test_identical_duplicates_collapse(self):
        from energydb._persist import _collect_series_rows

        t = edb.wind.WindTurbine(
            name="T",
            capacity=1.0,
            timeseries=[
                edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL),
                edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL),
            ],
        )
        rows = _collect_series_rows({t.id: t}, {})
        assert len(rows) == 1

    def test_conflicting_immutables_raise(self):
        from energydb._persist import _collect_series_rows

        t = edb.wind.WindTurbine(
            name="T",
            capacity=1.0,
            timeseries=[
                edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL),
                edb.TimeSeries(name="power", unit="kW", data_type=edb.DataType.ACTUAL),
            ],
        )
        with pytest.raises(ValueError, match="conflicting immutable"):
            _collect_series_rows({t.id: t}, {})

    def test_node_and_edge_owners_both_collected(self):
        from energydb._persist import _collect_series_rows

        t = edb.wind.WindTurbine(
            name="T",
            capacity=1.0,
            timeseries=[edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
        )
        a = edb.grid.JunctionPoint(name="A")
        b = edb.grid.JunctionPoint(name="B")
        line = edb.grid.Line(
            name="L",
            capacity=1,
            from_element=Reference(a),
            to_element=Reference(b),
            timeseries=[edb.TimeSeries(name="flow", unit="MW", data_type=edb.DataType.ACTUAL)],
        )
        rows = _collect_series_rows({t.id: t}, {line.id: line})
        owners = {(r[0], r[1]) for r in rows}
        assert owners == {(t.id, None), (None, line.id)}


# ---------------------------------------------------------------------------
# Live integration tests (skipped without env vars)
# ---------------------------------------------------------------------------


pytestmark_live = pytest.mark.skipif(
    not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")),
    reason="TIMEDB_PG_DSN / TIMEDB_CH_URL not set",
)


@pytest.fixture
def live_edb():
    client = Client()
    client.delete()
    client.create()
    yield client
    client.delete()
    client.close()


@pytestmark_live
def test_live_register_tree_then_write_and_read(live_edb):
    """Structure registration + manifest write + scope read round-trip."""
    base = datetime(2026, 1, 1, tzinfo=UTC)
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
                    )
                ],
            )
        ],
    )
    root_uuid = live_edb.register_tree(tree)
    assert isinstance(root_uuid, UUID)

    write_df = pl.DataFrame(
        {
            "path": ["P/S/T1"] * 3,
            "data_type": ["actual"] * 3,
            "name": ["power"] * 3,
            "valid_time": [base + timedelta(hours=i) for i in range(3)],
            "value": [1.0, 2.0, 3.0],
        }
    )
    live_edb.write(write_df)

    out = live_edb.get_node("P").get_node("S").get_node("T1").read(data_type="actual", name="power")
    assert out["value"].to_list() == [1.0, 2.0, 3.0]


@pytestmark_live
def test_live_create_edge_with_series(live_edb):
    """Create two grid nodes, an edge between them, register a series on the
    edge, write data, read it back."""
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
    live_edb.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b]))

    line = edb.grid.Line(name="Cable-1", capacity=500, from_element=Reference(bus_a), to_element=Reference(bus_b))
    edge_uuid = live_edb.create_edge(line)
    assert isinstance(edge_uuid, UUID)

    edge = live_edb.get_edge(uuid=edge_uuid).get()
    assert type(edge).__name__ == "Line"

    scope = live_edb.get_edge(uuid=edge_uuid)
    scope.register_series(
        name="power_flow",
        canonical_unit="MW",
        data_type="actual",
        timeseries_type="FLAT",
        retention="medium",
    )
    base = datetime(2026, 2, 1, tzinfo=UTC)
    flow = pl.DataFrame(
        {
            "valid_time": [base + timedelta(hours=i) for i in range(2)],
            "value": [200.0, 250.0],
        }
    )
    scope.write(flow, data_type="actual", name="power_flow")
    out = scope.read(data_type="actual", name="power_flow")
    assert out["value"].to_list() == [200.0, 250.0]


@pytestmark_live
def test_live_edge_triple_manifest_read_matches_uuid(live_edb):
    """Reading an edge series by (from_path, to_path, edge_type) is byte-identical
    to reading it by edge_uuid, for both output modes; get_raw returns the uuid."""
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
    live_edb.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b]))
    line = edb.grid.Line(name="L1", capacity=500, from_element=Reference(bus_a), to_element=Reference(bus_b))
    edge_uuid = live_edb.create_edge(line)

    scope = live_edb.get_edge(uuid=edge_uuid)
    scope.register_series(
        name="power_flow", canonical_unit="MW", data_type="actual", timeseries_type="FLAT", retention="medium"
    )
    base = datetime(2026, 4, 1, tzinfo=UTC)
    flow = pl.DataFrame({"valid_time": [base + timedelta(hours=i) for i in range(3)], "value": [10.0, 20.0, 30.0]})
    scope.write(flow, data_type="actual", name="power_flow")

    by_uuid = pl.DataFrame({"edge_uuid": [str(edge_uuid)], "data_type": ["actual"], "name": ["power_flow"]})
    by_triple = pl.DataFrame(
        {
            "from_path": ["Grid/BusA"],
            "to_path": ["Grid/BusB"],
            "edge_type": ["Line"],
            "data_type": ["actual"],
            "name": ["power_flow"],
        }
    )

    out_uuid = live_edb.read(by_uuid).sort("valid_time")
    out_triple = live_edb.read(by_triple).sort("valid_time")
    # edge_uuid never leaks; the customer-facing edge identity is present on both.
    assert "edge_uuid" not in out_triple.columns
    assert set(out_triple.columns) == set(out_uuid.columns)
    assert out_triple.select(sorted(out_triple.columns)).equals(out_uuid.select(sorted(out_uuid.columns)))
    assert out_triple["value"].to_list() == [10.0, 20.0, 30.0]
    assert out_triple["from_path"].unique().to_list() == ["Grid/BusA"]

    # by_path output is keyed by the same EdgeSeriesKey for both routes.
    kp_uuid = live_edb.read(by_uuid, output="by_path")
    kp_triple = live_edb.read(by_triple, output="by_path")
    assert set(kp_triple.keys()) == set(kp_uuid.keys())
    key = next(iter(kp_triple))
    assert (key.from_path, key.to_path, key.edge_type) == ("Grid/BusA", "Grid/BusB", "Line")

    # get_raw is the light uuid fetch: no EDM reconstruction.
    raw = live_edb.get_edge("Grid/BusA", "Grid/BusB", type="Line").get_raw()
    assert str(raw["uuid"]) == str(edge_uuid)
    assert raw["edge_type"] == "Line"


@pytestmark_live
def test_live_manifest_read(live_edb):
    base = datetime(2026, 3, 1, tzinfo=UTC)
    tree = edb.Portfolio(
        name="P",
        members=[
            edb.wind.WindTurbine(
                name="T1",
                capacity=3.5,
                timeseries=[edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
            ),
            edb.wind.WindTurbine(
                name="T2",
                capacity=3.5,
                timeseries=[edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
            ),
        ],
    )
    live_edb.register_tree(tree)

    write_df = pl.DataFrame(
        {
            "path": ["P/T1"] * 2 + ["P/T2"] * 2,
            "data_type": ["actual"] * 4,
            "name": ["power"] * 4,
            "valid_time": [base + timedelta(hours=i) for i in range(2)] * 2,
            "value": [1.0, 2.0, 10.0, 20.0],
        }
    )
    live_edb.write(write_df)

    manifest = pl.DataFrame(
        {
            "path": ["P/T1", "P/T2"],
            "data_type": ["actual", "actual"],
            "name": ["power", "power"],
        }
    )
    out = live_edb.read(manifest)
    # Path is the customer-facing identity; node/node_type/node_uuid no longer
    # leak onto rows.
    assert set(out["path"].unique().to_list()) == {"P/T1", "P/T2"}
    assert "node" not in out.columns


@pytestmark_live
def test_live_get_tree_with_series(live_edb):
    tree = edb.Portfolio(
        name="Pg",
        members=[
            edb.wind.WindTurbine(
                name="Tg",
                capacity=3.5,
                timeseries=[edb.TimeSeries(name="power", unit="MW", data_type=edb.DataType.ACTUAL)],
            )
        ],
    )
    live_edb.register_tree(tree)

    rebuilt = live_edb.get_tree("Pg", include_series=True)
    assert rebuilt.name == "Pg"
    turbine = rebuilt.children()[0]
    assert turbine.name == "Tg"
    assert turbine.timeseries is not None
    assert any(t.name == "power" for t in turbine.timeseries)


@pytestmark_live
def test_live_query_within_accepts_str_tuple_list_uuid(live_edb):
    """``within=`` should accept a bare str, tuple, list, or UUID."""
    bus_a = edb.grid.JunctionPoint(name="BusA")
    bus_b = edb.grid.JunctionPoint(name="BusB")
    portfolio = edb.Portfolio(name="My Portfolio", members=[bus_a, bus_b])
    portfolio_uuid = live_edb.register_tree(portfolio)

    line = edb.grid.Line(name="Cable-1", capacity=500, from_element=Reference(bus_a), to_element=Reference(bus_b))
    live_edb.create_edge(line)

    by_str = live_edb.query_nodes(within="My Portfolio")
    by_tuple = live_edb.query_nodes(within=("My Portfolio",))
    by_list = live_edb.query_nodes(within=["My Portfolio"])
    by_uuid = live_edb.query_nodes(within=portfolio_uuid)
    expected_names = {"My Portfolio", "BusA", "BusB"}
    assert {n.name for n in by_str} == expected_names
    assert {n.name for n in by_tuple} == expected_names
    assert {n.name for n in by_list} == expected_names
    assert {n.name for n in by_uuid} == expected_names

    e_str = live_edb.query_edges(type="Line", within="My Portfolio")
    e_tuple = live_edb.query_edges(type="Line", within=("My Portfolio",))
    e_list = live_edb.query_edges(type="Line", within=["My Portfolio"])
    e_uuid = live_edb.query_edges(type="Line", within=portfolio_uuid)
    assert len(e_str) == 1
    assert len(e_tuple) == 1
    assert len(e_list) == 1
    assert len(e_uuid) == 1


@pytestmark_live
def test_live_edge_triple_read_matches_uuid_read(live_edb):
    """Triple-addressed edge reads (one collapsed PG round-trip) return the
    identical frame as uuid-addressed reads, and a missing edge raises."""
    bus_a = edb.grid.JunctionPoint(name="BusX")
    bus_b = edb.grid.JunctionPoint(name="BusY")
    live_edb.register_tree(edb.Portfolio(name="Grid2", members=[bus_a, bus_b]))
    line = edb.grid.Line(name="Cable-2", capacity=300, from_element=Reference(bus_a), to_element=Reference(bus_b))
    edge_uuid = live_edb.create_edge(line)

    scope_uuid = live_edb.get_edge(uuid=edge_uuid)
    scope_uuid.register_series(
        name="power_flow", canonical_unit="MW", data_type="actual", timeseries_type="FLAT", retention="medium"
    )
    base = datetime(2026, 2, 1, tzinfo=UTC)
    scope_uuid.write(
        pl.DataFrame({"valid_time": [base + timedelta(hours=i) for i in range(2)], "value": [10.0, 20.0]}),
        data_type="actual",
        name="power_flow",
    )

    by_uuid = scope_uuid.read(data_type="actual", name="power_flow")
    by_triple = live_edb.get_edge("Grid2/BusX", "Grid2/BusY", type="Line").read(data_type="actual", name="power_flow")
    assert by_uuid.equals(by_triple)

    # Missing edge under triple addressing raises, matching uuid addressing.
    with pytest.raises(ValueError, match="Edge not found"):
        live_edb.get_edge("Grid2/BusX", "Grid2/BusY", type="nonexistent").read(data_type="actual", name="power_flow")
