"""Contract tests for the typed exception hierarchy (:mod:`energydb.errors`).

Three groups:

* **Hierarchy invariants** — every public error subclasses both
  :class:`~energydb.errors.EnergyDBError` and :class:`ValueError`, so the
  pre-taxonomy ``except ValueError`` contract still holds. Pinned as a test
  because it *is* the backwards-compatibility guarantee.
* **Source scan** — zero ``raise ValueError(`` left in ``energydb/``. The
  cheapest possible regression guard for the ~92-site sweep.
* **Per-family behavior** — the class and its structured identifier fields at
  the raise sites that matter. Mock-connection tests run everywhere; the ones
  that genuinely need PG + ClickHouse are marked and skip without a DB.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import re
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import energydb as edb
import polars as pl
import pytest
from energydb._frames import to_backend
from energydb._persist import _collect_target_state
from energydb._sync import _Portal
from energydb.client import AsyncClient
from energydb.errors import (
    AlreadyExistsError,
    ConfigurationError,
    EdgeNotFoundError,
    EnergyDBError,
    IncompatibleUnitError,
    ManifestError,
    NodeNotFoundError,
    NotFoundError,
    SeriesNotFoundError,
    ValidationError,
)
from energydb.paths import resolve_edge_uuid, resolve_manifest, resolve_node_uuid, resolve_path

needs_db = pytest.mark.skipif(
    not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")),
    reason="TIMEDB_PG_DSN / TIMEDB_CH_URL not set — skipping DB-backed error tests",
)

PUBLIC_ERRORS = [
    NotFoundError,
    NodeNotFoundError,
    EdgeNotFoundError,
    SeriesNotFoundError,
    AlreadyExistsError,
    ValidationError,
    ManifestError,
    ConfigurationError,
    IncompatibleUnitError,
]


# ---------------------------------------------------------------------------
# Hierarchy invariants
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", PUBLIC_ERRORS, ids=lambda c: c.__name__)
def test_every_public_error_is_an_energydb_error_and_a_value_error(cls):
    assert issubclass(cls, EnergyDBError)
    assert issubclass(cls, ValueError)


@pytest.mark.parametrize("cls", PUBLIC_ERRORS, ids=lambda c: c.__name__)
def test_except_value_error_still_catches_every_public_error(cls):
    """The backwards-compatibility guarantee, as a test."""
    try:
        raise cls("boom")
    except ValueError as exc:
        assert isinstance(exc, cls)
    else:  # pragma: no cover - the raise above always fires
        pytest.fail(f"{cls.__name__} was not caught by 'except ValueError'")


def test_manifest_error_is_a_validation_error():
    assert issubclass(ManifestError, ValidationError)


def test_taxonomy_is_re_exported_from_the_package_root():
    for cls in [EnergyDBError, *PUBLIC_ERRORS]:
        assert getattr(edb, cls.__name__) is cls
        assert cls.__name__ in edb.__all__


# ---------------------------------------------------------------------------
# Source scan — no bare raises left
# ---------------------------------------------------------------------------


def test_no_bare_value_error_raises_remain_in_the_package():
    package_dir = pathlib.Path(edb.__file__).parent
    offenders = [
        f"{path.relative_to(package_dir)}:{lineno}"
        for path in sorted(package_dir.rglob("*.py"))
        for lineno, line in enumerate(path.read_text().splitlines(), start=1)
        if re.search(r"\braise ValueError\(", line)
    ]
    assert offenders == [], "bare ValueError raise sites must use energydb.errors classes: " + ", ".join(offenders)


# ---------------------------------------------------------------------------
# Field conventions
# ---------------------------------------------------------------------------


def test_message_is_args0_and_str_is_unchanged():
    err = NodeNotFoundError("Node not found: a/b", path="a/b")
    assert err.args == ("Node not found: a/b",)
    assert str(err) == "Node not found: a/b"


def test_identifier_fields_default_to_none_when_unknown():
    node = NodeNotFoundError("x")
    assert (node.path, node.uuid) == (None, None)

    edge = EdgeNotFoundError("x")
    assert (edge.uuid, edge.from_path, edge.to_path, edge.edge_type) == (None, None, None, None)

    series = SeriesNotFoundError("x")
    assert (series.route, series.missing) == (None, None)


def test_identifier_fields_are_keyword_only():
    with pytest.raises(TypeError):
        NodeNotFoundError("x", "a/b")  # ty: ignore[too-many-positional-arguments]


# ---------------------------------------------------------------------------
# Per-family behavior — mock connection / pure paths
# ---------------------------------------------------------------------------


def _mock_conn(*result_sets: list[tuple]) -> MagicMock:
    """A conn whose successive ``fetchall``/``fetchone`` calls drain ``result_sets``."""
    cursor = MagicMock()
    cursor.fetchall = AsyncMock(side_effect=[list(rs) for rs in result_sets])
    cursor.fetchone = AsyncMock(side_effect=[rs[0] if rs else None for rs in result_sets])
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=cursor)
    return conn


def test_missing_node_by_path_raises_node_not_found_with_path():
    conn = _mock_conn([])
    with pytest.raises(NodeNotFoundError) as excinfo:
        asyncio.run(resolve_node_uuid(conn, ("P", "nope")))
    assert excinfo.value.path == "P/nope"
    assert excinfo.value.uuid is None
    assert str(excinfo.value) == "Node not found: P/nope"


def test_missing_node_by_uuid_raises_node_not_found_with_uuid():
    node_uuid = uuid4()
    conn = _mock_conn([])
    with pytest.raises(NodeNotFoundError) as excinfo:
        asyncio.run(resolve_path(conn, node_uuid))
    assert excinfo.value.uuid == node_uuid
    assert excinfo.value.path is None


def test_scope_not_found_hooks_return_typed_errors():
    """``_missing_error`` / ``_not_found_error`` carry the addressing they resolved from."""
    node_uuid = uuid4()
    by_path = edb.NodeScope(MagicMock(), path=("P", "nope"))._missing_error()
    assert isinstance(by_path, NodeNotFoundError)
    assert (by_path.path, by_path.uuid) == ("P/nope", None)

    by_uuid = edb.NodeScope(MagicMock(), node_uuid=node_uuid)._missing_error()
    assert (by_uuid.path, by_uuid.uuid) == (None, node_uuid)

    edge_err = edb.EdgeScope(MagicMock(), edge_uuid=node_uuid)._not_found_error(node_uuid)
    assert isinstance(edge_err, EdgeNotFoundError)
    assert edge_err.uuid == node_uuid


def test_missing_edge_by_triple_raises_edge_not_found_with_endpoints():
    from_uuid, to_uuid = uuid4(), uuid4()
    # First round-trip resolves both endpoint paths; the edge lookup then misses.
    conn = _mock_conn([("A", from_uuid), ("B", to_uuid)], [])
    with pytest.raises(EdgeNotFoundError) as excinfo:
        asyncio.run(resolve_edge_uuid(conn, ("A",), ("B",), "Line"))
    assert (excinfo.value.from_path, excinfo.value.to_path, excinfo.value.edge_type) == ("A", "B", "Line")
    assert excinfo.value.uuid is None


def test_ambiguous_routing_columns_raise_manifest_error():
    manifest = pl.DataFrame({"node_uuid": [str(uuid4())], "path": ["A"], "data_type": ["actual"], "name": ["power"]})
    with pytest.raises(ManifestError, match="ambiguous routing columns"):
        asyncio.run(resolve_manifest(MagicMock(), manifest))


def test_missing_routing_column_raises_manifest_error():
    manifest = pl.DataFrame({"data_type": ["actual"], "name": ["power"]})
    with pytest.raises(ManifestError, match="must include one of"):
        asyncio.run(resolve_manifest(MagicMock(), manifest))


def test_unregistered_series_by_uuid_raises_series_not_found_with_route_and_missing():
    node_uuid = uuid4()
    manifest = pl.DataFrame({"node_uuid": [str(node_uuid)], "data_type": ["actual"], "name": ["power"]})
    with pytest.raises(SeriesNotFoundError) as excinfo:
        asyncio.run(resolve_manifest(_mock_conn([]), manifest))
    assert excinfo.value.route == "node_uuid"
    assert excinfo.value.missing == [(str(node_uuid), "actual", "power")]


def test_unregistered_series_by_path_reports_every_missing_triple():
    manifest = pl.DataFrame(
        {
            "path": ["P/A", "P/B"],
            "data_type": ["actual", "actual"],
            "name": ["power", "wind_speed"],
        }
    )
    with pytest.raises(SeriesNotFoundError) as excinfo:
        asyncio.run(resolve_manifest(_mock_conn([]), manifest))
    assert excinfo.value.route == "path"
    assert sorted(excinfo.value.missing or []) == [
        ("P/A", "actual", "power"),
        ("P/B", "actual", "wind_speed"),
    ]


def test_unregistered_series_by_edge_triple_reports_the_quintuple():
    """The edge-triple route's owner is itself a triple, so ``missing`` holds 5-tuples."""
    manifest = pl.DataFrame(
        {
            "from_path": ["Grid/BusA"],
            "to_path": ["Grid/BusB"],
            "edge_type": ["Line"],
            "data_type": ["actual"],
            "name": ["flow"],
        }
    )
    with pytest.raises(SeriesNotFoundError) as excinfo:
        asyncio.run(resolve_manifest(_mock_conn([]), manifest))
    assert excinfo.value.route == "edge_triple"
    assert excinfo.value.missing == [("Grid/BusA", "Grid/BusB", "Line", "actual", "flow")]


def test_partial_edge_triple_raises_manifest_error():
    manifest = pl.DataFrame({"from_path": ["A"], "edge_type": ["Line"], "data_type": ["actual"], "name": ["flow"]})
    with pytest.raises(ManifestError, match="requires all of"):
        asyncio.run(resolve_manifest(MagicMock(), manifest))


def test_duplicate_uuid_in_tree_raises_already_exists():
    turbine = edb.wind.WindTurbine(name="T1", capacity=1.0)
    tree = edb.Portfolio(name="P", members=[turbine, turbine])
    with pytest.raises(AlreadyExistsError, match="Duplicate UUID"):
        _collect_target_state(tree, None)


def test_unknown_backend_raises_validation_error():
    with pytest.raises(ValidationError, match="backend must be"):
        to_backend(pl.DataFrame(), "csv")  # ty: ignore[invalid-argument-type]


def test_unconfigured_client_raises_configuration_error(monkeypatch):
    monkeypatch.delenv("TIMEDB_PG_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ConfigurationError, match="connection not configured"):
        AsyncClient()


def test_key_value_dsn_raises_configuration_error():
    with pytest.raises(ConfigurationError, match="must be a URI"):
        AsyncClient(pg_conninfo="host=localhost dbname=devdb")


def test_unknown_node_type_raises_validation_error():
    from energydb.serialization import reconstruct_node

    with pytest.raises(ValidationError, match="Unknown node type"):
        reconstruct_node({"uuid": uuid4(), "node_type": "NotAThing", "name": "x", "data": {}})


def test_incompatible_units_raise_incompatible_unit_error():
    from energydb.units import compute_unit_factor

    with pytest.raises(IncompatibleUnitError):
        compute_unit_factor("MW", "kg")


# ---------------------------------------------------------------------------
# Sync facade — typed errors survive the portal round-trip
# ---------------------------------------------------------------------------


def test_typed_error_from_a_portal_coroutine_reaches_the_sync_caller():
    """The sync :class:`energydb.Client` re-raises through ``Future.result()``."""
    portal = _Portal()
    node_uuid = uuid4()

    async def boom():
        raise NodeNotFoundError(f"Node not found: uuid={node_uuid}", uuid=node_uuid)

    try:
        with pytest.raises(NodeNotFoundError) as excinfo:
            portal.run(boom())
        assert excinfo.value.uuid == node_uuid
        assert isinstance(excinfo.value, ValueError)
    finally:
        portal.stop()


# ---------------------------------------------------------------------------
# DB-backed end-to-end raise paths
# ---------------------------------------------------------------------------


@pytest.fixture
def populated():
    """A ``P/S/T1`` tree with one registered series on ``T1``."""
    client = edb.Client()
    client.delete()
    client.create()
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
    client.register_tree(tree)
    yield client, tree
    client.delete()
    client.close()


@needs_db
def test_get_missing_node_raises_node_not_found_with_path(populated):
    client, _tree = populated
    with pytest.raises(NodeNotFoundError) as excinfo:
        client.get_node("nope").get()
    assert excinfo.value.path == "nope"


@needs_db
def test_get_missing_edge_by_uuid_raises_edge_not_found_with_uuid(populated):
    client, _tree = populated
    edge_uuid = uuid4()
    with pytest.raises(EdgeNotFoundError) as excinfo:
        client.get_edge(uuid=edge_uuid).get()
    assert excinfo.value.uuid == edge_uuid


@needs_db
def test_read_of_an_unregistered_triple_raises_series_not_found(populated):
    client, _tree = populated
    manifest = pl.DataFrame({"path": ["P/S/T1"], "data_type": ["actual"], "name": ["not_registered"]})
    with pytest.raises(SeriesNotFoundError) as excinfo:
        client.read(manifest)
    assert excinfo.value.route == "path"
    assert excinfo.value.missing == [("P/S/T1", "actual", "not_registered")]


@needs_db
def test_register_tree_with_a_pre_existing_uuid_raises_already_exists(populated):
    client, tree = populated
    with pytest.raises(AlreadyExistsError, match="create-only"):
        client.register_tree(tree)


@needs_db
def test_renaming_a_missing_node_raises_node_not_found_with_uuid(populated):
    client, _tree = populated
    node_uuid = uuid4()
    with pytest.raises(NodeNotFoundError) as excinfo:
        client.get_node(uuid=node_uuid).rename("other")
    assert excinfo.value.uuid == node_uuid
