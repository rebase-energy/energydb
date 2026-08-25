"""Validate the no-``/`` / non-empty name rule on Node, Edge, and Series.

``/`` is reserved as the path separator on the read API; node/edge/series
names cannot contain it. Empty names are also rejected. The rule is enforced
at two layers: a Python guard at register time (clear error before any PG
round-trip) and a PG ``CheckConstraint`` (defence-in-depth against direct
SQL inserts).

Skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL`` are not set.
"""

from __future__ import annotations

import os
from uuid import uuid4

import energydatamodel as edm
import energydb as edb
import psycopg
import pytest
from energydatamodel.reference import Reference
from energydb import Client
from energydb.series import validate_name

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip(
        "TIMEDB_PG_DSN / TIMEDB_CH_URL not set: skipping name-validation tests",
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
# Python guard: register_tree (node)
# ---------------------------------------------------------------------------


def test_register_tree_rejects_slash_in_node_name(client):
    tree = edb.Portfolio(name="P", members=[edb.Site(name="A/B")])
    with pytest.raises(ValueError, match="/"):
        client.register_tree(tree)


def test_register_tree_rejects_slash_in_root_name(client):
    tree = edb.Portfolio(name="P/Q")
    with pytest.raises(ValueError, match="/"):
        client.register_tree(tree)


def test_validate_name_direct():
    """The Python guard rejects empty names and slash names with a clear error.

    ``register_tree`` can't exercise the empty-name path because
    ``serialize_node`` falls back to the EDM class name when ``name=""``;
    we test the guard directly so the contract is still covered.
    """
    with pytest.raises(ValueError, match="non-empty"):
        validate_name("", kind="node")
    with pytest.raises(ValueError, match="/"):
        validate_name("a/b", kind="node")
    with pytest.raises(TypeError):
        validate_name(None, kind="series")  # type: ignore[arg-type]
    validate_name("ok-name", kind="node")  # no exception


def test_register_tree_rejects_deeply_nested_slash(client):
    """Bad name anywhere in the tree raises; the transaction rolls back."""
    bad = edb.wind.WindTurbine(name="T1/T2", capacity=3.5)
    tree = edb.Portfolio(name="P", members=[edb.Site(name="S", members=[bad])])
    with pytest.raises(ValueError, match="/"):
        client.register_tree(tree)
    # Confirm nothing was committed by attempting to fetch; should raise.
    with pytest.raises(ValueError, match="not found"):
        client.get_tree("P")


# ---------------------------------------------------------------------------
# Python guard: register_series
# ---------------------------------------------------------------------------


def test_register_series_rejects_slash_in_name(client):
    tree = edb.Portfolio(name="P", members=[edb.Site(name="S")])
    client.register_tree(tree)
    with pytest.raises(ValueError, match="/"):
        client.get_node("P", "S").register_series(
            name="power/actual",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
        )


def test_register_series_rejects_empty_name(client):
    tree = edb.Portfolio(name="P", members=[edb.Site(name="S")])
    client.register_tree(tree)
    with pytest.raises(ValueError, match="non-empty"):
        client.get_node("P", "S").register_series(
            name="",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
        )


# ---------------------------------------------------------------------------
# Python guard: create_edge (name is nullable, but if present must be valid)
# ---------------------------------------------------------------------------


def test_create_edge_rejects_slash_in_name(client):
    a = edb.Site(name="A")
    b = edb.Site(name="B")
    client.register_tree(edb.Portfolio(name="P", members=[a, b]))

    edge = edm.Edge(
        name="bad/edge",
        from_element=Reference(a),
        to_element=Reference(b),
    )
    with pytest.raises(ValueError, match="/"):
        client.create_edge(edge)


def test_create_edge_allows_null_name(client):
    a = edb.Site(name="A")
    b = edb.Site(name="B")
    client.register_tree(edb.Portfolio(name="P", members=[a, b]))

    edge = edm.Edge(
        from_element=Reference(a),
        to_element=Reference(b),
    )
    # No exception: edges can have NULL name.
    client.create_edge(edge)


# ---------------------------------------------------------------------------
# PG-level CheckConstraint: bypasses the Python guard
# ---------------------------------------------------------------------------


def _raw_dsn() -> str:
    return os.environ["TIMEDB_PG_DSN"]


def test_pg_check_rejects_slash_in_node_name(client):
    # path is NOT NULL with its own non-empty CHECK and a UNIQUE
    # constraint; pass a unique sentinel so the row only fails the
    # node_name_valid CHECK we're actually testing.
    with psycopg.connect(_raw_dsn()) as conn, pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            "INSERT INTO energydb.node (uuid, node_type, name, path, data) VALUES (%s, %s, %s, %s, %s)",
            (uuid4(), "Site", "a/b", f"__t_slash_{uuid4()}__", "{}"),
        )


def test_pg_check_rejects_empty_node_name(client):
    with psycopg.connect(_raw_dsn()) as conn, pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            "INSERT INTO energydb.node (uuid, node_type, name, path, data) VALUES (%s, %s, %s, %s, %s)",
            (uuid4(), "Site", "", f"__t_empty_{uuid4()}__", "{}"),
        )


def test_pg_check_rejects_slash_in_series_name(client):
    tree = edb.Portfolio(name="P", members=[edb.Site(name="S")])
    client.register_tree(tree)
    site_uuid = client.query_nodes(name="S")[0].id

    with psycopg.connect(_raw_dsn()) as conn, pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            """
            INSERT INTO energydb.series
                (node_uuid, edge_uuid, data_type, name, canonical_unit,
                 timeseries_type, retention)
            VALUES (%s, NULL, %s, %s, %s, %s, %s)
            """,
            (site_uuid, "actual", "power/actual", "MW", "FLAT", "forever"),
        )


def test_pg_check_rejects_slash_in_edge_name(client):
    a = edb.Site(name="A")
    b = edb.Site(name="B")
    client.register_tree(edb.Portfolio(name="P", members=[a, b]))

    with psycopg.connect(_raw_dsn()) as conn, pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            """
            INSERT INTO energydb.edge
                (uuid, edge_type, name, from_node_uuid, to_node_uuid, data)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (uuid4(), "Connection", "bad/edge", a.id, b.id, "{}"),
        )


def test_pg_check_allows_null_edge_name(client):
    a = edb.Site(name="A")
    b = edb.Site(name="B")
    client.register_tree(edb.Portfolio(name="P", members=[a, b]))

    # NULL name should be accepted at the PG level (CHECK is guarded by NULL).
    with psycopg.connect(_raw_dsn()) as conn:
        conn.execute(
            """
            INSERT INTO energydb.edge
                (uuid, edge_type, name, from_node_uuid, to_node_uuid, data)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (uuid4(), "Connection", None, a.id, b.id, "{}"),
        )
        conn.commit()
