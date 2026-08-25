"""Repr / _repr_html_ tests.

Most cases are pure formatting: no DB needed. Construct scopes with a
dummy client (None cast); the repr methods deliberately don't touch the
client, so this is safe. Client repr requires a real DSN and is gated on
the live-DB env vars.
"""

from __future__ import annotations

import os
from typing import Any, cast
from uuid import uuid4

import pytest
from energydb._transaction import Transaction
from energydb.scope import EdgeScope, NodeScope

# Any so the type checker accepts None for a parameter typed
# Client. The repr methods never touch the client, so this is safe.
_DUMMY_CLIENT: Any = None


# ---------------------------------------------------------------------------
# NodeScope.__repr__
# ---------------------------------------------------------------------------


def test_node_scope_repr_path_only():
    scope = NodeScope(_DUMMY_CLIENT, path=("P", "Site", "T01"))  # type: ignore[arg-type]
    r = repr(scope)
    assert r == "NodeScope(path='P/Site/T01')"


def test_node_scope_repr_uuid_only():
    u = uuid4()
    scope = NodeScope(_DUMMY_CLIENT, node_uuid=u)  # type: ignore[arg-type]
    r = repr(scope)
    assert r == f"NodeScope(uuid={u})"


def test_node_scope_repr_path_and_uuid():
    u = uuid4()
    scope = NodeScope(_DUMMY_CLIENT, node_uuid=u, path=("Site",))  # type: ignore[arg-type]
    r = repr(scope)
    assert "path='Site'" in r
    assert f"uuid={u}" in r


def test_node_scope_repr_with_filters():
    scope = NodeScope(_DUMMY_CLIENT, path=("P",), where_filters={"node_type": "WindTurbine"})  # type: ignore[arg-type]
    r = repr(scope)
    assert "where=" in r
    assert "WindTurbine" in r


def test_node_scope_repr_unresolved():
    scope = NodeScope(_DUMMY_CLIENT)  # type: ignore[arg-type]
    assert repr(scope) == "NodeScope(<unresolved>)"


# ---------------------------------------------------------------------------
# NodeScope._repr_html_
# ---------------------------------------------------------------------------


def test_node_scope_html_repr_contains_path():
    scope = NodeScope(_DUMMY_CLIENT, path=("P", "Site", "T01"))  # type: ignore[arg-type]
    html = scope._repr_html_()
    assert "NodeScope" in html
    assert "P/Site/T01" in html
    assert "<div" in html and "</div>" in html


def test_node_scope_html_repr_unresolved():
    scope = NodeScope(_DUMMY_CLIENT)  # type: ignore[arg-type]
    html = scope._repr_html_()
    assert "<unresolved>" in html


def test_node_scope_html_repr_shows_filters_and_txn():
    scope = NodeScope(
        _DUMMY_CLIENT,  # type: ignore[arg-type]
        path=("P",),
        where_filters={"node_type": "Site"},
        txn=cast("object", object()),  # type: ignore[arg-type]
    )
    html = scope._repr_html_()
    assert "where:" in html
    assert "Site" in html
    assert "transaction" in html


def test_node_scope_html_repr_uuid_path_shows_both():
    u = uuid4()
    scope = NodeScope(_DUMMY_CLIENT, node_uuid=u, path=("Site",))  # type: ignore[arg-type]
    html = scope._repr_html_()
    assert "Site" in html
    assert str(u) in html


# ---------------------------------------------------------------------------
# EdgeScope.__repr__
# ---------------------------------------------------------------------------


def test_edge_scope_repr_by_uuid():
    u = uuid4()
    scope = EdgeScope(_DUMMY_CLIENT, edge_uuid=u)  # type: ignore[arg-type]
    assert repr(scope) == f"EdgeScope(uuid={u})"


def test_edge_scope_repr_by_triple():
    scope = EdgeScope(
        _DUMMY_CLIENT,  # type: ignore[arg-type]
        from_path=("P", "BusA"),
        to_path=("P", "BusB"),
        edge_type="Line",
    )
    r = repr(scope)
    assert "from='P/BusA'" in r
    assert "to='P/BusB'" in r
    assert "type='Line'" in r


def test_edge_scope_repr_with_txn():
    scope = EdgeScope(
        _DUMMY_CLIENT,  # type: ignore[arg-type]
        from_path=("P", "A"),
        to_path=("P", "B"),
        edge_type="Line",
        txn=cast("object", object()),  # type: ignore[arg-type]
    )
    assert "txn=True" in repr(scope)


# ---------------------------------------------------------------------------
# Transaction.__repr__
# ---------------------------------------------------------------------------


def test_transaction_repr_unopened():
    txn = Transaction(cast("object", None))  # type: ignore[arg-type]
    r = repr(txn)
    assert "unopened" in r
    assert "0 node + 0 edge" in r


def test_transaction_repr_pending_counts():
    from energydb.diff import NodeChange, NodeSnapshot

    txn = Transaction(cast("object", None))  # type: ignore[arg-type]
    snap = NodeSnapshot(uuid=uuid4(), node_type="Site", name="S", parent_uuid=None, data={})
    txn._node_changes.append(NodeChange(old=None, new=snap))
    assert "1 node" in repr(txn)


# ---------------------------------------------------------------------------
# Client.__repr__: needs a real DSN; gated on live-DB env vars.
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")),
    reason="needs live PG/CH connections",
)
def test_client_repr_strips_credentials():
    from energydb import Client

    c = Client()
    try:
        r = repr(c)
        # Should never contain a raw password. The password itself is unknown,
        # but we can assert the userinfo sentinel is present when the DSN
        # has any '@'.
        if "@" in c._dsn:
            assert "***@" in r
        # Repr should mention "Client(pg=" prefix.
        assert r.startswith("Client(pg=")
    finally:
        c.close()
