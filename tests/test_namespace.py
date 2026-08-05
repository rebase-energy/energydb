"""Namespace-view tests.

The view mechanics (clone semantics, lifecycle guards, repr) are pure
Python — no database needed. ``TimeDBClient`` is stubbed so constructing
an :class:`AsyncClient` doesn't dial ClickHouse, and the pool is never
opened. The GUC round-trip test at the bottom is gated on the live-DB
env var, same as the integration suite.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import energydb.client as client_mod
import pytest
from energydb.client import AsyncClient
from energydb.errors import ValidationError

_DSN = "postgresql://user:pass@localhost:5432/energydb_unit_test"


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> AsyncClient:
    """An AsyncClient that never touches PG or CH (pool unopened, CH stubbed)."""
    monkeypatch.setattr(client_mod, "TimeDBClient", lambda ch_url=None: object())
    return AsyncClient(pg_conninfo=_DSN, ch_url="http://unused")


# ---------------------------------------------------------------------------
# View semantics
# ---------------------------------------------------------------------------


def test_namespace_returns_bound_view(client: AsyncClient) -> None:
    view = client.namespace("ws-1")
    assert view is not client
    assert type(view) is AsyncClient
    assert view._namespace == "ws-1"
    assert view._owns_pool is False
    # Shares the parent's resources.
    assert view._pool is client._pool
    assert view.td is client.td
    # Root client is untouched.
    assert client._namespace is None
    assert client._owns_pool is True


def test_namespace_rebind_from_view(client: AsyncClient) -> None:
    view = client.namespace("ws-1")
    view2 = view.namespace("ws-2")
    assert view2._namespace == "ws-2"
    assert view._namespace == "ws-1"
    assert view2._pool is client._pool


def test_namespace_rejects_empty(client: AsyncClient) -> None:
    with pytest.raises(ValueError, match="non-empty"):
        client.namespace("")


def test_view_disables_engine_reads(client: AsyncClient) -> None:
    """The CH meta-engine reads PG with its own (RLS-bypassing) credentials,
    so namespaced views must always take the sequential resolve."""
    view = client.namespace("ws-1")
    assert view._engine_unavailable is True
    # ...and binding a namespace never mutates the root client.
    assert client._engine_unavailable is False


def test_repr_shows_namespace(client: AsyncClient) -> None:
    assert "namespace" not in repr(client)
    assert "namespace='ws-1'" in repr(client.namespace("ws-1"))


# ---------------------------------------------------------------------------
# Lifecycle / schema guards
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("op", ["open", "close", "create", "delete", "setup_ch_meta_engine"])
def test_lifecycle_ops_raise_on_view(client: AsyncClient, op: str) -> None:
    view = client.namespace("ws-1")
    coro: Any = getattr(view, op)()
    with pytest.raises(ValidationError, match="root client"):
        asyncio.run(coro)


def test_lifecycle_guard_passes_on_root(client: AsyncClient) -> None:
    # The guard itself is a no-op on the root client. (Deliberately not
    # exercising open()/close() here: a pool opened and closed across two
    # event loops deadlocks, and pool lifecycle is psycopg_pool's business,
    # not this guard's.)
    for op in ("open", "close", "create", "delete", "setup_ch_meta_engine"):
        client._require_root(op)


# ---------------------------------------------------------------------------
# GUC binding (live DB only)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not os.environ.get("TIMEDB_PG_DSN"), reason="TIMEDB_PG_DSN not set")
def test_conn_binds_transaction_local_guc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client_mod, "TimeDBClient", lambda ch_url=None: object())

    async def run() -> None:
        root = AsyncClient(pg_conninfo=os.environ["TIMEDB_PG_DSN"], ch_url="http://unused")
        await root.open()
        try:
            view = root.namespace("ns-guc-test")
            async with view._conn() as conn:
                row = await (await conn.execute("SELECT current_setting('energydb.namespace', true)")).fetchone()
                assert row is not None and row[0] == "ns-guc-test"
            # A fresh checkout on the ROOT client must not see the value —
            # set_config(..., is_local := true) dies with the transaction.
            # For a custom GUC the post-transaction reading is NULL (never
            # defined on this connection) or '' (defined once, reset value);
            # both fail an equality test against any real namespace, which
            # is the deny-by-default property RLS will rely on.
            async with root._conn() as conn:
                row = await (await conn.execute("SELECT current_setting('energydb.namespace', true)")).fetchone()
                assert row is not None and row[0] in (None, "")
        finally:
            await root.close()

    asyncio.run(run())
