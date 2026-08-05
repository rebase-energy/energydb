"""Transaction — atomic batch of scope mutations with preview support.

A :class:`Transaction` owns one borrowed pool connection for its whole
lifetime. Mutations executed through ``txn.get_node(...)`` /
``txn.get_edge(...)`` apply immediately to the open transaction's
connection but are not committed until :meth:`Transaction.commit` is
called explicitly. Exit without commit raises and rolls back.

Each mutator records a :class:`NodeChange` / :class:`EdgeChange` snapshot
pair into the txn's internal log so :meth:`preview` can return a
:class:`TreeDiff` aggregating everything done so far.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from energydb._persist import register_tree_under
from energydb.diff import EdgeChange, NodeChange, TreeDiff
from energydb.paths import resolve_node_uuid

if TYPE_CHECKING:
    from energydb.client import AsyncClient
    from energydb.scope import EdgeScope, NodeScope, Path


class Transaction:
    """Context manager wrapping a single pool connection for atomic batches.

    Mid-transaction reads see the transaction's own uncommitted writes
    (single physical connection). Time-series I/O —
    ``scope.write(df, ...)`` / ``scope.read(...)`` — does not participate
    in the PG transaction and is **rejected with a RuntimeError** on a
    txn-bound scope: call :meth:`Client.write` / :meth:`Client.read`
    directly outside the transaction instead.
    """

    def __init__(self, client: AsyncClient):
        self._client = client
        self._pool_cm = None
        self._conn = None
        self._committed = False
        self._node_changes: list[NodeChange] = []
        self._edge_changes: list[EdgeChange] = []

    def __repr__(self) -> str:
        """Plain-text repr — no I/O. Shows state + pending-change counts."""
        if self._committed:
            state = "committed"
        elif self._conn is not None:
            state = "open"
        else:
            state = "unopened"
        return f"Transaction({state}, {len(self._node_changes)} node + {len(self._edge_changes)} edge changes pending)"

    async def __aenter__(self) -> Transaction:
        # Borrow via the client's checkout point: on a namespaced view this
        # binds the transaction-local namespace GUC, which covers the whole
        # batch because the txn commits exactly once (at .commit()). Any
        # statement issued after commit() would run outside that binding —
        # commit is terminal, so scopes must not be used past it.
        self._pool_cm = self._client._conn()
        self._conn = await self._pool_cm.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        assert self._conn is not None and self._pool_cm is not None
        try:
            if exc_type is not None:
                # User code raised — rollback and let the exception propagate.
                await self._conn.rollback()
                return False
            if not self._committed:
                # Clean exit without commit — rollback and raise loudly.
                await self._conn.rollback()
                raise RuntimeError(
                    "Transaction exited without .commit(); changes rolled back. "
                    "Call txn.commit() before leaving the with-block, or raise to abort."
                )
            return False
        finally:
            await self._pool_cm.__aexit__(exc_type, exc, tb)

    # ------------------------------------------------------------------
    # Scope factories — mirror Client surface, scopes bound to this txn.
    # ------------------------------------------------------------------

    def get_node(self, *names_or_path, uuid: UUID | None = None) -> NodeScope:
        scope = self._client.get_node(*names_or_path, uuid=uuid)
        return scope._with_txn(self)

    def get_edge(
        self,
        from_path: Path | list[str] | str | None = None,
        to_path: Path | list[str] | str | None = None,
        *,
        type: str | None = None,
        uuid: UUID | None = None,
    ) -> EdgeScope:
        scope = self._client.get_edge(from_path, to_path, type=type, uuid=uuid)
        return scope._with_txn(self)

    async def register_tree(
        self,
        edm_obj,
        *,
        under: Path | list[str] | str | None = None,
    ) -> UUID:
        """Create a new tree (or subtree) inside this transaction.

        Mirrors :meth:`Client.register_tree`'s create-only semantics, but
        reuses the transaction's connection and extends the change log so
        the inserts show up in :meth:`preview`.
        """
        from energydb.scope import _coerce_path

        parent_uuid = await resolve_node_uuid(self._conn, _coerce_path((), kwarg=under)) if under is not None else None
        root_uuid, diff = await register_tree_under(
            self._conn,
            edm_obj,
            parent_uuid=parent_uuid,
            dry_run=False,
        )
        self._node_changes.extend(diff.node_changes)
        self._edge_changes.extend(diff.edge_changes)
        return root_uuid

    # ------------------------------------------------------------------
    # Internal recording — called by scope mutators.
    # ------------------------------------------------------------------

    def _record_node(self, old, new) -> None:
        self._node_changes.append(NodeChange(old=old, new=new))

    def _record_edge(self, old, new) -> None:
        self._edge_changes.append(EdgeChange(old=old, new=new))

    # ------------------------------------------------------------------
    # Preview + commit
    # ------------------------------------------------------------------

    def preview(self) -> TreeDiff:
        """Return a :class:`TreeDiff` aggregating every change so far.

        Repeated mutations on the same uuid appear as multiple entries —
        no collapsing is done. The result is read-only; call again to
        re-snapshot after additional mutations.
        """
        return TreeDiff(
            node_changes=list(self._node_changes),
            edge_changes=list(self._edge_changes),
        )

    async def commit(self) -> None:
        """Commit the transaction. Required before exiting the with-block."""
        if self._committed:
            raise RuntimeError("Transaction already committed.")
        assert self._conn is not None
        await self._conn.commit()
        self._committed = True
