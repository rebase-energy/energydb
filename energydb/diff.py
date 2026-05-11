"""TreeDiff — compute and represent the difference produced by a mutation.

Categorizes node/edge changes into insert / update / delete buckets, keyed
by UUID, and renders a tree-shaped preview. Returned by
:meth:`Client.register_tree` (``dry_run=True``), by each scope mutator
when called with ``dry_run=True``, and by :meth:`Transaction.preview`.

With UUID identity, a rename (name changed) and a move (parent_uuid
changed) are *not* delete-then-insert — the row keeps its uuid and just
has its column updated. The diff structure exposes "renamed" and "moved"
booleans on each update so the user-facing print can label changes
accurately.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import IO, Any
from uuid import UUID

# ---------------------------------------------------------------------------
# Snapshots — the canonical content of one row at one point in time.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NodeSnapshot:
    """One node row's content."""

    uuid: UUID
    node_type: str
    name: str
    parent_uuid: UUID | None
    data: dict[str, Any]


@dataclass(frozen=True)
class EdgeSnapshot:
    """One edge row's content."""

    uuid: UUID
    edge_type: str
    name: str | None
    from_node_uuid: UUID
    to_node_uuid: UUID
    data: dict[str, Any]


# ---------------------------------------------------------------------------
# Per-row change records
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NodeChange:
    """A single node-level diff entry.

    ``old`` is the DB state before the change; ``new`` is the target state
    after. Inserts have ``old=None``; deletes have ``new=None``.
    """

    old: NodeSnapshot | None
    new: NodeSnapshot | None

    def __post_init__(self):
        if self.old is None and self.new is None:
            raise ValueError("NodeChange must have at least one of old/new set.")

    @property
    def uuid(self) -> UUID:
        return self.new.uuid if self.new is not None else self.old.uuid  # type: ignore[union-attr]

    @property
    def kind(self) -> str:
        if self.old is None:
            return "insert"
        if self.new is None:
            return "delete"
        return "update"

    @property
    def display_name(self) -> str:
        if self.new is not None:
            return self.new.name
        return self.old.name  # type: ignore[union-attr]

    @property
    def display_type(self) -> str:
        if self.new is not None:
            return self.new.node_type
        return self.old.node_type  # type: ignore[union-attr]

    @property
    def renamed(self) -> bool:
        return self.kind == "update" and self.old.name != self.new.name  # type: ignore[union-attr]

    @property
    def moved(self) -> bool:
        return self.kind == "update" and self.old.parent_uuid != self.new.parent_uuid  # type: ignore[union-attr]

    @property
    def data_changed(self) -> bool:
        return self.kind == "update" and self.old.data != self.new.data  # type: ignore[union-attr]


@dataclass(frozen=True)
class EdgeChange:
    """A single edge-level diff entry."""

    old: EdgeSnapshot | None
    new: EdgeSnapshot | None

    def __post_init__(self):
        if self.old is None and self.new is None:
            raise ValueError("EdgeChange must have at least one of old/new set.")

    @property
    def uuid(self) -> UUID:
        return self.new.uuid if self.new is not None else self.old.uuid  # type: ignore[union-attr]

    @property
    def kind(self) -> str:
        if self.old is None:
            return "insert"
        if self.new is None:
            return "delete"
        return "update"

    @property
    def display_name(self) -> str:
        if self.new is not None:
            return self.new.name or self.new.edge_type
        return self.old.name or self.old.edge_type  # type: ignore[union-attr]

    @property
    def display_type(self) -> str:
        if self.new is not None:
            return self.new.edge_type
        return self.old.edge_type  # type: ignore[union-attr]

    @property
    def endpoints_changed(self) -> bool:
        return self.kind == "update" and (
            self.old.from_node_uuid != self.new.from_node_uuid  # type: ignore[union-attr]
            or self.old.to_node_uuid != self.new.to_node_uuid  # type: ignore[union-attr]
        )

    @property
    def data_changed(self) -> bool:
        return self.kind == "update" and self.old.data != self.new.data  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# TreeDiff — the structured result of comparing target vs current state.
# ---------------------------------------------------------------------------


@dataclass
class TreeDiff:
    """Structured diff between a target EDM tree and the persisted subtree.

    Two flat lists of :class:`NodeChange` / :class:`EdgeChange` records.
    Convenience properties (``inserts``, ``deletes``, ``renames``, ``moves``,
    ``updates``) bin the changes by kind for callers that want to render or
    inspect specific subsets.
    """

    node_changes: list[NodeChange] = field(default_factory=list)
    edge_changes: list[EdgeChange] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Convenience views (computed on the fly — diff is small)
    # ------------------------------------------------------------------

    @property
    def has_changes(self) -> bool:
        """``True`` if any change exists."""
        return bool(self.node_changes or self.edge_changes)

    @property
    def node_inserts(self) -> list[NodeChange]:
        return [c for c in self.node_changes if c.kind == "insert"]

    @property
    def node_updates(self) -> list[NodeChange]:
        """All node updates (renames, moves, and/or data edits)."""
        return [c for c in self.node_changes if c.kind == "update"]

    @property
    def node_renames(self) -> list[NodeChange]:
        return [c for c in self.node_changes if c.kind == "update" and c.renamed]

    @property
    def node_moves(self) -> list[NodeChange]:
        return [c for c in self.node_changes if c.kind == "update" and c.moved]

    @property
    def node_data_edits(self) -> list[NodeChange]:
        """Node updates that changed only `data` (no rename / no move)."""
        return [c for c in self.node_changes if c.kind == "update" and c.data_changed and not c.renamed and not c.moved]

    @property
    def node_deletes(self) -> list[NodeChange]:
        return [c for c in self.node_changes if c.kind == "delete"]

    @property
    def edge_inserts(self) -> list[EdgeChange]:
        return [c for c in self.edge_changes if c.kind == "insert"]

    @property
    def edge_updates(self) -> list[EdgeChange]:
        return [c for c in self.edge_changes if c.kind == "update"]

    @property
    def edge_deletes(self) -> list[EdgeChange]:
        return [c for c in self.edge_changes if c.kind == "delete"]

    # ------------------------------------------------------------------
    # Pretty-print (tree-shaped)
    # ------------------------------------------------------------------

    def print(self, file: IO[str] | None = None) -> None:
        """Render the diff as a tree-shaped textual preview.

        Output format::

            Portfolio P
            ├── ~ Site OldName → NewName               [rename]
            │   ├── + WindTurbine T03 (capacity=4.0)   [insert]
            │   ├──   WindTurbine T01                  [unchanged]
            │   ├── ~ WindTurbine T02                  [update: capacity 3.5 → 4.0]
            │   └── - Battery B1                       [delete]
            └── → Site Other                           [moved from <old_parent>]
            edges:
              + Line 'Cable-1' BusA → BusB             [insert]
        """
        out = file if file is not None else sys.stdout

        # Build a "what should the tree look like AFTER + deletes inline"
        # view, keyed by uuid. For each uuid we record:
        #   marker  — ' ', '+', '~', '-'
        #   name    — display name
        #   type    — display type
        #   note    — bracketed annotation (rename, move, insert, delete, ...)
        #   parent  — uuid of parent in the rendered tree
        view: dict[UUID, _RenderRow] = {}
        children_by_parent: dict[UUID | None, list[UUID]] = {}

        for change in self.node_changes:
            row = _render_row_for_node(change)
            view[change.uuid] = row
            children_by_parent.setdefault(row.parent_uuid, []).append(change.uuid)

        # A change is a render trunk if its parent isn't itself a change —
        # either parent_uuid is None, or the parent is an unchanged ancestor
        # (e.g. a renamed Site whose Portfolio parent was not modified).
        roots = [uid for uid, row in view.items() if row.parent_uuid not in view]

        if not view and not self.edge_changes:
            out.write("(no changes)\n")
        else:
            for root in roots:
                _render_subtree(out, view, children_by_parent, root, prefix="", is_last=True, is_root=True)

        # Edges: flat at the bottom.
        if self.edge_changes:
            out.write("edges:\n")
            for change in self.edge_changes:
                marker = _MARKER_BY_KIND[change.kind]
                note = _edge_change_note(change)
                ftxt, ttxt = _edge_endpoint_str(change)
                out.write(f"  {marker} {change.display_type} {change.display_name!r} {ftxt} → {ttxt}{note}\n")


# ---------------------------------------------------------------------------
# Render helpers (private)
# ---------------------------------------------------------------------------


@dataclass
class _RenderRow:
    marker: str
    label: str
    note: str
    parent_uuid: UUID | None


_MARKER_BY_KIND = {"insert": "+", "update": "~", "delete": "-"}


def _render_row_for_node(change: NodeChange) -> _RenderRow:
    if change.kind == "insert":
        snap = change.new
        assert snap is not None
        label = f"{snap.node_type} {snap.name!r}"
        return _RenderRow(marker="+", label=label, note="  [insert]", parent_uuid=snap.parent_uuid)

    if change.kind == "delete":
        snap = change.old
        assert snap is not None
        label = f"{snap.node_type} {snap.name!r}"
        return _RenderRow(
            marker="-",
            label=label,
            note="  [delete] (allow_delete required)",
            parent_uuid=snap.parent_uuid,
        )

    # update
    assert change.new is not None and change.old is not None
    notes: list[str] = []
    if change.renamed:
        notes.append(f"rename {change.old.name!r} → {change.new.name!r}")
    if change.moved:
        notes.append(f"moved (parent {change.old.parent_uuid} → {change.new.parent_uuid})")
    if change.data_changed:
        notes.append(_data_diff_summary(change.old.data, change.new.data))
    note = "  [" + "; ".join(notes) + "]" if notes else ""
    label = f"{change.new.node_type} {change.new.name!r}"
    return _RenderRow(marker="~", label=label, note=note, parent_uuid=change.new.parent_uuid)


def _data_diff_summary(old: dict, new: dict) -> str:
    """Human-readable summary of changed keys in two JSONB blobs."""
    keys = set(old) | set(new)
    parts: list[str] = []
    for k in sorted(keys):
        ov = old.get(k, "<unset>")
        nv = new.get(k, "<unset>")
        if ov != nv:
            parts.append(f"{k}: {ov!r} → {nv!r}")
    if not parts:
        return "data changed"
    return "; ".join(parts)


def _render_subtree(
    out: IO[str],
    view: dict[UUID, _RenderRow],
    children_by_parent: dict[UUID | None, list[UUID]],
    node_uuid: UUID,
    *,
    prefix: str,
    is_last: bool,
    is_root: bool,
) -> None:
    row = view[node_uuid]
    connector = "" if is_root else ("└── " if is_last else "├── ")
    out.write(f"{prefix}{connector}{row.marker} {row.label}{row.note}\n")
    children = children_by_parent.get(node_uuid, [])
    new_prefix = prefix + ("" if is_root else ("    " if is_last else "│   "))
    for i, child in enumerate(children):
        _render_subtree(
            out,
            view,
            children_by_parent,
            child,
            prefix=new_prefix,
            is_last=(i == len(children) - 1),
            is_root=False,
        )


def _edge_endpoint_str(change: EdgeChange) -> tuple[str, str]:
    if change.kind == "delete":
        snap = change.old
        assert snap is not None
        return str(snap.from_node_uuid), str(snap.to_node_uuid)
    snap = change.new
    assert snap is not None
    return str(snap.from_node_uuid), str(snap.to_node_uuid)


def _edge_change_note(change: EdgeChange) -> str:
    if change.kind == "insert":
        return "  [insert]"
    if change.kind == "delete":
        return "  [delete] (allow_delete required)"
    notes: list[str] = []
    if change.endpoints_changed:
        notes.append("endpoints changed")
    if change.data_changed:
        notes.append(_data_diff_summary(change.old.data, change.new.data))  # type: ignore[union-attr]
    return "  [" + "; ".join(notes) + "]" if notes else ""
