"""TreeDiff: compute and represent the difference produced by a mutation.

Categorizes node/edge changes into insert / update / delete buckets, keyed
by UUID, and renders a tree-shaped preview. Returned by
:meth:`Client.register_tree` (``dry_run=True``), by each scope mutator
when called with ``dry_run=True``, and by :meth:`Transaction.preview`.

With UUID identity, a rename (name changed) and a move (parent_uuid
changed) are *not* delete-then-insert; the row keeps its uuid and just has
its column updated. The diff structure exposes "renamed" and "moved"
booleans on each update so the user-facing print can label changes
accurately.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import IO, Any
from uuid import UUID

from energydb.errors import ValidationError


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


@dataclass(frozen=True)
class _BaseChange[SnapshotT]:
    """Shared structure for :class:`NodeChange` and :class:`EdgeChange`.

    ``old`` is the DB state before the change; ``new`` is the target state
    after. Inserts have ``old=None``; deletes have ``new=None``. At least
    one must be set.
    """

    old: SnapshotT | None
    new: SnapshotT | None

    def __post_init__(self):
        if self.old is None and self.new is None:
            raise ValidationError(f"{type(self).__name__} must have at least one of old/new set.")

    def _present(self) -> SnapshotT:
        """Return whichever of ``new`` / ``old`` is non-null. For inserts
        and updates that's ``new``; for deletes it's ``old``."""
        return self.new if self.new is not None else self.old  # type: ignore[return-value]

    @property
    def uuid(self) -> UUID:
        """The changed element's UUID, taken from whichever of ``new`` /
        ``old`` is present (they always share it)."""
        return self._present().uuid  # type: ignore[attr-defined]

    @property
    def kind(self) -> str:
        """``"insert"`` (no ``old``), ``"delete"`` (no ``new``), or
        ``"update"`` (both present)."""
        if self.old is None:
            return "insert"
        if self.new is None:
            return "delete"
        return "update"

    @property
    def data_changed(self) -> bool:
        """``True`` when this is an update whose ``data`` payload differs.
        Always ``False`` for inserts and deletes."""
        return self.kind == "update" and self.old.data != self.new.data  # type: ignore[union-attr]


@dataclass(frozen=True)
class NodeChange(_BaseChange[NodeSnapshot]):
    """A single node-level diff entry."""

    @property
    def display_name(self) -> str:
        """The node's name, for rendering the diff."""
        return self._present().name

    @property
    def display_type(self) -> str:
        """The node's ``node_type``, for rendering the diff."""
        return self._present().node_type

    @property
    def renamed(self) -> bool:
        """``True`` when this update changed the node's ``name``."""
        return self.kind == "update" and self.old.name != self.new.name  # type: ignore[union-attr]

    @property
    def moved(self) -> bool:
        """``True`` when this update re-parented the node
        (``parent_uuid`` changed)."""
        return self.kind == "update" and self.old.parent_uuid != self.new.parent_uuid  # type: ignore[union-attr]


@dataclass(frozen=True)
class EdgeChange(_BaseChange[EdgeSnapshot]):
    """A single edge-level diff entry."""

    @property
    def display_name(self) -> str:
        """The edge's name, falling back to its ``edge_type`` when unnamed."""
        snap = self._present()
        return snap.name or snap.edge_type

    @property
    def display_type(self) -> str:
        """The edge's ``edge_type``, for rendering the diff."""
        return self._present().edge_type

    @property
    def endpoints_changed(self) -> bool:
        """``True`` when this update moved either endpoint of the edge."""
        return self.kind == "update" and (
            self.old.from_node_uuid != self.new.from_node_uuid  # type: ignore[union-attr]
            or self.old.to_node_uuid != self.new.to_node_uuid  # type: ignore[union-attr]
        )


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

    @property
    def has_changes(self) -> bool:
        """``True`` if any change exists."""
        return bool(self.node_changes or self.edge_changes)

    @property
    def node_inserts(self) -> list[NodeChange]:
        """Nodes created by this diff."""
        return [c for c in self.node_changes if c.kind == "insert"]

    @property
    def node_updates(self) -> list[NodeChange]:
        """All node updates (renames, moves, and/or data edits)."""
        return [c for c in self.node_changes if c.kind == "update"]

    @property
    def node_renames(self) -> list[NodeChange]:
        """Node updates that changed the name (may also have moved)."""
        return [c for c in self.node_changes if c.kind == "update" and c.renamed]

    @property
    def node_moves(self) -> list[NodeChange]:
        """Node updates that changed the parent (may also have been renamed)."""
        return [c for c in self.node_changes if c.kind == "update" and c.moved]

    @property
    def node_data_edits(self) -> list[NodeChange]:
        """Node updates that changed only `data` (no rename / no move)."""
        return [c for c in self.node_changes if c.kind == "update" and c.data_changed and not c.renamed and not c.moved]

    @property
    def node_deletes(self) -> list[NodeChange]:
        """Nodes removed by this diff."""
        return [c for c in self.node_changes if c.kind == "delete"]

    @property
    def edge_inserts(self) -> list[EdgeChange]:
        """Edges created by this diff."""
        return [c for c in self.edge_changes if c.kind == "insert"]

    @property
    def edge_updates(self) -> list[EdgeChange]:
        """Edges whose endpoints, name, or ``data`` changed."""
        return [c for c in self.edge_changes if c.kind == "update"]

    @property
    def edge_deletes(self) -> list[EdgeChange]:
        """Edges removed by this diff."""
        return [c for c in self.edge_changes if c.kind == "delete"]

    def render(self, file: IO[str] | None = None) -> None:
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

        # Post-change view of the tree with deletes inline, keyed by uuid:
        #   marker: ' ', '+', '~', '-'
        #   type: display type
        view: dict[UUID, _RenderRow] = {}
        children_by_parent: dict[UUID | None, list[UUID]] = {}

        for change in self.node_changes:
            row = _render_row_for_node(change)
            view[change.uuid] = row
            children_by_parent.setdefault(row.parent_uuid, []).append(change.uuid)

        # A change is a render trunk when its parent is not itself a change.
        roots = [uid for uid, row in view.items() if row.parent_uuid not in view]

        if not view and not self.edge_changes:
            out.write("(no changes)\n")
        else:
            for root in roots:
                _render_subtree(out, view, children_by_parent, root, prefix="", is_last=True, is_root=True)

        if self.edge_changes:
            out.write("edges:\n")
            for change in self.edge_changes:
                marker = _MARKER_BY_KIND[change.kind]
                note = _edge_change_note(change)
                ftxt, ttxt = _edge_endpoint_str(change)
                out.write(f"  {marker} {change.display_type} {change.display_name!r} {ftxt} → {ttxt}{note}\n")


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
            note="  [delete]",
            parent_uuid=snap.parent_uuid,
        )

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
        return "  [delete]"
    notes: list[str] = []
    if change.endpoints_changed:
        notes.append("endpoints changed")
    if change.data_changed:
        notes.append(_data_diff_summary(change.old.data, change.new.data))  # type: ignore[union-attr]
    return "  [" + "; ".join(notes) + "]" if notes else ""
