"""Unit tests for ``energydb.diff`` — TreeDiff structure, change kinds,
and tree-shaped print formatting.
"""

from __future__ import annotations

import io
from uuid import UUID, uuid4

from energydb.diff import (
    EdgeChange,
    EdgeSnapshot,
    NodeChange,
    NodeSnapshot,
    TreeDiff,
)


def _node(
    uuid: UUID,
    *,
    type: str = "Site",
    name: str = "S",
    parent: UUID | None = None,
    data: dict | None = None,
) -> NodeSnapshot:
    return NodeSnapshot(uuid=uuid, node_type=type, name=name, parent_uuid=parent, data=data or {})


def _edge(
    uuid: UUID,
    *,
    type: str = "Line",
    name: str | None = "L",
    a: UUID | None = None,
    b: UUID | None = None,
    data: dict | None = None,
) -> EdgeSnapshot:
    return EdgeSnapshot(
        uuid=uuid,
        edge_type=type,
        name=name,
        from_node_uuid=a or uuid4(),
        to_node_uuid=b or uuid4(),
        data=data or {},
    )


# ---------------------------------------------------------------------------
# NodeChange — kind classification
# ---------------------------------------------------------------------------


class TestNodeChangeKind:
    def test_insert(self):
        n = _node(uuid4(), name="new")
        c = NodeChange(old=None, new=n)
        assert c.kind == "insert"
        assert c.uuid == n.uuid
        assert c.display_name == "new"
        assert not c.renamed
        assert not c.moved
        assert not c.data_changed

    def test_delete(self):
        n = _node(uuid4(), name="gone")
        c = NodeChange(old=n, new=None)
        assert c.kind == "delete"
        assert c.uuid == n.uuid
        assert c.display_name == "gone"
        assert not c.renamed

    def test_unchanged_update_classified_only_by_kind(self):
        u = uuid4()
        n = _node(u, name="x")
        c = NodeChange(old=n, new=n)
        # Same content, both sides set → kind == update (caller filters
        # noop pairs out). Defaults all change-detector booleans to False.
        assert c.kind == "update"
        assert not c.renamed
        assert not c.moved
        assert not c.data_changed

    def test_renamed_update(self):
        u = uuid4()
        old = _node(u, name="old")
        new = _node(u, name="new")
        c = NodeChange(old=old, new=new)
        assert c.kind == "update"
        assert c.renamed
        assert not c.moved
        assert not c.data_changed

    def test_moved_update(self):
        u = uuid4()
        p1, p2 = uuid4(), uuid4()
        old = _node(u, parent=p1)
        new = _node(u, parent=p2)
        c = NodeChange(old=old, new=new)
        assert c.kind == "update"
        assert c.moved
        assert not c.renamed

    def test_data_changed_update(self):
        u = uuid4()
        old = _node(u, data={"capacity": 3.5})
        new = _node(u, data={"capacity": 4.0})
        c = NodeChange(old=old, new=new)
        assert c.kind == "update"
        assert c.data_changed

    def test_combined_rename_move_data(self):
        u = uuid4()
        p1, p2 = uuid4(), uuid4()
        old = _node(u, name="a", parent=p1, data={"v": 1})
        new = _node(u, name="b", parent=p2, data={"v": 2})
        c = NodeChange(old=old, new=new)
        assert c.renamed and c.moved and c.data_changed

    def test_must_have_old_or_new(self):
        import pytest

        with pytest.raises(ValueError):
            NodeChange(old=None, new=None)


# ---------------------------------------------------------------------------
# EdgeChange — kind classification
# ---------------------------------------------------------------------------


class TestEdgeChangeKind:
    def test_insert_uses_name_as_display_name(self):
        e = _edge(uuid4(), name="Cable-1")
        c = EdgeChange(old=None, new=e)
        assert c.kind == "insert"
        assert c.display_name == "Cable-1"

    def test_falls_back_to_edge_type_when_name_missing(self):
        e = _edge(uuid4(), name=None)
        c = EdgeChange(old=None, new=e)
        assert c.display_name == "Line"

    def test_endpoints_changed_update(self):
        u = uuid4()
        a, b, c_uuid = uuid4(), uuid4(), uuid4()
        old = _edge(u, a=a, b=b)
        new = _edge(u, a=a, b=c_uuid)
        change = EdgeChange(old=old, new=new)
        assert change.kind == "update"
        assert change.endpoints_changed
        assert not change.data_changed

    def test_data_changed_update(self):
        u = uuid4()
        a, b = uuid4(), uuid4()
        old = _edge(u, a=a, b=b, data={"capacity": 100})
        new = _edge(u, a=a, b=b, data={"capacity": 200})
        change = EdgeChange(old=old, new=new)
        assert change.data_changed
        assert not change.endpoints_changed


# ---------------------------------------------------------------------------
# TreeDiff — bin classifiers
# ---------------------------------------------------------------------------


class TestTreeDiffBins:
    def test_empty_diff_has_no_changes(self):
        d = TreeDiff()
        assert not d.has_changes
        assert d.node_inserts == []
        assert d.node_deletes == []
        assert d.node_updates == []

    def test_inserts_bin(self):
        n = _node(uuid4(), name="n")
        d = TreeDiff(node_changes=[NodeChange(old=None, new=n)])
        assert d.has_changes
        assert len(d.node_inserts) == 1
        assert d.node_deletes == []

    def test_renames_and_moves_bins(self):
        u1, u2 = uuid4(), uuid4()
        renamed = NodeChange(old=_node(u1, name="a"), new=_node(u1, name="b"))
        moved = NodeChange(
            old=_node(u2, parent=uuid4()),
            new=_node(u2, parent=uuid4()),
        )
        d = TreeDiff(node_changes=[renamed, moved])
        assert len(d.node_renames) == 1
        assert d.node_renames[0].uuid == u1
        assert len(d.node_moves) == 1
        assert d.node_moves[0].uuid == u2

    def test_data_edits_bin_excludes_renames_and_moves(self):
        u1, u2 = uuid4(), uuid4()
        # Pure data edit → goes in data_edits.
        pure_data = NodeChange(old=_node(u1, data={"x": 1}), new=_node(u1, data={"x": 2}))
        # Rename + data → goes in renames, not data_edits.
        rename_and_data = NodeChange(
            old=_node(u2, name="a", data={"x": 1}),
            new=_node(u2, name="b", data={"x": 2}),
        )
        d = TreeDiff(node_changes=[pure_data, rename_and_data])
        assert [c.uuid for c in d.node_data_edits] == [u1]
        assert [c.uuid for c in d.node_renames] == [u2]


# ---------------------------------------------------------------------------
# TreeDiff.render() — tree-shaped output
# ---------------------------------------------------------------------------


class TestTreeDiffRender:
    def _capture(self, diff: TreeDiff) -> str:
        buf = io.StringIO()
        diff.render(file=buf)
        return buf.getvalue()

    def test_empty_diff_prints_no_changes(self):
        out = self._capture(TreeDiff())
        assert "no changes" in out

    def test_insert_at_root(self):
        root = _node(uuid4(), type="Portfolio", name="P")
        d = TreeDiff(node_changes=[NodeChange(old=None, new=root)])
        out = self._capture(d)
        assert "+ Portfolio 'P'" in out
        assert "[insert]" in out

    def test_rename_renders_arrow(self):
        u = uuid4()
        d = TreeDiff(
            node_changes=[
                NodeChange(old=_node(u, type="Site", name="OldName"), new=_node(u, type="Site", name="NewName")),
            ]
        )
        out = self._capture(d)
        assert "~ Site 'NewName'" in out
        assert "rename 'OldName' → 'NewName'" in out

    def test_delete_marker(self):
        u = uuid4()
        d = TreeDiff(node_changes=[NodeChange(old=_node(u, type="Battery", name="B1"), new=None)])
        out = self._capture(d)
        assert "- Battery 'B1'" in out
        assert "[delete]" in out

    def test_data_edit_summary(self):
        u = uuid4()
        d = TreeDiff(
            node_changes=[
                NodeChange(
                    old=_node(u, type="WindTurbine", name="T", data={"capacity": 3.5}),
                    new=_node(u, type="WindTurbine", name="T", data={"capacity": 4.0}),
                )
            ]
        )
        out = self._capture(d)
        assert "capacity: 3.5 → 4.0" in out

    def test_tree_shape_with_parent_child(self):
        """Insert under an explicit parent renders nested under it."""
        root = uuid4()
        child = uuid4()
        d = TreeDiff(
            node_changes=[
                NodeChange(old=None, new=_node(root, type="Portfolio", name="P")),
                NodeChange(old=None, new=_node(child, type="Site", name="S", parent=root)),
            ]
        )
        out = self._capture(d)
        # Root line precedes child line; child appears with a tree connector.
        lines = out.splitlines()
        p_idx = next(i for i, line in enumerate(lines) if "Portfolio" in line)
        s_idx = next(i for i, line in enumerate(lines) if "Site" in line)
        assert p_idx < s_idx
        assert "└──" in lines[s_idx] or "├──" in lines[s_idx]

    def test_edges_section(self):
        a, b = uuid4(), uuid4()
        edge_uuid = uuid4()
        d = TreeDiff(
            edge_changes=[
                EdgeChange(old=None, new=_edge(edge_uuid, type="Line", name="Cable", a=a, b=b)),
            ]
        )
        out = self._capture(d)
        assert "edges:" in out
        assert "+ Line 'Cable'" in out

    def test_change_under_unchanged_ancestor_renders(self):
        """A change whose parent is unchanged (and thus absent from the diff)
        must still render — it becomes a render trunk in its own right.
        Regression for `diff.render()` silently producing no output when
        ``replace_subtree`` only edits a deep node."""
        unchanged_portfolio = uuid4()
        site_uuid = uuid4()
        d = TreeDiff(
            node_changes=[
                NodeChange(
                    old=_node(site_uuid, type="Site", name="Old", parent=unchanged_portfolio),
                    new=_node(site_uuid, type="Site", name="New", parent=unchanged_portfolio),
                ),
            ]
        )
        out = self._capture(d)
        assert "~ Site 'New'" in out
        assert "rename 'Old' → 'New'" in out

    def test_mixed_subtree_edit_under_unchanged_root(self):
        """Mirrors the notebook flow: rename a Site, edit a child's data,
        delete a sibling — all under an unchanged Portfolio. The Site is
        the render trunk; the two children nest under it."""
        portfolio = uuid4()
        site = uuid4()
        t01 = uuid4()
        t02 = uuid4()
        d = TreeDiff(
            node_changes=[
                NodeChange(
                    old=_node(site, type="Site", name="Offshore-1", parent=portfolio),
                    new=_node(site, type="Site", name="Offshore-Renamed", parent=portfolio),
                ),
                NodeChange(
                    old=_node(t01, type="WindTurbine", name="T01", parent=site, data={"capacity": 3.5}),
                    new=_node(t01, type="WindTurbine", name="T01", parent=site, data={"capacity": 4.0}),
                ),
                NodeChange(
                    old=_node(t02, type="WindTurbine", name="T02", parent=site),
                    new=None,
                ),
            ]
        )
        out = self._capture(d)
        lines = out.splitlines()
        site_idx = next(i for i, line in enumerate(lines) if "Site 'Offshore-Renamed'" in line)
        t01_idx = next(i for i, line in enumerate(lines) if "WindTurbine 'T01'" in line)
        t02_idx = next(i for i, line in enumerate(lines) if "WindTurbine 'T02'" in line)
        # Site comes first (no indent), children below indented under it.
        assert site_idx < t01_idx < t02_idx
        assert lines[site_idx].lstrip().startswith("~")
        for child_line in (lines[t01_idx], lines[t02_idx]):
            assert "├──" in child_line or "└──" in child_line
        assert "capacity: 3.5 → 4.0" in out
        assert "- WindTurbine 'T02'" in out

    def test_edges_only_diff_does_not_say_no_changes(self):
        """A diff with only edge changes must not also print '(no changes)'."""
        edge_uuid = uuid4()
        d = TreeDiff(edge_changes=[EdgeChange(old=None, new=_edge(edge_uuid, type="Line", name="L"))])
        out = self._capture(d)
        assert "no changes" not in out
        assert "edges:" in out
