"""Unit + integration tests for the per-client ``SeriesRegistry`` cache.

Unit tests exercise the cache class in isolation (no DB). Integration tests
verify the read-through, write-through, and eviction hooks against a live
PG / CH stack — skipped when ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL`` are not
set.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import polars as pl
import pytest
from energydb._resolve_cache import EdgeMeta, NodeMeta, SeriesMeta, SeriesRegistry

# ---------------------------------------------------------------------------
# Unit tests (no DB)
# ---------------------------------------------------------------------------


class TestSeriesRegistryUnit:
    def test_empty_lookup_returns_all_misses(self):
        reg = SeriesRegistry()
        triples = [("u1", "actual", "power"), ("u1", "forecast", "power")]
        hits, misses = reg.lookup_triples(triples)
        assert hits == {}
        assert sorted(misses) == sorted(triples)
        stats = reg.stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 2
        assert stats["size"] == 0

    def test_insert_then_lookup_is_a_hit(self):
        reg = SeriesRegistry()
        meta = SeriesMeta(series_id=42, canonical_unit="MW", timeseries_type="FLAT", retention="forever")
        reg.insert("u1", "actual", "power", meta)

        hits, misses = reg.lookup_triples([("u1", "actual", "power")])
        assert hits == {("u1", "actual", "power"): meta}
        assert misses == []
        stats = reg.stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 0
        assert stats["size"] == 1

    def test_mixed_hit_and_miss(self):
        reg = SeriesRegistry()
        meta = SeriesMeta(series_id=1, canonical_unit="MW", timeseries_type="FLAT", retention="forever")
        reg.insert("u1", "actual", "power", meta)

        hits, misses = reg.lookup_triples(
            [
                ("u1", "actual", "power"),
                ("u1", "forecast", "power"),
                ("u2", "actual", "power"),
            ]
        )
        assert hits == {("u1", "actual", "power"): meta}
        assert sorted(misses) == sorted([("u1", "forecast", "power"), ("u2", "actual", "power")])
        assert reg.stats()["hits"] == 1
        assert reg.stats()["misses"] == 2

    def test_duplicate_triples_each_count_separately(self):
        reg = SeriesRegistry()
        meta = SeriesMeta(series_id=1, canonical_unit="MW", timeseries_type="FLAT", retention="forever")
        reg.insert("u1", "actual", "power", meta)

        hits, misses = reg.lookup_triples(
            [("u1", "actual", "power"), ("u1", "actual", "power"), ("u1", "actual", "power")]
        )
        assert hits == {("u1", "actual", "power"): meta}
        assert misses == []
        assert reg.stats()["hits"] == 3

    def test_get_by_id_after_insert(self):
        reg = SeriesRegistry()
        meta = SeriesMeta(series_id=42, canonical_unit="MW", timeseries_type="FLAT", retention="forever")
        reg.insert("u1", "actual", "power", meta)
        assert reg.get_by_id(42) is meta
        assert reg.get_by_id(99) is None

    def test_evict_owner_drops_all_entries_for_owner(self):
        reg = SeriesRegistry()
        m1 = SeriesMeta(series_id=1, canonical_unit="MW", timeseries_type="FLAT", retention="forever")
        m2 = SeriesMeta(series_id=2, canonical_unit="MW", timeseries_type="OVERLAPPING", retention="medium")
        m3 = SeriesMeta(series_id=3, canonical_unit="MW", timeseries_type="FLAT", retention="forever")
        reg.insert("u1", "actual", "power", m1)
        reg.insert("u1", "forecast", "power", m2)
        reg.insert("u2", "actual", "power", m3)

        reg.evict_owner("u1")

        hits, misses = reg.lookup_triples(
            [("u1", "actual", "power"), ("u1", "forecast", "power"), ("u2", "actual", "power")]
        )
        assert hits == {("u2", "actual", "power"): m3}
        assert sorted(misses) == sorted([("u1", "actual", "power"), ("u1", "forecast", "power")])
        # Reverse index also evicted.
        assert reg.get_by_id(1) is None
        assert reg.get_by_id(2) is None
        assert reg.get_by_id(3) is m3

    def test_evict_owner_for_unknown_uuid_is_a_noop(self):
        reg = SeriesRegistry()
        reg.insert(
            "u1",
            "actual",
            "power",
            SeriesMeta(series_id=1, canonical_unit="MW", timeseries_type="FLAT", retention="forever"),
        )
        reg.evict_owner("nonexistent")
        assert reg.stats()["size"] == 1

    def test_clear_resets_state_and_stats(self):
        reg = SeriesRegistry()
        reg.insert(
            "u1",
            "actual",
            "power",
            SeriesMeta(series_id=1, canonical_unit="MW", timeseries_type="FLAT", retention="forever"),
        )
        reg.lookup_triples([("u1", "actual", "power"), ("missing", "x", "y")])
        assert reg.stats()["size"] == 1

        reg.clear()
        stats = reg.stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["size"] == 0
        assert stats["node_size"] == 0
        assert stats["edge_size"] == 0
        assert reg.get_by_id(1) is None


class TestNodeCacheUnit:
    def test_insert_then_lookup_is_a_hit(self):
        reg = SeriesRegistry()
        meta = NodeMeta.from_path(("Europe", "Sweden"), name="Sweden", node_type="Country")
        reg.insert_node("u-se", meta, parent_uuid="u-eu")

        hits, misses = reg.lookup_nodes(["u-se"])
        assert hits == {"u-se": meta}
        assert misses == []
        assert reg.stats()["node_hits"] == 1

    def test_mixed_node_hit_and_miss(self):
        reg = SeriesRegistry()
        meta = NodeMeta.from_path(("A",), name="A", node_type="X")
        reg.insert_node("u-a", meta, parent_uuid=None)

        hits, misses = reg.lookup_nodes(["u-a", "u-b", "u-a"])
        assert hits == {"u-a": meta}
        assert sorted(misses) == ["u-b"]
        # Two hits (the duplicate), one miss.
        assert reg.stats()["node_hits"] == 2
        assert reg.stats()["node_misses"] == 1

    def test_evict_node_subtree_walks_children(self):
        reg = SeriesRegistry()
        # u-eu -> u-se -> u-sthlm
        reg.insert_node("u-eu", NodeMeta.from_path(("EU",), name="EU", node_type="X"), parent_uuid=None)
        reg.insert_node("u-se", NodeMeta.from_path(("EU", "SE"), name="SE", node_type="X"), parent_uuid="u-eu")
        reg.insert_node("u-sthlm", NodeMeta.from_path(("EU", "SE", "S"), name="S", node_type="X"), parent_uuid="u-se")

        reg.evict_node_subtree("u-se")
        assert reg.get_node("u-eu") is not None
        assert reg.get_node("u-se") is None
        assert reg.get_node("u-sthlm") is None

    def test_evict_node_subtree_for_unknown_uuid_is_noop(self):
        reg = SeriesRegistry()
        reg.insert_node("u-a", NodeMeta.from_path(("A",), name="A", node_type="X"), parent_uuid=None)
        reg.evict_node_subtree("nonexistent")
        assert reg.get_node("u-a") is not None

    def test_reparent_in_cache_updates_indexes(self):
        reg = SeriesRegistry()
        reg.insert_node("u-a", NodeMeta.from_path(("A",), name="A", node_type="X"), parent_uuid=None)
        reg.insert_node("u-b", NodeMeta.from_path(("A", "B"), name="B", node_type="X"), parent_uuid="u-a")
        # Move B under a fresh parent C; later evicting old parent A must not
        # take B with it.
        reg.insert_node("u-c", NodeMeta.from_path(("C",), name="C", node_type="X"), parent_uuid=None)
        reg.insert_node("u-b", NodeMeta.from_path(("C", "B"), name="B", node_type="X"), parent_uuid="u-c")
        reg.evict_node_subtree("u-a")
        assert reg.get_node("u-a") is None
        assert reg.get_node("u-b") is not None


class TestHashMemo:
    """The ``hash_rows`` → ``SeriesMeta`` memo powers the warm-cache
    short-circuit in :func:`resolve_manifest` — once a triple's polars
    hash has been seen, subsequent resolves skip the 4-column unique on
    the manifest and attach metadata directly via this memo."""

    def _seed(self, reg: SeriesRegistry) -> tuple[tuple[str, str, str], int, SeriesMeta]:
        triple = ("u1", "actual", "power")
        h = 12345  # arbitrary 64-bit value; the actual polars hash isn't relevant in unit tests.
        meta = SeriesMeta(series_id=42, canonical_unit="MW", timeseries_type="FLAT", retention="forever")
        reg.insert(*triple, meta)
        reg.populate_memo([(h, triple, meta)])
        return triple, h, meta

    def test_populate_then_lookup_is_a_hit(self):
        reg = SeriesRegistry()
        _, h, meta = self._seed(reg)
        hits, misses = reg.lookup_hashes([h, 999])
        assert hits == {h: meta}
        assert misses == [999]
        stats = reg.stats()
        assert stats["hash_hits"] == 1
        assert stats["hash_misses"] == 1
        assert stats["hash_memo_size"] == 1

    def test_evict_owner_drops_memo_entry(self):
        reg = SeriesRegistry()
        triple, h, _ = self._seed(reg)
        reg.evict_owner(triple[0])
        hits, misses = reg.lookup_hashes([h])
        assert hits == {}
        assert misses == [h]
        # Reverse map is gone too.
        assert reg.stats()["hash_memo_size"] == 0

    def test_clear_empties_memo_and_reverse_map(self):
        reg = SeriesRegistry()
        _, h, _ = self._seed(reg)
        reg.clear()
        stats = reg.stats()
        assert stats["hash_memo_size"] == 0
        assert stats["hash_hits"] == 0
        assert stats["hash_misses"] == 0
        hits, _ = reg.lookup_hashes([h])
        assert hits == {}

    def test_insert_updates_memo_for_seen_triple(self):
        """If a triple has been seen by the memo, re-inserting it (e.g. an
        in-process registration update) must keep memo and triple cache
        in sync — otherwise the warm path would serve stale metadata."""
        reg = SeriesRegistry()
        triple, h, _ = self._seed(reg)
        new_meta = SeriesMeta(series_id=42, canonical_unit="kW", timeseries_type="FLAT", retention="medium")
        reg.insert(*triple, new_meta)
        hits, _ = reg.lookup_hashes([h])
        assert hits == {h: new_meta}

    def test_insert_does_not_invent_memo_entries(self):
        """A bare ``insert`` of a triple never seen by the memo must not
        create a phantom memo entry — we don't know the polars hash for
        it. The memo populates lazily on the next resolve."""
        reg = SeriesRegistry()
        reg.insert(
            "u1",
            "actual",
            "power",
            SeriesMeta(series_id=42, canonical_unit="MW", timeseries_type="FLAT", retention="forever"),
        )
        assert reg.stats()["hash_memo_size"] == 0


class TestReversePathIndex:
    """The ``joined_path → node_uuid`` reverse index powers warm-cache
    short-circuit on path-routed writes."""

    def test_lookup_paths_empty_returns_all_misses(self):
        reg = SeriesRegistry()
        hits, misses = reg.lookup_paths(["A/B", "C"])
        assert hits == {}
        assert sorted(misses) == ["A/B", "C"]

    def test_insert_node_populates_reverse_index(self):
        reg = SeriesRegistry()
        reg.insert_node(
            "u-1",
            NodeMeta.from_path(("Europe", "Sweden"), name="Sweden", node_type="Country"),
            parent_uuid=None,
        )
        hits, misses = reg.lookup_paths(["Europe/Sweden", "Europe/Norway"])
        assert hits == {"Europe/Sweden": "u-1"}
        assert misses == ["Europe/Norway"]

    def test_evict_node_subtree_drops_reverse_entry(self):
        reg = SeriesRegistry()
        reg.insert_node("u-eu", NodeMeta.from_path(("EU",), name="EU", node_type="X"), parent_uuid=None)
        reg.insert_node("u-se", NodeMeta.from_path(("EU", "SE"), name="SE", node_type="X"), parent_uuid="u-eu")
        reg.insert_node(
            "u-sthlm",
            NodeMeta.from_path(("EU", "SE", "S"), name="S", node_type="X"),
            parent_uuid="u-se",
        )
        # Reverse index has all three before eviction.
        assert reg.lookup_paths(["EU/SE", "EU/SE/S"])[0] == {"EU/SE": "u-se", "EU/SE/S": "u-sthlm"}

        reg.evict_node_subtree("u-se")
        hits, _ = reg.lookup_paths(["EU", "EU/SE", "EU/SE/S"])
        # Only the un-evicted ancestor survives.
        assert hits == {"EU": "u-eu"}

    def test_rename_in_cache_drops_stale_path(self):
        """A re-insert under a new path must not leave the old path key dangling."""
        reg = SeriesRegistry()
        reg.insert_node("u-1", NodeMeta.from_path(("A",), name="A", node_type="X"), parent_uuid=None)
        # Simulate rename: same uuid, new joined_path.
        reg.insert_node("u-1", NodeMeta.from_path(("A2",), name="A2", node_type="X"), parent_uuid=None)
        hits, misses = reg.lookup_paths(["A", "A2"])
        assert hits == {"A2": "u-1"}
        assert misses == ["A"]

    def test_clear_drops_reverse_index(self):
        reg = SeriesRegistry()
        reg.insert_node("u-1", NodeMeta.from_path(("A",), name="A", node_type="X"), parent_uuid=None)
        reg.clear()
        hits, misses = reg.lookup_paths(["A"])
        assert hits == {}
        assert misses == ["A"]


class TestEdgeCacheUnit:
    def test_insert_then_lookup_is_a_hit(self):
        reg = SeriesRegistry()
        meta = EdgeMeta(name="cable-1", edge_type="Cable", from_node_uuid="u-a", to_node_uuid="u-b")
        reg.insert_edge("e-1", meta)

        hits, misses = reg.lookup_edges(["e-1"])
        assert hits == {"e-1": meta}
        assert misses == []
        assert reg.stats()["edge_hits"] == 1

    def test_evict_edge_drops_entry(self):
        reg = SeriesRegistry()
        reg.insert_edge(
            "e-1",
            EdgeMeta(name="x", edge_type="Cable", from_node_uuid="u-a", to_node_uuid="u-b"),
        )
        reg.evict_edge("e-1")
        assert reg.lookup_edges(["e-1"])[0] == {}


# ---------------------------------------------------------------------------
# Integration tests (require live PG + CH)
# ---------------------------------------------------------------------------


pytestmark_integration = pytest.mark.skipif(
    not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")),
    reason="TIMEDB_PG_DSN / TIMEDB_CH_URL not set — skipping integration tests",
)


BASE_VT = datetime(2026, 1, 1, tzinfo=UTC)


def _ts_df(n: int = 3) -> pl.DataFrame:
    times = pl.datetime_range(
        start=BASE_VT,
        end=BASE_VT + timedelta(hours=n - 1),
        interval="1h",
        time_unit="us",
        time_zone="UTC",
        eager=True,
    )
    return pl.DataFrame({"valid_time": times, "value": [float(i) for i in range(n)]})


@pytest.fixture
def live_client():
    from energydb import Client

    client = Client()
    client.delete()
    client.create()
    yield client
    client.delete()
    client.close()


@pytestmark_integration
class TestRegistryWithLiveDB:
    def test_register_series_writes_through_to_cache(self, live_client):
        import energydb as edb

        live_client.register_tree(edb.wind.WindTurbine(name="T1", capacity=1.0))
        sid = live_client.get_node("T1").register_series(
            name="power",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
        )
        # Write-through populated the cache directly.
        meta = live_client._series_registry.get_by_id(sid)
        assert meta is not None
        assert meta.canonical_unit == "MW"
        assert meta.timeseries_type == "FLAT"

    def test_second_write_hits_cache(self, live_client):
        import energydb as edb

        live_client.register_tree(edb.wind.WindTurbine(name="T1", capacity=1.0))
        live_client.get_node("T1").register_series(
            name="power",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
        )

        # First write fills the cache (or finds it pre-filled by registration).
        live_client.get_node("T1").write(_ts_df(), data_type="actual", name="power")
        stats_after_first = live_client.resolve_cache_stats()

        # Second write should be served purely from the cache. Resolve now
        # has two probe layers — the hash memo (fast path) and the triple
        # cache (slow path, hit on cold memo). A warm second write may hit
        # either layer; the invariant that matters is "no new misses".
        live_client.get_node("T1").write(_ts_df(), data_type="actual", name="power")
        stats_after_second = live_client.resolve_cache_stats()
        total_hits_first = stats_after_first["hits"] + stats_after_first["hash_hits"]
        total_hits_second = stats_after_second["hits"] + stats_after_second["hash_hits"]
        assert total_hits_second > total_hits_first
        assert stats_after_second["misses"] == stats_after_first["misses"]
        assert stats_after_second["hash_misses"] == stats_after_first["hash_misses"]

    def test_unregistered_triple_raises_and_does_not_poison_cache(self, live_client):
        import energydb as edb

        live_client.register_tree(edb.wind.WindTurbine(name="T1", capacity=1.0))
        # No register_series — write should fail.
        with pytest.raises(ValueError, match="Series not registered"):
            live_client.get_node("T1").write(_ts_df(), data_type="actual", name="never_registered")

        # Now register and try again — should succeed (not poisoned).
        live_client.get_node("T1").register_series(
            name="never_registered",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
        )
        live_client.get_node("T1").write(_ts_df(), data_type="actual", name="never_registered")

    def test_node_delete_evicts_cache(self, live_client):
        import energydb as edb

        live_client.register_tree(edb.wind.WindTurbine(name="T1", capacity=1.0))
        sid = live_client.get_node("T1").register_series(
            name="power",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
        )
        assert live_client._series_registry.get_by_id(sid) is not None

        live_client.get_node("T1").delete()
        assert live_client._series_registry.get_by_id(sid) is None

    def test_invalidate_series_cache_clears(self, live_client):
        import energydb as edb

        live_client.register_tree(edb.wind.WindTurbine(name="T1", capacity=1.0))
        live_client.get_node("T1").register_series(
            name="power",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
        )
        assert live_client.resolve_cache_stats()["size"] > 0

        live_client.invalidate_series_cache()
        stats = live_client.resolve_cache_stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["size"] == 0
        assert stats["node_size"] == 0
        assert stats["edge_size"] == 0

    def test_read_warms_node_cache(self, live_client):
        import energydb as edb

        live_client.register_tree(edb.wind.WindTurbine(name="T1", capacity=1.0))
        live_client.get_node("T1").register_series(
            name="power",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
        )
        live_client.get_node("T1").write(_ts_df(), data_type="actual", name="power")

        # First read: cold miss on the node cache.
        live_client.invalidate_series_cache()
        live_client.get_node("T1").read()
        stats_after_first = live_client.resolve_cache_stats()
        assert stats_after_first["node_size"] >= 1
        misses_after_first = stats_after_first["node_misses"]

        # Second read: must hit the node cache (no new misses).
        live_client.get_node("T1").read()
        stats_after_second = live_client.resolve_cache_stats()
        assert stats_after_second["node_hits"] > stats_after_first["node_hits"]
        assert stats_after_second["node_misses"] == misses_after_first

    def test_rename_evicts_node_subtree(self, live_client):
        import energydb as edb

        # Two-level tree: WindFarm -> WindTurbine. After warming the cache,
        # renaming the farm must evict the cached turbine too.
        farm = edb.wind.WindFarm(name="F1")
        farm.add_child(edb.wind.WindTurbine(name="T1", capacity=1.0))
        live_client.register_tree(farm)
        live_client.get_node("F1", "T1").register_series(
            name="power",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
        )
        live_client.get_node("F1", "T1").write(_ts_df(), data_type="actual", name="power")
        live_client.get_node("F1", "T1").read()  # warm
        with live_client._pool.connection() as conn:
            farm_uuid = str(live_client.get_node("F1")._resolve_node_uuid(conn))
            turbine_uuid = str(live_client.get_node("F1", "T1")._resolve_node_uuid(conn))

        # Sanity: turbine should be cached (we just read through it).
        assert live_client._series_registry.get_node(turbine_uuid) is not None

        live_client.get_node("F1").rename("F1_renamed")
        # After rename, the farm AND its descendant turbine entries should
        # be gone — the cache will refill cold on next read.
        assert live_client._series_registry.get_node(farm_uuid) is None
        assert live_client._series_registry.get_node(turbine_uuid) is None

    def test_move_to_evicts_node_subtree(self, live_client):
        import energydb as edb

        farm_a = edb.wind.WindFarm(name="A")
        farm_a.add_child(edb.wind.WindTurbine(name="T1", capacity=1.0))
        farm_b = edb.wind.WindFarm(name="B")
        live_client.register_tree(farm_a)
        live_client.register_tree(farm_b)
        live_client.get_node("A", "T1").register_series(
            name="power",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
        )
        live_client.get_node("A", "T1").write(_ts_df(), data_type="actual", name="power")
        live_client.get_node("A", "T1").read()  # warm

        live_client.get_node("A", "T1").move_to(live_client.get_node("B"))
        # Subsequent read under the new path must succeed and reflect the move.
        # Scope-style read on a single-series target auto-strips identity cols
        # (path/data_type/name) — to confirm the moved path is observable
        # through the read pipeline, use Client.read directly with a manifest.
        result = live_client.get_node("B", "T1").read()
        assert result.height > 0
        manifest = pl.DataFrame(
            {
                "path": ["B/T1"],
                "data_type": ["actual"],
                "name": ["power"],
            }
        )
        full = live_client.read(manifest)
        assert full["path"].to_list()[0] == "B/T1"

    def test_resolve_query_uses_unnest_join_no_text_cast(self, live_client):
        """Smoke-test: the resolve query no longer carries a ``::text`` cast on
        the owner column. We can't easily inspect the rendered SQL, so we
        rely on the broader integration path completing successfully against
        a real PG. A cold (uncached) lookup must still resolve.
        """
        import energydb as edb

        live_client.register_tree(edb.wind.WindTurbine(name="T1", capacity=1.0))
        live_client.get_node("T1").register_series(
            name="power",
            canonical_unit="MW",
            data_type="actual",
            timeseries_type="FLAT",
        )
        # Force a cold lookup by clearing the cache.
        live_client.invalidate_series_cache()
        live_client.get_node("T1").write(_ts_df(), data_type="actual", name="power")
        # Got here without a PG error → resolve query is valid.
        assert live_client.resolve_cache_stats()["misses"] >= 1


# ---------------------------------------------------------------------------
# Resolve-path SQL-shape unit test (no DB) — confirms the rewrite
# ---------------------------------------------------------------------------


class TestResolveSqlShape:
    """Regression guard: the resolve query must not cast the indexed UUID
    column to text (that breaks the partial index on the owner column).

    Phase 2 (2026-05-15) swapped the cold-resolve query from
    ``unnest(...) JOIN series ON (owner, dt, name)`` to a bulk
    ``WHERE owner_col = ANY(::uuid[])`` because the latter is 2.7–3.3×
    faster — the unnest plan paid for materializing three arrays and a
    three-column join when a single indexed scan already returns the rows
    we need (extras land in the registry as free pre-warm).
    """

    def test_no_text_cast_in_resolve_sql(self):
        import inspect

        from energydb import paths

        src = inspect.getsource(paths._resolve_manifest_by_owner)
        # Must not cast the owner column to text on the indexed side —
        # ``ix_series_node_uuid`` / ``ix_series_edge_uuid`` are uuid-typed.
        assert "::text = ANY" not in src
        assert "owner_col}::text" not in src

    def test_uses_bulk_any_lookup(self):
        import inspect

        from energydb import paths

        src = inspect.getsource(paths._resolve_manifest_by_owner)
        # Cold-resolve fetches by owner via the partial uuid index — the
        # query must pass a uuid[] (not text[]) to keep the index applicable.
        assert "{owner_col} = ANY(%s::uuid[])" in src


# Quiet pyright/ty about the unused uuid4 import (kept for future tests).
_ = uuid4
