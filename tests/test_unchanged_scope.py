"""``skip_unchanged`` scope selection — per-series comparison keys.

``unchanged_scope`` used to be a per-call flag while OVERLAPPING is a
per-series property, so a manifest mixing the two could not be written
correctly with any setting: the ``valid_time`` key silently dropped genuine
forecast republications. The default is now ``"auto"`` (each series compared
by the key its type needs), and an explicit ``"valid_time"`` over OVERLAPPING
series raises instead of losing data.

The pure tests cover the guard and the summary plumbing; the live tests cover
the actual data-loss regression and the run-row bookkeeping around the raise.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import energydb as edb
import polars as pl
import pytest
from energydb import Client
from energydb._io import _check_unchanged_scope
from energydb.errors import UnchangedScopeError, ValidationError
from energydb.paths import ResolveSummary

# ``missing`` is the on_missing="skip" report; irrelevant to scope selection, so
# these summaries carry the empty frame a fully-resolved manifest produces.
NOTHING_MISSING = pl.DataFrame(schema={"path": pl.Utf8, "data_type": pl.Utf8, "name": pl.Utf8})
FLAT_ONLY = ResolveSummary(overlapping_series_ids=frozenset(), missing=NOTHING_MISSING)
MIXED = ResolveSummary(overlapping_series_ids=frozenset({7, 9}), missing=NOTHING_MISSING)


# ---------------------------------------------------------------------------
# The guard (no DB needed)
# ---------------------------------------------------------------------------


def test_explicit_valid_time_over_overlapping_series_raises():
    with pytest.raises(UnchangedScopeError) as excinfo:
        _check_unchanged_scope(MIXED, skip_unchanged=True, unchanged_scope="valid_time")

    err = excinfo.value
    assert err.overlapping_series_ids == [7, 9]  # sorted, from the raise site's frozenset
    # The message must name the count and both ways out — the caller has to be
    # able to act on it without reading the source.
    assert "2 OVERLAPPING series" in str(err)
    assert '"auto"' in str(err) and '"knowledge_time"' in str(err)


def test_the_error_is_part_of_the_taxonomy():
    err = UnchangedScopeError("boom")
    assert isinstance(err, ValidationError)
    assert isinstance(err, edb.EnergyDBError)
    assert isinstance(err, ValueError)  # the backwards-compat guarantee
    assert err.overlapping_series_ids is None
    assert edb.UnchangedScopeError is UnchangedScopeError


@pytest.mark.parametrize("scope", ["auto", "knowledge_time"])
def test_the_safe_scopes_never_raise(scope):
    _check_unchanged_scope(MIXED, skip_unchanged=True, unchanged_scope=scope)


def test_valid_time_is_fine_for_a_flat_only_manifest():
    """The overwhelmingly common case: no OVERLAPPING series, nothing to lose."""
    _check_unchanged_scope(FLAT_ONLY, skip_unchanged=True, unchanged_scope="valid_time")


def test_scope_is_not_policed_when_skip_unchanged_is_off():
    """No comparison happens, so no key can drop anything — don't invent an error."""
    _check_unchanged_scope(MIXED, skip_unchanged=False, unchanged_scope="valid_time")


# ---------------------------------------------------------------------------
# Live: the data-loss regression and run-row bookkeeping
# ---------------------------------------------------------------------------

pytestmark_live = pytest.mark.skipif(
    not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")),
    reason="TIMEDB_PG_DSN / TIMEDB_CH_URL not set",
)

BASE_VT = datetime(2026, 5, 1, tzinfo=UTC)
KT_1 = BASE_VT - timedelta(hours=2)
KT_2 = BASE_VT - timedelta(hours=1)


@pytest.fixture
def mixed_client():
    """One FLAT series and one OVERLAPPING series on the same node."""
    client = Client()
    client.delete()
    client.create()
    client.register_tree(edb.Portfolio(name="P", members=[edb.wind.WindTurbine(name="T1", capacity=3.0)]))
    node = client.get_node("P", "T1")
    node.register_series(
        name="power", canonical_unit="MW", data_type="actual", timeseries_type="FLAT", retention="forever"
    )
    node.register_series(
        name="power", canonical_unit="MW", data_type="forecast", timeseries_type="OVERLAPPING", retention="medium"
    )
    yield client
    client.delete()
    client.close()


def _mixed_manifest() -> pl.DataFrame:
    """Two rows for the FLAT series, two for the OVERLAPPING one, same values."""
    vts = [BASE_VT, BASE_VT + timedelta(hours=1)]
    return pl.DataFrame(
        {
            "path": ["P/T1"] * 4,
            "data_type": ["actual", "actual", "forecast", "forecast"],
            "name": ["power"] * 4,
            "valid_time": vts * 2,
            "value": [1.0, 2.0, 1.0, 2.0],
        }
    )


def _run_ids(client: Client) -> set[int]:
    rows = client.td._ch.query("SELECT DISTINCT run_id FROM run_series").result_rows
    return {int(r[0]) for r in rows}


def _pg_run_count(client: Client) -> int:
    async def _count():
        async with client._async._pool.connection() as conn:
            row = await (await conn.execute("SELECT count(*) FROM runs")).fetchone()
            return int(row[0])

    return client._portal.run(_count())


@pytestmark_live
def test_overlapping_republication_survives_the_default_scope(mixed_client):
    """**The regression this plan exists for.**

    Both series are re-sent with identical values at a new knowledge_time. Under
    the old uniform ``valid_time`` key every row looked like a duplicate and the
    forecast republication was dropped — unrecoverable information loss. With
    ``"auto"`` the FLAT rows are still skipped while the OVERLAPPING rows are
    written, and the new vintage is readable at its own knowledge time.
    """
    manifest = _mixed_manifest()
    mixed_client.write(manifest, knowledge_time=KT_1)

    res = mixed_client.write(manifest, knowledge_time=KT_2, skip_unchanged=True)
    assert (res.written, res.skipped) == (2, 2)  # forecast kept, actual dropped

    forecast = pl.DataFrame({"path": ["P/T1"], "data_type": ["forecast"], "name": ["power"]})
    history = mixed_client.read(forecast, include_updates=True, include_knowledge_time=True)
    assert set(history["knowledge_time"].to_list()) == {KT_1, KT_2}

    # The FLAT series really was deduped — one vintage only.
    actual = pl.DataFrame({"path": ["P/T1"], "data_type": ["actual"], "name": ["power"]})
    flat_history = mixed_client.read(actual, include_updates=True, include_knowledge_time=True)
    assert set(flat_history["knowledge_time"].to_list()) == {KT_1}


@pytestmark_live
def test_explicit_valid_time_raises_and_writes_nothing(mixed_client):
    """The kt-known path: the folded runs upsert has already committed by the time
    the scope error is knowable, so it must be compensated — no orphan run row,
    and nothing reaches ClickHouse."""
    manifest = _mixed_manifest()
    mixed_client.write(manifest, knowledge_time=KT_1)
    runs_before, ch_runs_before = _pg_run_count(mixed_client), _run_ids(mixed_client)

    with pytest.raises(UnchangedScopeError, match="would silently drop republications"):
        mixed_client.write(manifest, knowledge_time=KT_2, skip_unchanged=True, unchanged_scope="valid_time")

    assert _pg_run_count(mixed_client) == runs_before  # no orphan runs row
    assert _run_ids(mixed_client) == ch_runs_before  # nothing written to CH


@pytestmark_live
def test_no_orphan_run_row_on_the_knowledge_time_unknown_path(mixed_client):
    """Same no-orphan property on the transactional path. Here the raise is the
    knowledge_time-required error (OVERLAPPING series with no kt), which precedes
    the commit — so the run row rolls back rather than needing compensation."""
    manifest = _mixed_manifest()
    runs_before = _pg_run_count(mixed_client)

    with pytest.raises(ValidationError, match="knowledge_time is required for OVERLAPPING"):
        mixed_client.write(manifest, skip_unchanged=True, unchanged_scope="valid_time")

    assert _pg_run_count(mixed_client) == runs_before


@pytestmark_live
def test_explicit_knowledge_time_scope_is_accepted(mixed_client):
    """The uniform override stays available: every series compared per
    ``(valid_time, knowledge_time)``, so an identical re-send at the *same* kt is
    skipped and a new kt is written."""
    manifest = _mixed_manifest()
    mixed_client.write(manifest, knowledge_time=KT_1)

    same_kt = mixed_client.write(manifest, knowledge_time=KT_1, skip_unchanged=True, unchanged_scope="knowledge_time")
    assert (same_kt.written, same_kt.skipped) == (0, 4)

    new_kt = mixed_client.write(manifest, knowledge_time=KT_2, skip_unchanged=True, unchanged_scope="knowledge_time")
    assert (new_kt.written, new_kt.skipped) == (4, 0)


@pytestmark_live
def test_flat_only_manifest_is_unaffected_by_the_new_default(mixed_client):
    """Backwards compatibility: for a FLAT-only manifest ``"auto"`` and
    ``"valid_time"`` agree exactly — which is every correct caller today."""
    flat = _mixed_manifest().filter(pl.col("data_type") == "actual")
    mixed_client.write(flat, knowledge_time=KT_1)

    auto = mixed_client.write(flat, knowledge_time=KT_2, skip_unchanged=True)
    assert (auto.written, auto.skipped) == (0, 2)

    explicit = mixed_client.write(flat, knowledge_time=KT_2, skip_unchanged=True, unchanged_scope="valid_time")
    assert (explicit.written, explicit.skipped) == (0, 2)
