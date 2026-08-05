"""Actionable diagnostics for a misconfigured ``ENERGYDB_SCHEMA``.

``ENERGYDB_SCHEMA`` is read at import time and baked into the declarative
models, so making it a per-``Client`` parameter is a real refactor and out of
scope. What *is* in scope: when a client connects to a schema that doesn't
contain the energydb tables (wrong schema, or ``create()`` never ran), the
failure used to surface as a bare ``relation "node" does not exist`` from inside
whatever query ran first — naming neither the schema searched, nor the env var
that controls it, nor the fix.

The exception is now *annotated* (:pep:`678`) rather than wrapped: it stays an
``UndefinedTable``, so nothing catching psycopg errors breaks, and every
traceback gains the context.
"""

from __future__ import annotations

import os

import energydb as edb
import polars as pl
import psycopg
import pytest
from energydb import Client
from energydb._io import _ENERGYDB_RELATIONS, _relation_name


class _Diag:
    def __init__(self, message_primary: str | None) -> None:
        self.message_primary = message_primary


class _FakeUndefinedTable(psycopg.errors.UndefinedTable):
    """``UndefinedTable`` with a synthetic ``diag`` — psycopg builds the real one
    from a server error field we can't fabricate directly."""

    def __init__(self, message_primary: str | None) -> None:
        super().__init__(message_primary or "")
        self._diag = _Diag(message_primary)

    @property
    def diag(self):  # type: ignore[override]
        return self._diag


# ---------------------------------------------------------------------------
# _relation_name (no DB needed)
# ---------------------------------------------------------------------------


def test_the_relation_name_is_extracted():
    assert _relation_name(_FakeUndefinedTable('relation "node" does not exist')) == "node"


def test_a_schema_qualified_name_is_reduced_to_the_relation():
    """PG qualifies the name when the statement did, so ``energydb.node`` has to
    match the same allowlist entry as a bare ``node``."""
    assert _relation_name(_FakeUndefinedTable('relation "energydb.node" does not exist')) == "node"


def test_every_energydb_relation_is_covered():
    """The four tables plus the ``series_meta`` view — the set the annotation is
    scoped to, so a host application's own missing tables stay unannotated."""
    assert set(_ENERGYDB_RELATIONS) == {"node", "edge", "series", "runs", "series_meta"}


def test_an_unparseable_message_yields_none_rather_than_raising():
    """This runs inside an exception handler: it must never raise an error of its
    own on top of the one being reported."""
    assert _relation_name(_FakeUndefinedTable("something else entirely")) is None
    assert _relation_name(_FakeUndefinedTable(None)) is None


# ---------------------------------------------------------------------------
# Live: the note reaches every funnel
# ---------------------------------------------------------------------------

pytestmark_live = pytest.mark.skipif(
    not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")),
    reason="TIMEDB_PG_DSN / TIMEDB_CH_URL not set",
)

_MANIFEST = pl.DataFrame({"path": ["P/T1"], "data_type": ["actual"], "name": ["power"]})


@pytest.fixture
def unprovisioned():
    """A client whose configured schema exists but holds no energydb tables —
    exactly the state a wrong ``ENERGYDB_SCHEMA`` or a missing ``create()`` puts
    you in."""
    c = Client()
    c.delete()  # drops the schema (named) or energydb's own tables (public)
    if edb.client.SCHEMA is not None:
        # ``delete()`` dropped the whole schema; recreate it empty so the failure
        # is "no tables here", not "no schema here".
        c._portal.run(_create_empty_schema(c))
    yield c
    c.close()


async def _create_empty_schema(c: Client) -> None:
    async with c._async._pool.connection() as conn:
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {edb.client.SCHEMA}")
        await conn.commit()


def _assert_actionable(excinfo) -> None:
    notes = getattr(excinfo.value, "__notes__", [])
    assert len(notes) == 1, f"expected exactly one note, got {notes!r}"
    note = notes[0]
    assert repr(edb.client.SCHEMA or "public") in note  # which schema was searched
    assert "ENERGYDB_SCHEMA" in note  # the knob that controls it
    assert "client.create()" in note  # the fix


@pytestmark_live
def test_a_read_explains_the_schema_it_searched(unprovisioned):
    """**The regression this item exists for.** Reads go through
    ``autocommit_read_conn``, the busiest funnel."""
    with pytest.raises(psycopg.errors.UndefinedTable) as excinfo:
        unprovisioned.read(_MANIFEST)

    _assert_actionable(excinfo)


@pytestmark_live
def test_a_write_explains_it_too(unprovisioned):
    """``write_manifest`` is a separate funnel with its own connection handling
    (autocommit + orphan-run compensation), so assert it independently."""
    write_df = _MANIFEST.with_columns(
        pl.lit(None).cast(pl.Datetime("us", "UTC")).alias("valid_time"), pl.lit(1.0).alias("value")
    )
    with pytest.raises(psycopg.errors.UndefinedTable) as excinfo:
        unprovisioned.write(write_df)

    _assert_actionable(excinfo)


@pytestmark_live
def test_a_scope_get_explains_it_too(unprovisioned):
    """The third funnel: ``_use_read_conn`` on the fluent scopes."""
    with pytest.raises(psycopg.errors.UndefinedTable) as excinfo:
        unprovisioned.get_node("P", "T1").get_raw()

    _assert_actionable(excinfo)


@pytestmark_live
def test_the_exception_type_is_unchanged(unprovisioned):
    """Annotating rather than wrapping is the whole compatibility story: anything
    downstream catching psycopg errors must be unaffected."""
    with pytest.raises(psycopg.errors.UndefinedTable) as excinfo:
        unprovisioned.read(_MANIFEST)

    assert type(excinfo.value) is psycopg.errors.UndefinedTable
    # The original message is untouched; the note is additive.
    assert "does not exist" in str(excinfo.value)


@pytestmark_live
def test_the_bootstrap_flow_still_works(unprovisioned):
    """Regression guard for the rejected alternative (a check-at-``open()``
    round-trip): ``open()`` → ``create()`` on a fresh schema must not trip over a
    new startup check, and the annotation must not fire once provisioned."""
    unprovisioned.create()
    unprovisioned.register_tree(edb.Portfolio(name="P", members=[edb.wind.WindTurbine(name="T1", capacity=3.0)]))
    unprovisioned.get_node("P", "T1").register_series(
        name="power", canonical_unit="MW", data_type="actual", timeseries_type="FLAT", retention="forever"
    )

    assert unprovisioned.read(_MANIFEST).is_empty()  # registered, no data — no raise
    unprovisioned.delete()
