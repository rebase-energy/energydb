"""energydb must not care what ``search_path`` is.

Earlier versions pinned *how* energydb set the search path: first a ``SET`` +
commit in the pool's ``configure`` callback, then a libpq startup ``options``
parameter. Both were transports for the same idea: put the schema into
per-connection session state and let unqualified relation names resolve against
it. Behind a transaction-mode pooler that idea does not hold. PgBouncer-based
poolers (Neon's pooled endpoints) reject the ``options`` startup parameter
outright, so every connect fails; and the older ``SET`` is worse than it looks,
because a pooler hands out a possibly-different server connection per
transaction, so the session state a client set is not the state its next
statement runs under.

So the framing inverts. energydb now schema-qualifies every raw-SQL relation
reference (``FROM {SQL_SCHEMA_PREFIX}node``), which makes relation resolution a
property of the SQL text rather than of the connection. ``search_path`` becomes
someone else's business: whatever the server, the role, or a proxy says, it does
not change what energydb reads or writes.

The tests below are that contract. :func:`test_no_relation_reference_is_left_unqualified`
is the regression lock: it scans the package source, so SQL added later is
covered without anyone remembering this file exists.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import energydb
import pytest
from energydb import Client
from energydb._io import _ENERGYDB_RELATIONS
from energydb.models import SCHEMA

# ---------------------------------------------------------------------------
# Static guard: no unqualified relation reference anywhere in the package
# ---------------------------------------------------------------------------

_PACKAGE_DIR = Path(energydb.__file__).parent


def _package_sources() -> list[Path]:
    """Every module in the package. ``rglob`` rather than ``glob`` so a
    subpackage added later is scanned instead of silently exempted."""
    sources = sorted(p for p in _PACKAGE_DIR.rglob("*.py") if "__pycache__" not in p.parts)
    assert sources, f"no energydb sources found under {_PACKAGE_DIR}"
    return sources


# The two spellings of "a schema prefix is interpolated here" that exist in the
# package: {P} (models.SQL_SCHEMA_PREFIX, aliased at every import site)
# and {qualifier} (_ch_meta_engine.series_meta_view_ddl, whose prefix is
# a parameter). A third spelling should fail this test until it is added here;
# that is a deliberate speed bump, not an oversight.
_QUALIFIERS = ("{P}", "{qualifier}")

# The SQL positions where a bare word is a relation name. DELETE FROM is
# covered by the FROM branch; it is spelled out so the intent is readable.
_RELATION_POSITION = r"\b(?:DELETE\s+FROM|FROM|JOIN|INTO|UPDATE)\s+"


def _unqualified_re(relation: str) -> re.Pattern[str]:
    """Match ``relation`` in a relation position, capturing any schema prefix."""
    prefixes = "|".join(re.escape(q) for q in _QUALIFIERS)
    return re.compile(_RELATION_POSITION + f"({prefixes})?" + re.escape(relation) + r"\b")


def _unqualified_hits(text: str, source: str) -> list[str]:
    """``file:line: snippet`` for every relation reference missing its prefix."""
    hits = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        for relation in sorted(_ENERGYDB_RELATIONS):
            for match in _unqualified_re(relation).finditer(line):
                if match.group(1) is None:
                    hits.append(f"{source}:{lineno}: {line.strip()}")
    return hits


def test_the_detector_can_actually_fail():
    """A scanner that matches nothing passes vacuously forever. Pin both
    directions on a snippet of each shape before trusting it on the package."""
    assert _unqualified_hits("SELECT uuid FROM node WHERE x = %s", "<bad>")
    assert _unqualified_hits('"INSERT INTO series (a) VALUES (%s)"', "<bad>")
    assert _unqualified_hits('"DELETE FROM runs WHERE run_id = %s"', "<bad>")
    assert not _unqualified_hits('f"SELECT uuid FROM {P}node WHERE x = %s"', "<good>")
    assert not _unqualified_hits('f"LEFT JOIN {qualifier}edge e ON e.uuid = s.edge_uuid"', "<good>")
    # A column named after a relation is not a relation reference.
    assert not _unqualified_hits('f"SELECT s.node_uuid FROM {P}series s"', "<good>")


def test_no_relation_reference_is_left_unqualified():
    """**The regression lock.** Every ``FROM`` / ``JOIN`` / ``INTO`` / ``UPDATE``
    naming one of energydb's own relations must carry the schema prefix, in every
    module, forever. An unqualified name would silently reintroduce the
    search_path dependency, and would keep working on the author's direct
    PostgreSQL connection while failing behind a pooler."""
    offenders = [hit for path in _package_sources() for hit in _unqualified_hits(path.read_text(), path.name)]

    assert not offenders, "unqualified energydb relation references:\n" + "\n".join(offenders)


def test_the_package_never_mentions_search_path():
    """The failure class, not just its current instances: energydb does not set,
    read, or reason about ``search_path`` at all. Docs, changelog, and this file
    talk about it; the library does not."""
    mentions = [
        f"{path.name}:{lineno}"
        for path in _package_sources()
        for lineno, line in enumerate(path.read_text().splitlines(), start=1)
        if "search_path" in line
    ]

    assert not mentions, f"energydb source mentions search_path at {mentions}"


# ---------------------------------------------------------------------------
# Live: the caller's conninfo is untouched, and a hostile search_path is inert
# ---------------------------------------------------------------------------

pytestmark_live = pytest.mark.skipif(
    not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")),
    reason="TIMEDB_PG_DSN / TIMEDB_CH_URL not set",
)


@pytestmark_live
def test_the_pool_gets_the_callers_conninfo_verbatim():
    """There is no conninfo rewrite: the pool and ``_dsn`` both hold
    exactly what the caller passed. Rewriting it was how the 0.9.0 startup-option
    transport worked, and how it broke every pooled deployment; assert the
    absence so nobody quietly adds a new augmentation."""
    caller_dsn = os.environ["TIMEDB_PG_DSN"]
    c = Client(pg_conninfo=caller_dsn)
    try:
        assert c._async._dsn == caller_dsn
        assert str(c._async._pool.conninfo) == caller_dsn
        # The URI form is what _sqlalchemy_url / engine_table_ddl /
        # _safe_dsn all parse, so it has to survive intact.
        assert c._async._sqlalchemy_url().startswith("postgresql+psycopg://")
        assert "***@" in c._async._safe_dsn()
    finally:
        c.close()


@pytestmark_live
def test_the_connection_is_not_left_in_a_transaction():
    """``configure`` sets a client-side attribute and nothing else. If anything
    there opened a transaction, ``psycopg_pool`` would reject the connection on
    return, so borrow and return one twice."""
    c = Client()
    try:
        for _ in range(2):
            assert c._portal.run(_roundtrip(c)) == 1
    finally:
        c.close()


async def _roundtrip(c: Client) -> int:
    async with c._async._pool.connection() as conn:
        row = await (await conn.execute("SELECT 1")).fetchone()
        assert row is not None
        return row[0]


@pytest.fixture
def hostile_search_path(monkeypatch):
    """Hand out every pooled connection with a ``search_path`` that resolves none
    of energydb's relations.

    Patches the pool *class* energydb constructs rather than an already-built
    pool, so connections created later, including ones the pool grows under
    load, get the hostile setting too. Direct PostgreSQL accepts a
    per-connection ``SET`` fine; the point is not that the setting is exotic but
    that energydb is indifferent to it.
    """
    import energydb.client as client_mod

    base_pool_cls = client_mod.AsyncConnectionPool

    class _HostilePool(base_pool_cls):  # type: ignore[valid-type,misc]
        def __init__(self, *args, configure=None, **kwargs):
            async def _hostile_configure(conn):
                if configure is not None:
                    await configure(conn)
                await conn.execute("SET search_path TO pg_catalog")
                await conn.commit()

            super().__init__(*args, configure=_hostile_configure, **kwargs)

    monkeypatch.setattr(client_mod, "AsyncConnectionPool", _HostilePool)


@pytestmark_live
@pytest.mark.skipif(
    SCHEMA is None,
    reason="the default (public) schema resolves through the server's own search path by design",
)
def test_a_hostile_search_path_changes_nothing(hostile_search_path):
    """**The behaviour the qualification buys.** A full lifecycle (provision,
    structure, series registration, write, read, catalog listing, teardown) on
    connections whose search path contains none of energydb's tables. Any query
    still leaning on session state fails loudly right here.

    Named-schema mode only: with ``ENERGYDB_SCHEMA=public`` energydb emits
    unqualified names *on purpose*, so that they resolve alongside the host
    application's own tables via the server-side default. There is nothing to
    assert there beyond the static guard above.
    """
    import energydatamodel as edm
    import energydb as edb
    import polars as pl
    from energydatamodel import Reference

    c = Client()
    try:
        c.delete()
        c.create()

        bus_a, bus_b = edb.grid.JunctionPoint(name="BusA"), edb.grid.JunctionPoint(name="BusB")
        turbine = edb.wind.WindTurbine(name="T1", capacity=3.0)
        c.register_tree(edb.Portfolio(name="P", members=[turbine, bus_a, bus_b]))

        # Path resolution, subtree navigation, and the raw-node read.
        assert c.get_node("P", "T1").path() == ("P", "T1")
        assert {n["name"] for n in c.get_node("P").children()} == {"T1", "BusA", "BusB"}

        line = edm.Edge(name="L1", from_element=Reference(bus_a), to_element=Reference(bus_b))
        edge_uuid = c.create_edge(line)

        node_uuid = c.get_node("P", "T1").get_raw()["uuid"]
        sid = c.get_node("P", "T1").register_series(
            name="power", canonical_unit="MW", data_type="actual", timeseries_type="FLAT", retention="forever"
        )
        c.get_edge(uuid=edge_uuid).register_series(
            name="flow", canonical_unit="MW", data_type="actual", timeseries_type="FLAT", retention="forever"
        )

        # The catalog read (series) and the run-recording write path
        # (runs + the folded resolve CTE).
        assert [r["series_id"] for r in c.list_series(node_uuid)] == [sid]
        assert [r["name"] for r in c.list_series(edge_uuid, owner_col="edge_uuid")] == ["flow"]

        manifest = pl.DataFrame({"path": ["P/T1"], "data_type": ["actual"], "name": ["power"]})
        c.write(
            manifest.with_columns(
                pl.datetime(2026, 4, 1, time_zone="UTC").alias("valid_time"), pl.lit(1.0).alias("value")
            ),
            workflow_id="hostile-search-path",
        )
        assert c.read(manifest).height == 1

        c.delete()
    finally:
        c.close()


@pytestmark_live
def test_the_full_cycle_still_works():
    """End-to-end smoke on the configured schema, with the pool left alone:
    the same cycle as above minus the hostility, so a failure here separates
    "energydb is broken" from "energydb depends on the search path"."""
    import energydb as edb
    import polars as pl

    c = Client()
    try:
        c.delete()
        c.create()
        c.register_tree(edb.Portfolio(name="P", members=[edb.wind.WindTurbine(name="T1", capacity=3.0)]))
        c.get_node("P", "T1").register_series(
            name="power", canonical_unit="MW", data_type="actual", timeseries_type="FLAT", retention="forever"
        )
        manifest = pl.DataFrame({"path": ["P/T1"], "data_type": ["actual"], "name": ["power"]})
        c.write(
            manifest.with_columns(
                pl.datetime(2026, 4, 1, time_zone="UTC").alias("valid_time"), pl.lit(1.0).alias("value")
            )
        )
        assert c.read(manifest).height == 1
        c.delete()
    finally:
        c.close()
