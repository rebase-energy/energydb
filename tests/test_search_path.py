"""``search_path`` as a connection property, not a per-checkout statement.

The pool's ``configure`` callback used to run ``SET search_path TO …`` **and
commit** on every checkout — two client-visible round-trips' worth of latency
per borrowed connection (the commit was required because an uncommitted ``SET``
leaves the connection in a transaction, which ``psycopg_pool`` rejects). libpq's
startup ``options`` makes it a property of the connection instead.

The risk is all in the conninfo composition, and in one specific way: the
augmented form is key=value, while ``self._dsn`` is parsed as a URI by
``_sqlalchemy_url()``, ``engine_table_ddl()`` and ``_safe_dsn()``. Leaking the
augmented form into any of those breaks them, so the unit tests below pin
``_dsn`` as byte-identical to the caller's input.
"""

from __future__ import annotations

import os

import pytest
from energydb import Client
from energydb.client import _pool_conninfo
from energydb.models import SCHEMA

_URI = "postgresql://app_user:s3cret@db.example.com:6543/proddb"


# ---------------------------------------------------------------------------
# Conninfo composition (no DB needed)
# ---------------------------------------------------------------------------


def _options_of(conninfo: str) -> str:
    from psycopg.conninfo import conninfo_to_dict

    return str(conninfo_to_dict(conninfo).get("options") or "")


def test_the_search_path_becomes_a_startup_option():
    expected = f"-c search_path={SCHEMA},public" if SCHEMA else "-c search_path=public"
    assert _options_of(_pool_conninfo(_URI)) == expected


def test_the_default_schema_is_not_doubled():
    """``public`` for both entries would render ``search_path=public,public``,
    which makes ``SHOW search_path`` needlessly confusing. Mirrors the old ``SET``
    exactly: named schema → ``"{schema},public"``, default → ``"public"``."""
    import energydb.client as client_mod

    for schema, expected in ((None, "-c search_path=public"), ("energydb", "-c search_path=energydb,public")):
        original = client_mod.SCHEMA
        client_mod.SCHEMA = schema
        try:
            assert _options_of(_pool_conninfo(_URI)) == expected
        finally:
            client_mod.SCHEMA = original


def test_caller_supplied_options_are_kept():
    """Clobbering a caller's ``options`` would silently drop their PG settings —
    the one composition mistake with real consequences."""
    with_geqo = _URI + "?options=-c%20geqo%3Doff"
    options = _options_of(_pool_conninfo(with_geqo))

    assert "-c geqo=off" in options
    assert "search_path=" in options
    assert options.index("geqo") < options.index("search_path")  # appended, not prepended


def test_connection_details_survive_the_rewrite():
    """``make_conninfo`` emits key=value, so assert the parts round-tripped rather
    than eyeballing the string."""
    from psycopg.conninfo import conninfo_to_dict

    parts = conninfo_to_dict(_pool_conninfo(_URI))

    assert parts["user"] == "app_user"
    assert parts["password"] == "s3cret"
    assert parts["host"] == "db.example.com"
    assert parts["port"] == "6543"
    assert parts["dbname"] == "proddb"


# ---------------------------------------------------------------------------
# Live: the augmented form must not leak, and must actually take effect
# ---------------------------------------------------------------------------

pytestmark_live = pytest.mark.skipif(
    not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")),
    reason="TIMEDB_PG_DSN / TIMEDB_CH_URL not set",
)


@pytestmark_live
def test_the_client_dsn_is_untouched():
    """**The regression guard.** ``_sqlalchemy_url()`` does
    ``self._dsn.split('://')`` and ``engine_table_ddl`` ``urlparse``s it; a
    key=value DSN would break both silently at ``create()`` time."""
    caller_dsn = os.environ["TIMEDB_PG_DSN"]
    c = Client(pg_conninfo=caller_dsn)
    try:
        assert c._async._dsn == caller_dsn
        assert "://" in c._async._dsn
        assert c._async._sqlalchemy_url().startswith("postgresql+psycopg://")
        assert "***@" in c._async._safe_dsn()  # credentials still masked
        # ...while the pool got the augmented form.
        assert "search_path=" in str(c._async._pool.conninfo or "")
    finally:
        c.close()


@pytestmark_live
def test_pooled_connections_resolve_the_configured_schema():
    """The behaviour the ``SET`` provided, now from the startup packet. Asserted on
    a *pooled* connection, since that is where the per-checkout statement was.

    Compared as the parsed entry list, not the raw string: ``SHOW search_path``
    renders the startup-option form without the space the ``SET`` form produced
    (``energydb,public`` vs ``energydb, public``). Resolution is identical — only
    the echo differs — and pinning the spelling would be pinning cosmetics.
    """
    expected = [SCHEMA, "public"] if SCHEMA else ["public"]
    c = Client()
    try:
        shown = c._portal.run(_show_search_path(c))
        assert [part.strip() for part in shown.split(",")] == expected
    finally:
        c.close()


async def _show_search_path(c: Client) -> str:
    async with c._async._pool.connection() as conn:
        row = await (await conn.execute("SHOW search_path")).fetchone()
        assert row is not None
        return row[0]


@pytestmark_live
def test_the_connection_is_not_left_in_a_transaction():
    """``configure`` dropped its ``commit()`` along with the ``SET``. If anything
    there opened a transaction, ``psycopg_pool`` would reject the connection on
    return — so borrow and return one twice."""
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


@pytestmark_live
def test_the_full_cycle_still_works():
    """End-to-end smoke on the configured schema: unqualified raw SQL in the
    resolvers depends entirely on the search path being right."""
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
