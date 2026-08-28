"""The sync facade's portal thread must run a loop psycopg accepts, and must
shut down cleanly: idempotently, without leaking its thread or event loop fd.

psycopg refuses async mode on Windows' default ``ProactorEventLoop``, and the
refusal is invisible through a connection pool: the background connect worker
logs it, so the caller only ever sees ``PoolTimeout`` after the full timeout,
naming a database that was never contacted (rebase-energy/platform#116).

The loop-choice and lifecycle tests below need no database; the double-close
test does (a real pool and ClickHouse client to close) and is marked
accordingly.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading

import pytest
from energydb._sync import Client, _new_portal_loop, _Portal
from energydb.errors import ConfigurationError

needs_db = pytest.mark.skipif(
    not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")),
    reason="TIMEDB_PG_DSN / TIMEDB_CH_URL not set: skipping DB-backed lifecycle tests",
)


def _run_with_timeout(fn, *, timeout: float):
    """Run ``fn()`` on a background thread; fail fast instead of hanging.

    A lifecycle regression (e.g. a second ``close()`` scheduling a coroutine
    on an already-stopped loop) blocks forever on ``Future.result()``; without
    this, a single regressed test would hang the whole suite instead of
    failing.
    """
    outcome: dict = {}

    def target():
        try:
            outcome["result"] = fn()
        except BaseException as exc:  # noqa: BLE001  (re-raised on the caller's thread below)
            outcome["error"] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout=timeout)
    if thread.is_alive():
        pytest.fail(f"{fn!r} did not complete within {timeout}s (hung instead of returning/raising)")
    if "error" in outcome:
        raise outcome["error"]
    return outcome.get("result")


def test_windows_gets_a_selector_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    """The *choice* is what's asserted, not the resulting class.

    On Linux and macOS the default loop already is a selector loop, so
    ``isinstance(loop, asyncio.SelectorEventLoop)`` would pass with or without
    the fix: a test that cannot fail. Standing in sentinels for both
    constructors pins the branch on every platform, including the CI that
    cannot construct a ``ProactorEventLoop`` at all.
    """
    monkeypatch.setattr(asyncio, "SelectorEventLoop", lambda: "selector-loop")
    monkeypatch.setattr(asyncio, "new_event_loop", lambda: "default-loop")

    assert _new_portal_loop("win32") == "selector-loop"


@pytest.mark.parametrize("platform", ["linux", "darwin", "freebsd"])
def test_every_other_platform_keeps_the_default_loop(platform: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Only Windows is special-cased.

    Elsewhere ``new_event_loop()`` stays, so a caller who installed uvloop (or
    any other policy) still gets it; energydb overrides the process default
    only where psycopg leaves it no choice.
    """
    monkeypatch.setattr(asyncio, "SelectorEventLoop", lambda: "selector-loop")
    monkeypatch.setattr(asyncio, "new_event_loop", lambda: "default-loop")

    assert _new_portal_loop(platform) == "default-loop"


def test_the_real_loop_runs_coroutines_on_this_platform() -> None:
    """End to end on whatever platform the suite runs on: the loop the portal
    would build is a working loop, not just the right class."""
    loop = _new_portal_loop()
    try:
        assert isinstance(loop, asyncio.AbstractEventLoop)

        async def _answer() -> int:
            return 42

        assert loop.run_until_complete(_answer()) == 42
    finally:
        loop.close()


class TestPortalStop:
    def test_stop_is_idempotent(self) -> None:
        """A second ``stop()`` used to schedule a coroutine-stop on an
        already-stopped loop, whose ``Future.result()`` then blocked forever."""
        portal = _Portal()
        portal.stop()
        _run_with_timeout(portal.stop, timeout=5)

    def test_loop_is_closed_after_a_clean_stop(self) -> None:
        portal = _Portal()
        portal.stop()
        assert portal._loop.is_closed()

    def test_stop_leaves_the_loop_open_and_warns_if_the_thread_does_not_join(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Closing a loop whose thread is still (apparently) running would
        leak the running loop's fd from underneath it; ``stop()`` must warn
        instead of closing in that case."""
        portal = _Portal()
        monkeypatch.setattr(portal._thread, "join", lambda timeout=None: None)
        monkeypatch.setattr(portal._thread, "is_alive", lambda: True)

        with caplog.at_level(logging.WARNING):
            portal.stop()

        assert not portal._loop.is_closed()
        assert any("did not stop" in record.message for record in caplog.records)


class TestClientGetattrRecursionGuard:
    def test_underscore_names_raise_attribute_error_instead_of_recursing(self) -> None:
        """Before ``_proxy`` is set (e.g. mid-``__init__``, or probed by
        pickling/copy machinery), looking it up via ``__getattr__`` must not
        try to look up ``_proxy`` again to satisfy that same lookup."""
        client = object.__new__(Client)
        with pytest.raises(AttributeError):
            _ = client._proxy
        with pytest.raises(AttributeError):
            _ = client._totally_unset_private_attr


class TestFailedInitLeaksNoThread:
    def test_bad_dsn_leaves_no_live_portal_thread(self) -> None:
        with pytest.raises(ConfigurationError):
            Client(pg_conninfo="host=localhost dbname=devdb")  # key=value DSN: rejected before any I/O

        for thread in threading.enumerate():
            if thread.name == "energydb-sync-portal":
                thread.join(timeout=2)
                assert not thread.is_alive()


@needs_db
class TestDoubleCloseNoHang:
    def test_double_close_does_not_hang(self) -> None:
        client = Client()
        _run_with_timeout(client.close, timeout=10)
        _run_with_timeout(client.close, timeout=10)
