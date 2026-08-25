"""The sync facade's portal thread must run a loop psycopg accepts.

psycopg refuses async mode on Windows' default ``ProactorEventLoop``, and the
refusal is invisible through a connection pool: the background connect worker
logs it, so the caller only ever sees ``PoolTimeout`` after the full timeout,
naming a database that was never contacted (rebase-energy/platform#116).

No database needed: these tests are about which loop gets constructed.
"""

from __future__ import annotations

import asyncio

import pytest
from energydb._sync import _new_portal_loop


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
