"""Synchronous facade over the async-native :class:`AsyncClient`.

The library is async-native: :class:`energydb.client.AsyncClient` is the
single implementation. Synchronous callers (scripts, notebooks, the test
suite) get :class:`Client` — a thin, *generic* facade that runs the async
client's coroutines to completion on a dedicated background event loop and
returns plain results.

Two mechanisms, no per-method code and no codegen:

* **Portal** — a daemon thread running one asyncio event loop for the
  client's whole lifetime. Coroutines are submitted to it with
  :func:`asyncio.run_coroutine_threadsafe` and awaited via ``.result()``,
  which blocks the caller and re-raises any exception. Because the pool is
  opened on this loop and every call runs on it, all connections stay bound
  to a single loop (psycopg's requirement). psycopg constrains the *kind* of
  loop too, not just how many — see :func:`_new_portal_loop`.
* **Reflection proxy** — :func:`_wrap` inspects the wrapped object: coroutine
  methods become blocking calls; sync methods that return fluent scopes /
  transactions get their results wrapped too (so chains like
  ``client.get_node(...).where(...).get()`` work); async context managers
  (``Transaction``) are bridged to the sync ``with`` protocol. Plain results
  (dicts, DataFrames, UUIDs, ...) pass straight through.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import sys
import threading
from typing import Any

from energydb.client import AsyncClient


def _new_portal_loop(platform: str = sys.platform) -> asyncio.AbstractEventLoop:
    """The event loop for the portal thread, chosen to satisfy psycopg.

    psycopg refuses to run in async mode on Windows' default
    ``ProactorEventLoop``: ``AsyncConnection.connect`` raises ``InterfaceError``
    on sight of one, before it ever dials the server. Inside a pool that
    failure is invisible — the background connect worker only *logs* it, so the
    caller blocks for the full timeout and gets a bare ``PoolTimeout`` naming
    nothing but the database it never contacted.

    energydb owns this loop and hard-depends on psycopg, so it picks a
    compatible one rather than inheriting whatever the process defaults to.
    Windows only: everywhere else ``new_event_loop()`` stays, so a caller's
    installed policy (uvloop) still applies. Confining the choice here also
    avoids ``asyncio.set_event_loop_policy``, the process-wide alternative,
    which is deprecated in Python 3.14 and removed in 3.16.

    ``platform`` is a parameter so the branch is testable off Windows, where
    a ``ProactorEventLoop`` cannot even be constructed.
    """
    if platform == "win32":
        return asyncio.SelectorEventLoop()
    return asyncio.new_event_loop()


class _Portal:
    """A daemon thread running one asyncio event loop, forever."""

    def __init__(self) -> None:
        self._loop = _new_portal_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever,
            name="energydb-sync-portal",
            daemon=True,
        )
        self._thread.start()

    def run(self, coro) -> Any:
        """Run ``coro`` on the portal loop, block, and return its result.

        Exceptions raised inside the coroutine propagate to the caller.
        """
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def stop(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


def _has_coro_methods(obj: Any) -> bool:
    """True if ``type(obj)`` defines any coroutine method or async-CM dunder."""
    cls = type(obj)
    if hasattr(cls, "__aenter__"):
        return True
    return any(inspect.iscoroutinefunction(getattr(cls, n, None)) for n in dir(cls))


class _SyncProxy:
    """Synchronous view over an async object — see :func:`_wrap`.

    Coroutine methods are run on the portal and their results re-wrapped;
    sync methods returning further async objects are wrapped too; async
    context managers are bridged to the ``with`` protocol. Proxy *arguments*
    passed back into a method are unwrapped to their underlying objects
    first, so e.g. ``scope.move_to(other_scope)`` reaches the real method
    with a real :class:`NodeScope`, not a proxy.
    """

    def __init__(self, obj: Any, portal: _Portal) -> None:
        object.__setattr__(self, "_obj", obj)
        object.__setattr__(self, "_portal", portal)

    @property
    def __class__(self):
        # Transparent-proxy spoof: ``isinstance(proxy, NodeScope)`` holds for
        # sync callers (scopes look like the real thing), while ``type(proxy)``
        # stays ``_SyncProxy`` so :func:`_unwrap` still detects proxies.
        return type(object.__getattribute__(self, "_obj"))

    def __getattr__(self, name: str) -> Any:
        obj = object.__getattribute__(self, "_obj")
        portal = object.__getattribute__(self, "_portal")
        attr = getattr(obj, name)
        if inspect.iscoroutinefunction(attr):

            @functools.wraps(attr)
            def run_coro(*a, **k):
                a, k = _unwrap_args(a, k)
                return _wrap(portal.run(attr(*a, **k)), portal)

            return run_coro
        if callable(attr) and not inspect.isclass(attr):

            @functools.wraps(attr)
            def call_and_wrap(*a, **k):
                a, k = _unwrap_args(a, k)
                return _wrap(attr(*a, **k), portal)

            return call_and_wrap
        return attr

    # -- async context-manager bridge (e.g. Transaction) --------------
    def __enter__(self) -> Any:
        obj = object.__getattribute__(self, "_obj")
        portal = object.__getattribute__(self, "_portal")
        aenter = getattr(obj, "__aenter__", None)
        if aenter is None:
            raise TypeError(f"{type(obj).__name__!r} is not a context manager")
        return _wrap(portal.run(aenter()), portal)

    def __exit__(self, exc_type, exc, tb) -> Any:
        obj = object.__getattribute__(self, "_obj")
        portal = object.__getattribute__(self, "_portal")
        aexit = getattr(obj, "__aexit__", None)
        if aexit is None:
            raise TypeError(f"{type(obj).__name__!r} is not a context manager")
        return portal.run(aexit(exc_type, exc, tb))

    def __repr__(self) -> str:
        return repr(object.__getattribute__(self, "_obj"))


def _unwrap(x: Any) -> Any:
    """Return the underlying async object if ``x`` is a proxy, else ``x``."""
    if isinstance(x, _SyncProxy):
        return object.__getattribute__(x, "_obj")
    return x


def _unwrap_args(args: tuple, kwargs: dict) -> tuple[tuple, dict]:
    return tuple(_unwrap(a) for a in args), {k: _unwrap(v) for k, v in kwargs.items()}


def _wrap(obj: Any, portal: _Portal) -> Any:
    """Wrap an async object so it is usable from synchronous code.

    Coroutine methods run to completion on ``portal``; sync methods that
    return further async objects have their results wrapped recursively;
    async context managers gain ``__enter__``/``__exit__``. Objects with no
    async surface (the actual data returned by reads) are returned unchanged.
    Idempotent: an already-wrapped object is returned as-is.
    """
    if isinstance(obj, _SyncProxy):
        return obj
    if not _has_coro_methods(obj):
        return obj
    return _SyncProxy(obj, portal)


class Client:
    """Synchronous EnergyDB client — a blocking facade over :class:`AsyncClient`.

    Accepts the same constructor arguments as :class:`AsyncClient`. The
    connection pool is opened eagerly on construction.

    ``client.namespace(ns)`` works here too: the reflection proxy wraps the
    returned :class:`AsyncClient` view, so the result is a sync,
    namespace-bound view sharing this client's pool (and, like the async
    view, refuses lifecycle/schema operations). The
    connection pool is opened eagerly on construction, so the client is ready
    to use immediately. Always call :meth:`close` (or use it as a context
    manager) to release the pool and stop the background loop.

    >>> with Client(pg_conninfo=..., ch_url=...) as client:
    ...     client.create_node(node_type="site", name="S1")
    ...     row = client.get_node(uuid=...).get_raw()
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._portal = _Portal()
        self._async = AsyncClient(*args, **kwargs)
        self._portal.run(self._async.open())
        self._proxy = _wrap(self._async, self._portal)

    def __getattr__(self, name: str) -> Any:
        # Only reached for names not found on the instance/class — forward to
        # the wrapped async client. ``_portal``/``_async``/``_proxy`` are real
        # instance attributes and never hit this path.
        return getattr(self._proxy, name)

    def __repr__(self) -> str:
        return f"Client(pg={self._async._safe_dsn()!r})"

    def __enter__(self) -> Client:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        """Close the connection pool and stop the background event loop."""
        self._portal.run(self._async.close())
        self._portal.stop()
