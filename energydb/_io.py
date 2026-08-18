"""Manifest I/O pipeline — bulk timeseries read and write.

A *manifest* is a polars DataFrame with a routing column (``node_uuid``,
``edge_uuid``, or ``path``), ``data_type``, ``name``, and (for writes) the
data columns (``valid_time``, ``value``, optional ``knowledge_time``,
optional ``unit``). Both ``client.read``/``client.write`` and the scope
single-series helpers route through here, so there is exactly one
read pipeline and one write pipeline in the library.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, NamedTuple

import polars as pl
import psycopg
from timedb import PgEngineMeta, UnchangedScope, profiling

if TYPE_CHECKING:
    import pandas as pd

from energydb import runs as runs_mod
from energydb._ch_meta_engine import CH_ENGINE_TABLE
from energydb._join import (
    EdgeSeriesKey,
    SeriesKey,
    attach_edge_hierarchy,
    attach_node_hierarchy,
    partition_edge_by_path,
    partition_node_by_path,
)
from energydb._persist import apply_manifest_unit_conversion
from energydb.errors import UnchangedScopeError, ValidationError
from energydb.models import SCHEMA
from energydb.models import SQL_SCHEMA_PREFIX as P
from energydb.paths import OnMissing, ResolveSummary, _check_on_missing, resolve_manifest
from energydb.units import compute_unit_factor

# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


_ROUTING_AND_META_COLS = (
    "node_uuid",
    "edge_uuid",
    "path",
    "from_path",
    "to_path",
    "edge_type",
    "data_type",
    "name",
    "canonical_unit",
    "unit",
)

# Edge-triple routing columns; their joint presence marks an edge-routed manifest
# even before ``edge_uuid`` is attached during resolution.
_EDGE_TRIPLE_COLS = ("from_path", "to_path", "edge_type")


class WriteResult(int):
    """The ``run_id`` (an ``int``) carrying row counts from a write.

    Subclasses ``int`` so existing callers that treat the return value as a
    run_id keep working unchanged; ``written`` / ``skipped`` ride along as
    attributes, and ``.run_id`` reads as the int value.
    """

    written: int
    skipped: int

    def __new__(cls, run_id: int, written: int, skipped: int) -> WriteResult:
        self = super().__new__(cls, run_id)
        self.written = written
        self.skipped = skipped
        return self

    @property
    def run_id(self) -> int:
        """The run id for this write — the same value as ``int(result)``."""
        return int(self)

    def __repr__(self) -> str:
        return f"WriteResult(run_id={int(self)}, written={self.written}, skipped={self.skipped})"


class ReadResult(NamedTuple):
    """A read's data plus the manifest triples that resolved to no series.

    Returned by :meth:`~energydb.AsyncClient.read` /
    :meth:`~energydb.AsyncClient.read_relative` **only** when
    ``on_missing="skip"`` — the default (``"raise"``) returns ``data`` bare, so
    existing callers never see this type. Opting in is new code by definition,
    which is what makes the changed return shape safe.

    ``data`` is exactly what the same call would return without ``on_missing``
    (honouring ``output`` and ``backend``, including the empty shapes).
    ``missing`` holds the unique unresolvable triples — the manifest's routing
    column(s) plus ``data_type`` / ``name``, ``Utf8`` throughout (uuids
    stringified), zero-row with the right schema when everything resolved. It
    follows ``backend`` like ``data`` does.

    A :class:`~typing.NamedTuple`, so ``data, missing = await client.read(...)``
    unpacks — mirroring :class:`WriteResult`'s enriched-but-simple shape.
    """

    data: (
        pl.DataFrame
        | pd.DataFrame
        | dict[SeriesKey, pl.DataFrame]
        | dict[SeriesKey, pd.DataFrame]
        | dict[EdgeSeriesKey, pl.DataFrame]
        | dict[EdgeSeriesKey, pd.DataFrame]
    )
    missing: pl.DataFrame | pd.DataFrame


def _check_unchanged_scope(summary: ResolveSummary, *, skip_unchanged: bool, unchanged_scope: UnchangedScope) -> None:
    """Reject a uniform ``valid_time`` comparison over OVERLAPPING series.

    That key ignores ``knowledge_time``, so a republication whose values match
    the previous vintage would be dropped — unrecoverable for a forecast, which
    is exactly why the series is OVERLAPPING. The caller asked for it
    explicitly (the default is ``"auto"``), so raising beats silently losing
    data. No-op unless ``skip_unchanged`` is actually on.
    """
    if not skip_unchanged or unchanged_scope != "valid_time":
        return
    ids = summary.overlapping_series_ids
    if not ids:
        return
    raise UnchangedScopeError(
        f'skip_unchanged with unchanged_scope="valid_time" would silently drop republications '
        f"for {len(ids)} OVERLAPPING series in this manifest. "
        f'Use unchanged_scope="auto" (per-series, recommended) or "knowledge_time" (uniform).',
        overlapping_series_ids=ids,
    )


async def write_manifest(
    client,
    td,
    df: pl.DataFrame,
    *,
    knowledge_time: datetime | None = None,
    run_id: int | None = None,
    workflow_id: str | None = None,
    model_name: str | None = None,
    run_start_time: datetime | None = None,
    run_finish_time: datetime | None = None,
    run_params: dict | None = None,
    skip_unchanged: bool = False,
    unchanged_scope: UnchangedScope = "auto",
) -> WriteResult:
    """Resolve a manifest's routing → series_id and bulk-write.

    The manifest carries the data columns (``valid_time`` + ``value``)
    alongside the routing columns and ``data_type`` / ``name``. An optional
    ``unit`` column triggers per-row unit conversion to each series's
    canonical unit.

    ``skip_unchanged`` drops rows whose stored value already matches;
    ``unchanged_scope`` picks the comparison key:

    * ``"auto"`` (default) — per series: FLAT series compare per
      ``valid_time``, OVERLAPPING series per ``(valid_time, knowledge_time)``,
      in one call. The resolve already knows each series' type, so the
      OVERLAPPING ids ride along as timedb's ``knowledge_time_scoped_series``.
      For a FLAT-only manifest this is identical to ``"valid_time"``.
    * ``"knowledge_time"`` — that key uniformly, for every series.
    * ``"valid_time"`` — that key uniformly. **Raises**
      :class:`~energydb.errors.UnchangedScopeError` when the manifest contains
      OVERLAPPING series, because ignoring ``knowledge_time`` for them would
      silently drop genuine republications.

    ``unchanged_scope`` is ignored entirely when ``skip_unchanged`` is false.
    The ``runs`` row is upserted regardless, so an all-skipped write still
    records a run (with no ``run_series`` mapping). Returns a :class:`WriteResult`
    — an ``int`` run_id carrying ``written`` / ``skipped`` counts.

    PG round-trips: when ``knowledge_time`` is known (kwarg or manifest column)
    the resolve statement runs on an autocommit connection — psycopg's implicit
    ``BEGIN`` and the explicit ``COMMIT`` (one round-trip each) both disappear,
    leaving a single round-trip on the path route. A client-side resolve
    failure then compensates the already-committed folded runs row (auto-generated
    run_ids only; an explicit ``run_id`` may reference earlier successful
    batches, so its upserted row is left in place). Without ``knowledge_time``
    the transactional path is kept, because the OVERLAPPING check below must
    be able to roll back the run row.
    """
    rid = run_id if run_id is not None else runs_mod.generate_run_id()
    run = runs_mod.RunRow(
        run_id=rid,
        workflow_id=workflow_id,
        model_name=model_name,
        run_start_time=run_start_time or datetime.now(UTC),
        run_finish_time=run_finish_time,
        run_params=run_params,
    )
    kt_known = knowledge_time is not None or "knowledge_time" in df.columns

    # Raw checkout on purpose (not client._conn()): the fast path below
    # flips autocommit, which psycopg refuses inside a transaction — and a
    # namespaced _conn() opens one for its SET LOCAL. Namespace binding is
    # therefore per-branch: session-level around the autocommit resolve
    # (cleared in the finally), transaction-local on the classic branch.
    # Without it, RLS would hide the view's series from the resolve.
    async with annotate_undefined_table(), client._pool.connection() as conn:
        if kt_known:
            # The knowledge_time-required check cannot fire here, so no rollback is
            # needed for it. The unchanged_scope check *can*, and it runs inside the
            # try below where the orphan-run compensation covers it.
            await conn.set_autocommit(True)
            if client._namespace is not None:
                await conn.execute(
                    "SELECT set_config('energydb.namespace', %s, false)",
                    (client._namespace,),
                )
            try:
                with profiling._phase(profiling.PHASE_EDB_RESOLVE):
                    try:
                        # Writes drop path / edge meta before the CH insert anyway, so
                        # skip the hierarchy JOIN. ``run`` folds the runs upsert into
                        # the resolve statement on every route (one round-trip).
                        resolved, summary = await resolve_manifest(conn, df, attach_path=False, run=run)
                        # Only answerable after resolve, and on this path the folded
                        # runs upsert has already committed — so it must raise inside
                        # this block to get the same orphan-row compensation.
                        _check_unchanged_scope(summary, skip_unchanged=skip_unchanged, unchanged_scope=unchanged_scope)
                    except Exception:
                        # The folded runs-upsert CTE committed with the statement;
                        # don't leave the orphan row behind a client-side resolve
                        # failure.
                        if run_id is None:
                            with contextlib.suppress(Exception):
                                await conn.execute(f"DELETE FROM {P}runs WHERE run_id = %s", (rid,))
                        raise
            finally:
                if client._namespace is not None:
                    await conn.execute("SELECT set_config('energydb.namespace', '', false)")
                await conn.set_autocommit(False)
        else:
            if client._namespace is not None:
                await conn.execute(
                    "SELECT set_config('energydb.namespace', %s, true)",
                    (client._namespace,),
                )
            with profiling._phase(profiling.PHASE_EDB_RESOLVE):
                resolved, summary = await resolve_manifest(conn, df, attach_path=False, run=run)

            # OVERLAPPING contract: knowledge_time must be supplied (kwarg or column).
            # Raising here (before commit) rolls back the folded run upsert too — a bad
            # call records no run, same as before the fold.
            if summary.has_overlapping:
                raise ValidationError(
                    "knowledge_time is required for OVERLAPPING series; "
                    "pass knowledge_time as a kwarg or as a 'knowledge_time' column on the manifest."
                )
            # No _check_unchanged_scope here: it can only fire when the manifest has
            # OVERLAPPING series, which the check above already rejects on this path.
            # Either way the raise precedes the commit, so the run row rolls back.

            with profiling._phase(profiling.PHASE_EDB_COMMIT):
                await conn.commit()

    if "unit" in resolved.columns:
        with profiling._phase(profiling.PHASE_EDB_UNIT_CONVERT):
            resolved = apply_manifest_unit_conversion(resolved)

    with profiling._phase(profiling.PHASE_EDB_MANIFEST_BUILD):
        keep = [c for c in resolved.columns if c not in _ROUTING_AND_META_COLS]
        write_df = resolved.select(keep).with_columns(pl.lit(rid, dtype=pl.Int64).alias("run_id"))

    # PG state is committed; CH write happens after. A CH failure leaves an
    # orphaned runs row but no PG inconsistency — detectable by run_id. TimeDB is
    # synchronous (clickhouse-connect), so offload the CH leg to a worker thread
    # to keep the event loop free.
    # timedb needs the id set only for the per-series ("auto") comparison, and
    # rejects it alongside any other scope — pass it exactly when it applies.
    kt_scoped = summary.overlapping_series_ids if (skip_unchanged and unchanged_scope == "auto") else None
    counts = await asyncio.to_thread(
        td.write,
        write_df,
        knowledge_time=knowledge_time,
        skip_unchanged=skip_unchanged,
        unchanged_scope=unchanged_scope,
        knowledge_time_scoped_series=kt_scoped,
    )
    return WriteResult(rid, counts.written, counts.skipped)


# ---------------------------------------------------------------------------
# Schema diagnostics
# ---------------------------------------------------------------------------

# energydb's own relations: the four tables plus the series_meta view. Scoping the
# annotation to these keeps it off a host application's own missing tables, which
# have nothing to do with ENERGYDB_SCHEMA.
_ENERGYDB_RELATIONS = frozenset({"node", "edge", "series", "runs", "series_meta"})

_RELATION_RE = re.compile(r'relation "([^"]+)" does not exist')


def _relation_name(exc: psycopg.errors.UndefinedTable) -> str | None:
    """The unqualified relation name from an ``UndefinedTable``, or ``None``.

    PostgreSQL reports ``relation "x" does not exist`` (schema-qualified as
    ``"s.x"`` when the statement qualified it). Returns ``None`` on anything
    unexpected: this runs inside an exception handler, so it must never raise an
    error of its own on top of the one being reported.
    """
    diag = getattr(exc, "diag", None)
    message = getattr(diag, "message_primary", None) or str(exc)
    match = _RELATION_RE.search(message)
    if match is None:
        return None
    return match.group(1).rsplit(".", 1)[-1]


@contextlib.asynccontextmanager
async def annotate_undefined_table():
    """Attach an actionable note to ``UndefinedTable`` for energydb's own relations.

    A client pointed at a schema without the energydb tables (wrong
    ``ENERGYDB_SCHEMA``, or ``create()`` never ran) used to fail with a bare
    ``relation "node" does not exist`` from inside whatever query happened to run
    first — no mention of which schema was searched, that a schema knob exists, or
    what to do.

    Annotates rather than wraps (:pep:`678`): the exception stays an
    ``UndefinedTable``, so anything downstream catching psycopg errors is
    unaffected and there is no new class to document — every traceback just gains
    the missing context. The sync facade propagates the object as-is, notes
    included, so it needs no changes.
    """
    try:
        yield
    except psycopg.errors.UndefinedTable as exc:
        if _relation_name(exc) in _ENERGYDB_RELATIONS:
            exc.add_note(
                f"energydb: the configured schema {(SCHEMA or 'public')!r} "
                f"(ENERGYDB_SCHEMA={os.environ.get('ENERGYDB_SCHEMA', '<unset>')!r}) "
                "does not contain the energydb tables. Either run "
                "'await client.create()' once to provision them, or point "
                "ENERGYDB_SCHEMA at the schema that has them."
            )
        raise


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


@contextlib.asynccontextmanager
async def autocommit_read_conn(pool):
    """A pooled connection in autocommit mode, for read-only resolves.

    psycopg opens the implicit transaction with a separate ``BEGIN`` command
    (one round-trip) and the pool rolls it back when the connection returns
    (another). Read resolves are pure SELECTs under READ COMMITTED — each
    statement gets its own snapshot with or without the wrapping transaction —
    so autocommit drops both round-trips without changing visibility.
    Autocommit is switched back off before the connection returns to the pool.

    Also the annotation point for every read: a schema-misconfiguration failure
    surfaces here rather than from inside an arbitrary query.
    """
    async with annotate_undefined_table(), pool.connection() as conn:
        await conn.set_autocommit(True)
        try:
            yield conn
        finally:
            await conn.set_autocommit(False)


async def _execute_read(
    pool,
    meta: pl.DataFrame,
    td_call: Callable[[list[int], list[str]], pl.DataFrame],
    *,
    unit: str | None,
    output: str,
) -> pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame]:
    """Execute a read given fully-resolved per-series ``meta``.

    ``meta`` must carry ``series_id``, ``retention``, ``canonical_unit``,
    ``data_type``, ``name``, plus exactly one of ``node_uuid`` / ``edge_uuid``
    — one row per series. The hierarchy-attach step is purely polars-side:
    paths already ride on ``meta`` from :func:`resolve_manifest`
    (``attach_path=True``), so no second PG round-trip is needed.
    """
    if output not in {"frame", "by_path"}:
        raise ValidationError(f"output must be 'frame' or 'by_path', got {output!r}")
    series_ids = meta["series_id"].unique().to_list()
    retentions = meta["retention"].unique().to_list()
    # TimeDB (clickhouse-connect) is synchronous — offload the CH read so the
    # event loop stays free.
    result = await asyncio.to_thread(td_call, series_ids, retentions)
    return _finish_read(pool, result, meta, unit=unit, output=output)


def _finish_read(
    pool,
    result: pl.DataFrame,
    meta: pl.DataFrame,
    *,
    unit: str | None,
    output: str,
) -> pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame]:
    """Unit-convert then attach/partition the labelled hierarchy onto a CH value frame.

    The shared tail of every read: joins ``meta`` (path / data_type / name / canonical_unit)
    onto ``result`` by ``series_id``. Pure polars — no DB round-trip. Both the sequential and
    the engine-parallel branch of :func:`execute_read` converge here.
    """
    is_edge = "edge_uuid" in meta.columns

    if unit is not None and not result.is_empty():
        with profiling._phase(profiling.PHASE_EDB_UNIT_CONVERT):
            result = apply_per_series_unit(result, meta, unit)

    if output == "by_path":
        with profiling._phase(profiling.PHASE_EDB_HIERARCHY_JOIN):
            if is_edge:
                return partition_edge_by_path(pool, result, meta)
            return partition_node_by_path(pool, result, meta)

    if result.is_empty():
        # CH returned no rows — drop the internal series_id and bail. The
        # public column shape is incomplete on empty (no path / data_type /
        # name) but callers typically branch on is_empty() first.
        return result.drop("series_id") if "series_id" in result.columns else result

    with profiling._phase(profiling.PHASE_EDB_HIERARCHY_JOIN):
        if is_edge:
            return attach_edge_hierarchy(pool, result, meta)
        return attach_node_hierarchy(pool, result, meta)


logger = logging.getLogger(__name__)

# Strict mode: an engine-read failure raises instead of degrading to the sequential
# path -- for tests/debugging, so a broken engine is loud rather than masked.
_ENGINE_STRICT = os.environ.get("ENERGYDB_ENGINE_STRICT") == "1"


class _EngineReadError(RuntimeError):
    """Wraps a failure of the engine-backed CH value read.

    Lets :func:`execute_read` distinguish engine trouble (degrade the session +
    fall back to the sequential path) from resolve/user errors raised by the
    parallel PG leg (propagate unchanged -- e.g. unregistered series).
    """


# ClickHouse's UNKNOWN_TABLE (error code 60). clickhouse-connect exposes no
# structured error code -- server errors arrive as a DatabaseError whose message
# embeds the server text -- so the match is on tokens from that text. The name is
# the primary marker; the code is a fallback for a server locale/format that
# omits it.
_UNKNOWN_TABLE_MARKERS = ("UNKNOWN_TABLE", "Code: 60.")


def _is_unknown_table(exc: BaseException | None) -> bool:
    """True when an engine-read failure is "the engine table does not exist".

    That is a *configuration state*, not an anomaly: any deployment that never
    provisions the engine table sits in it permanently, and a traceback teaches
    those operators to silence the log with ``ENERGYDB_DISABLE_ENGINE=1`` --
    which then hides real engine failures too. Everything else (network, auth,
    schema drift, CH-side PG connectivity) stays loud.

    Walks the ``__cause__`` / ``__context__`` chain, because the driver may have
    re-wrapped the server error before it reached us. Depth-bounded so a
    self-referential chain can't spin.
    """
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        text = str(exc)
        if any(marker in text for marker in _UNKNOWN_TABLE_MARKERS):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


def _td_call(
    td,
    *,
    relative: bool,
    kwargs: dict,
    meta_source: PgEngineMeta | None = None,
) -> Callable[[list[int], list[str] | None], pl.DataFrame]:
    """Build the ClickHouse value-read closure for :func:`_execute_read`.

    One factory instead of a copy of this closure per read entry point.
    ``relative`` picks ``td.read_relative`` over ``td.read``; ``kwargs`` carry
    the read's bitemporal/window arguments; ``meta_source`` (the concurrent
    path) makes the CH query self-resolve its ``series_id`` set via the
    PostgreSQL engine table instead of the id array.
    """
    if relative:

        def _call(series_ids: list[int], retentions: list[str] | None) -> pl.DataFrame:
            return td.read_relative(series_ids=series_ids, retention=retentions, meta_source=meta_source, **kwargs)

    else:

        def _call(series_ids: list[int], retentions: list[str] | None) -> pl.DataFrame:
            return td.read(series_ids=series_ids, retention=retentions, meta_source=meta_source, **kwargs)

    return _call


def _project_meta(resolved: pl.DataFrame, *, is_edge: bool) -> pl.DataFrame:
    """Project a resolved manifest to the canonical per-series meta slice.

    Returns the per-series identity slice deduplicated by ``series_id``.
    Includes the hierarchy paths attached by :func:`resolve_manifest` so
    the post-read attach step needs no extra PG round-trip.

    * Node-routed: ``(series_id, data_type, name, canonical_unit,
      retention, node_uuid, path)``.
    * Edge-routed: ``(series_id, data_type, name, canonical_unit,
      retention, edge_uuid, edge_type, from_path, to_path)``.
    """
    cols = ["series_id", "data_type", "name", "canonical_unit", "retention"]
    if is_edge:
        cols += ["edge_uuid", "edge_type", "from_path", "to_path"]
        if "edge_uuid" not in resolved.columns:
            # edge-triple-routed manifest: edge_uuid is attached during resolve_manifest.
            raise RuntimeError("resolve_manifest did not attach edge_uuid for an edge-routed manifest")
    else:
        cols += ["node_uuid", "path"]
        if "node_uuid" not in resolved.columns:
            # path-routed manifest: node_uuid is attached during resolve_manifest.
            raise RuntimeError("resolve_manifest did not attach node_uuid for a node-routed manifest")
    return resolved.select(cols).unique()


def engine_meta_for_manifest(manifest: pl.DataFrame) -> PgEngineMeta | None:
    """Superset engine predicate for a routing manifest, or ``None`` if inexpressible.

    Built from the manifest's routing column plus the ``data_type`` / ``name``
    value sets — single-column ``IN`` conditions only (guaranteed pushdown
    through the PostgreSQL engine). The resulting series set is the cartesian
    superset of the manifest's triples; :func:`execute_read` trims values
    against the exactly-resolved meta, so correctness never depends on it.

    Returns ``None`` for anything the engine can't cleanly express — missing or
    null-carrying columns, and non-Utf8 ``path`` / edge-triple columns — so the
    read runs sequentially and ``resolve_manifest`` surfaces the proper error.
    The ``node_uuid`` / ``edge_uuid`` routes accept *any* representation of a
    uuid (``Utf8``, an ``Object`` column of :class:`uuid.UUID`, anything
    ``str()``-able) and stay engine-eligible: they are the routes callers reach
    for most, so degrading them to the sequential path would be an invisible
    performance regression.

    Total by construction — never raises. Callers rely on that: a crash here
    would break ``read()`` outright, including for sessions that never take the
    engine path.
    """
    if "data_type" not in manifest.columns or "name" not in manifest.columns:
        return None
    if manifest["data_type"].null_count() or manifest["name"].null_count():
        return None
    # resolve_manifest lowercases data_type before matching; mirror it here.
    dts = tuple(str(v).lower() for v in manifest["data_type"].unique().to_list())
    names = tuple(str(v) for v in manifest["name"].unique().to_list())

    # Edge-triple route: all three columns present, Utf8, no nulls. Pushed down
    # as three single-column INs over the (superset) unique triples.
    if all(c in manifest.columns for c in _EDGE_TRIPLE_COLS):
        cols = [manifest[c] for c in _EDGE_TRIPLE_COLS]
        if any(c.null_count() or c.dtype != pl.Utf8 for c in cols):
            return None
        triples = tuple(
            (str(fp), str(tp), str(et)) for fp, tp, et in manifest.select(_EDGE_TRIPLE_COLS).unique().iter_rows()
        )
        return PgEngineMeta(table=CH_ENGINE_TABLE, edge_triples=triples, data_type=dts, name=names)

    routes = [c for c in ("path", "node_uuid", "edge_uuid") if c in manifest.columns]
    if len(routes) != 1:
        return None
    route = routes[0]
    col = manifest[route]
    if route == "path":
        # Paths keep their Utf8-only contract: a non-string path is a caller
        # error that resolve_manifest reports properly, so degrade here rather
        # than guess at a stringification.
        if col.null_count() or col.dtype != pl.Utf8:
            return None
        paths = tuple(dict.fromkeys(col.to_list()))
        return PgEngineMeta(table=CH_ENGINE_TABLE, paths=paths, data_type=dts, name=names)

    # uuid routes: normalize to strings *before* deduplicating. ``to_list()``
    # works for every dtype (including the ``Object`` column a manifest of
    # ``uuid.UUID`` produces, which ``unique()`` cannot handle), and
    # ``dict.fromkeys`` dedupes dtype-independently in first-seen order.
    # ``null_count()`` is unreliable on ``Object``, hence the explicit scan.
    raw = col.to_list()
    if any(v is None for v in raw):
        return None
    owners = tuple(dict.fromkeys(str(v) for v in raw))
    if route == "node_uuid":
        return PgEngineMeta(table=CH_ENGINE_TABLE, node_uuids=owners, data_type=dts, name=names)
    return PgEngineMeta(table=CH_ENGINE_TABLE, edge_uuids=owners, data_type=dts, name=names)


async def execute_read(
    pool,
    td,
    client,
    *,
    resolve: Callable[[], Awaitable[pl.DataFrame | None]] | None = None,
    manifest: pl.DataFrame | None = None,
    engine_meta: Callable[[], PgEngineMeta | None] | None = None,
    relative: bool = False,
    unit: str | None = None,
    output: str = "frame",
    start_valid: datetime | None = None,
    end_valid: datetime | None = None,
    start_known: datetime | None = None,
    end_known: datetime | None = None,
    include_updates: bool = False,
    include_knowledge_time: bool = False,
    on_missing: OnMissing = "raise",
    td_kwargs: dict | None = None,
) -> tuple[pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame], int, pl.DataFrame]:
    """The one read pipeline: resolve meta, read values from CH, finish.

    Callers hand in exactly one of:

    * ``resolve`` — a zero-arg factory for the scope's exact PG resolve
      (single round-trip via ``resolve_subtree_series_for_read`` /
      ``resolve_edge_series_for_read``; ``None`` result = nothing matched), or
    * ``manifest`` — a routing manifest (``Client.read`` /
      ``Client.read_relative``), resolved via :func:`resolve_manifest` +
      :func:`_project_meta`.

    ``engine_meta`` is a zero-arg **factory** for the engine predicate, not the
    predicate itself: it is invoked only once this function has confirmed the
    session's engine is available, so an engine-disabled session never pays for
    a value it would discard. A factory rather than a caller-side guard keeps
    the ``_engine_unavailable`` check at the single place that owns it, and
    makes the eager/lazy decision impossible to get wrong at a new call site.

    When the factory yields a predicate, the PG resolve runs **in parallel**
    with a CH value read that self-resolves its ``series_id`` set through the
    PostgreSQL engine table; values are then trimmed against the exact resolve
    (engine predicates may be supersets) and joined client-side —
    byte-identical to the sequential path. On an engine failure the session
    degrades (``client._engine_unavailable``) and the read falls back to the
    sequential path; resolve/user errors propagate unchanged.

    ``on_missing="skip"`` (``manifest=`` only) reads past manifest triples with
    no registered series instead of failing the whole batch; they come back in
    the third return element.

    ``relative`` picks ``td.read_relative`` (window args in ``td_kwargs``) over
    ``td.read`` (explicit bitemporal args). Returns
    ``(result, n_series, missing)``, where ``missing`` is the unresolved-triple
    frame from the resolve — always zero-row under the default ``"raise"``, and
    schema-less for ``resolve=`` callers, which route by subtree rather than by
    a manifest and so have no routing column to report.
    """
    if (resolve is None) == (manifest is None):
        raise ValidationError("execute_read requires exactly one of resolve= or manifest=.")
    if output not in {"frame", "by_path"}:
        raise ValidationError(f"output must be 'frame' or 'by_path', got {output!r}")
    _check_on_missing(on_missing)

    kwargs = (
        dict(td_kwargs or {})
        if relative
        else {
            "start_valid": start_valid,
            "end_valid": end_valid,
            "start_known": start_known,
            "end_known": end_known,
            "include_updates": include_updates,
            "include_knowledge_time": include_knowledge_time,
        }
    )

    # ``missing`` is set by the resolve leg and read by every return path below.
    missing = pl.DataFrame()

    async def _resolve_meta() -> pl.DataFrame | None:
        nonlocal missing
        if resolve is not None:
            return await resolve()
        assert manifest is not None
        is_edge = "edge_uuid" in manifest.columns or all(c in manifest.columns for c in _EDGE_TRIPLE_COLS)
        with profiling._phase(profiling.PHASE_EDB_RESOLVE):
            async with client._read_conn() as conn:
                resolved, summary = await resolve_manifest(conn, manifest, on_missing=on_missing)
        missing = summary.missing
        with profiling._phase(profiling.PHASE_EDB_MANIFEST_BUILD):
            return _project_meta(resolved, is_edge=is_edge)

    def _empty() -> tuple[pl.DataFrame | dict, int, pl.DataFrame]:
        return ({} if output == "by_path" else pl.DataFrame()), 0, missing

    # Build the predicate only if the engine is actually usable this session.
    meta_source = engine_meta() if (engine_meta is not None and not client._engine_unavailable) else None
    if meta_source is not None:
        call = _td_call(td, relative=relative, kwargs=kwargs, meta_source=meta_source)

        def _engine_call() -> pl.DataFrame:
            # series_ids/retention are unused when meta_source resolves them server-side.
            try:
                return call([], None)
            except Exception as exc:
                raise _EngineReadError() from exc

        engine_task = asyncio.create_task(asyncio.to_thread(_engine_call))
        try:
            meta = await _resolve_meta()
        except BaseException:
            # A to_thread CH call is not cancellable; await it so the query
            # thread doesn't outlive the read call that started it.
            with contextlib.suppress(BaseException):
                await engine_task
            raise

        try:
            values = await engine_task
        except _EngineReadError as err:
            # Strict mode is decided before the classification below: a quieter log
            # for one cause must never soften "raise instead of degrading".
            if _ENGINE_STRICT:
                raise (err.__cause__ or err) from None
            client._engine_unavailable = True
            if _is_unknown_table(err.__cause__):
                # Not provisioned. One line, no traceback -- there is no anomaly to
                # investigate, just a feature that was never turned on.
                logger.info(
                    "energydb: ClickHouse meta-engine table %r not provisioned "
                    "(ENERGYDB_SCHEMA=%r) -- using sequential reads for this session. "
                    "Provision it once with `await client.setup_ch_meta_engine()` to "
                    "enable parallel engine reads.",
                    CH_ENGINE_TABLE,
                    os.environ.get("ENERGYDB_SCHEMA", "public"),
                )
            else:
                logger.warning(
                    "energydb: engine-backed read failed for meta-engine table %r "
                    "(ENERGYDB_SCHEMA=%r); falling back to the slower sequential read path "
                    "(higher latency and an extra PostgreSQL round-trip per read) for the rest "
                    "of this session. The usual cause after an upgrade or an ENERGYDB_SCHEMA "
                    "change is a missing or mis-targeted meta-engine table -- (re)provision it "
                    "once with `await client.setup_ch_meta_engine()` (or `create()`).",
                    CH_ENGINE_TABLE,
                    os.environ.get("ENERGYDB_SCHEMA", "public"),
                    exc_info=err.__cause__,
                )
            # The parallel leg already resolved meta exactly -- fall back to the
            # sequential CH read without paying the resolve again.
        else:
            if meta is None or meta.height == 0:
                return _empty()
            # Engine predicates may resolve a superset (set-valued filters); trim to
            # the exact PG resolve before unit conversion / label attach.
            trim = meta.select(pl.col("series_id").cast(pl.UInt64)).unique()
            values = values.join(trim, on="series_id", how="semi")
            return _finish_read(pool, values, meta, unit=unit, output=output), meta.height, missing

        if meta is None or meta.height == 0:
            return _empty()
        call = _td_call(td, relative=relative, kwargs=kwargs)
        return await _execute_read(pool, meta, call, unit=unit, output=output), meta.height, missing

    meta = await _resolve_meta()
    if meta is None or meta.height == 0:
        return _empty()
    call = _td_call(td, relative=relative, kwargs=kwargs)
    return await _execute_read(pool, meta, call, unit=unit, output=output), meta.height, missing


def apply_per_series_unit(
    result: pl.DataFrame,
    meta: pl.DataFrame,
    requested_unit: str,
) -> pl.DataFrame:
    """Multiply value by the per-series canonical→requested factor.

    Single join over (series_id, canonical_unit). Factor computed once per
    unique canonical_unit.
    """
    unique_units = meta["canonical_unit"].unique().to_list()
    factors = {u: (compute_unit_factor(u, requested_unit) or 1.0) for u in unique_units}
    factor_df = pl.DataFrame(
        {
            "canonical_unit": list(factors.keys()),
            "_factor": list(factors.values()),
        },
        schema={"canonical_unit": pl.Utf8, "_factor": pl.Float64},
    )
    series_factor = (
        meta.select(["series_id", "canonical_unit"])
        .unique(subset=["series_id"])
        .join(factor_df, on="canonical_unit", how="left")
        .select(["series_id", "_factor"])
    )
    return (
        result.join(series_factor, on="series_id", how="left")
        .with_columns((pl.col("value") * pl.col("_factor")).alias("value"))
        .drop("_factor")
    )
