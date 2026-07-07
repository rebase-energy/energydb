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
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import polars as pl
from timedb import PgEngineMeta, UnchangedScope, profiling

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
from energydb.paths import resolve_manifest
from energydb.units import compute_unit_factor

# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------


_ROUTING_AND_META_COLS = (
    "node_uuid",
    "edge_uuid",
    "path",
    "data_type",
    "name",
    "canonical_unit",
    "unit",
)


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
        return int(self)

    def __repr__(self) -> str:
        return f"WriteResult(run_id={int(self)}, written={self.written}, skipped={self.skipped})"


async def write_manifest(
    pool,
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
    unchanged_scope: UnchangedScope = "valid_time",
) -> WriteResult:
    """Resolve a manifest's routing → series_id and bulk-write.

    The manifest carries the data columns (``valid_time`` + ``value``)
    alongside the routing columns and ``data_type`` / ``name``. An optional
    ``unit`` column triggers per-row unit conversion to each series's
    canonical unit.

    ``skip_unchanged`` (and ``unchanged_scope``) are forwarded to
    :func:`timedb.write`; see that for the comparison semantics. The
    ``runs`` row is upserted regardless, so an all-skipped write still
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

    async with pool.connection() as conn:
        if kt_known:
            # The OVERLAPPING check cannot fail with knowledge_time known, so
            # nothing after the resolve statement needs a rollback.
            await conn.set_autocommit(True)
            try:
                with profiling._phase(profiling.PHASE_EDB_RESOLVE):
                    try:
                        # Writes drop path / edge meta before the CH insert anyway, so
                        # skip the hierarchy JOIN. ``run`` folds the runs upsert into
                        # the resolve statement on every route (one round-trip).
                        resolved, summary = await resolve_manifest(conn, df, attach_path=False, run=run)
                    except Exception:
                        # The folded runs-upsert CTE committed with the statement;
                        # don't leave the orphan row behind a client-side resolve
                        # failure.
                        if run_id is None:
                            with contextlib.suppress(Exception):
                                await conn.execute("DELETE FROM runs WHERE run_id = %s", (rid,))
                        raise
            finally:
                await conn.set_autocommit(False)
        else:
            with profiling._phase(profiling.PHASE_EDB_RESOLVE):
                resolved, summary = await resolve_manifest(conn, df, attach_path=False, run=run)

            # OVERLAPPING contract: knowledge_time must be supplied (kwarg or column).
            # Raising here (before commit) rolls back the folded run upsert too — a bad
            # call records no run, same as before the fold.
            if summary.has_overlapping:
                raise ValueError(
                    "knowledge_time is required for OVERLAPPING series; "
                    "pass knowledge_time as a kwarg or as a 'knowledge_time' column on the manifest."
                )

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
    counts = await asyncio.to_thread(
        td.write,
        write_df,
        knowledge_time=knowledge_time,
        skip_unchanged=skip_unchanged,
        unchanged_scope=unchanged_scope,
    )
    return WriteResult(rid, counts.written, counts.skipped)


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
    """
    async with pool.connection() as conn:
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
        raise ValueError(f"output must be 'frame' or 'by_path', got {output!r}")
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
            return td.read_relative(series_ids=series_ids, retention=retentions, **kwargs)

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

    Returns ``None`` for anything the engine can't cleanly express (missing or
    null-carrying columns, non-Utf8 paths) — the read then runs sequentially
    and ``resolve_manifest`` surfaces the proper error.
    """
    if "data_type" not in manifest.columns or "name" not in manifest.columns:
        return None
    routes = [c for c in ("path", "node_uuid", "edge_uuid") if c in manifest.columns]
    if len(routes) != 1:
        return None
    route = routes[0]
    col = manifest[route]
    if col.null_count() or (route == "path" and col.dtype != pl.Utf8):
        return None
    if manifest["data_type"].null_count() or manifest["name"].null_count():
        return None
    owners = tuple(str(v) for v in col.unique().to_list())
    # resolve_manifest lowercases data_type before matching; mirror it here.
    dts = tuple(str(v).lower() for v in manifest["data_type"].unique().to_list())
    names = tuple(str(v) for v in manifest["name"].unique().to_list())
    if route == "path":
        return PgEngineMeta(table=CH_ENGINE_TABLE, paths=owners, data_type=dts, name=names)
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
    engine_meta: PgEngineMeta | None = None,
    relative: bool = False,
    unit: str | None = None,
    output: str = "frame",
    start_valid: datetime | None = None,
    end_valid: datetime | None = None,
    start_known: datetime | None = None,
    end_known: datetime | None = None,
    include_updates: bool = False,
    include_knowledge_time: bool = False,
    td_kwargs: dict | None = None,
) -> tuple[pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame], int]:
    """The one read pipeline: resolve meta, read values from CH, finish.

    Callers hand in exactly one of:

    * ``resolve`` — a zero-arg factory for the scope's exact PG resolve
      (single round-trip via ``resolve_subtree_series_for_read`` /
      ``resolve_edge_series_for_read``; ``None`` result = nothing matched), or
    * ``manifest`` — a routing manifest (``Client.read`` /
      ``Client.read_relative``), resolved via :func:`resolve_manifest` +
      :func:`_project_meta`.

    When ``engine_meta`` is given and the session's engine is healthy, the PG
    resolve runs **in parallel** with a CH value read that self-resolves its
    ``series_id`` set through the PostgreSQL engine table; values are then
    trimmed against the exact resolve (engine predicates may be supersets) and
    joined client-side — byte-identical to the sequential path. On an engine
    failure the session degrades (``client._engine_unavailable``) and the read
    falls back to the sequential path; resolve/user errors propagate unchanged.

    ``relative`` picks ``td.read_relative`` (window args in ``td_kwargs``) over
    ``td.read`` (explicit bitemporal args). Returns ``(result, n_series)``.
    """
    if (resolve is None) == (manifest is None):
        raise ValueError("execute_read requires exactly one of resolve= or manifest=.")
    if output not in {"frame", "by_path"}:
        raise ValueError(f"output must be 'frame' or 'by_path', got {output!r}")

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

    async def _resolve_meta() -> pl.DataFrame | None:
        if resolve is not None:
            return await resolve()
        assert manifest is not None
        is_edge = "edge_uuid" in manifest.columns
        with profiling._phase(profiling.PHASE_EDB_RESOLVE):
            async with autocommit_read_conn(pool) as conn:
                resolved, _summary = await resolve_manifest(conn, manifest)
        with profiling._phase(profiling.PHASE_EDB_MANIFEST_BUILD):
            return _project_meta(resolved, is_edge=is_edge)

    def _empty() -> tuple[pl.DataFrame | dict, int]:
        return ({} if output == "by_path" else pl.DataFrame()), 0

    if engine_meta is not None and not client._engine_unavailable:
        call = _td_call(td, relative=relative, kwargs=kwargs, meta_source=engine_meta)

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
            # A to_thread CH call is not cancellable and shares the clickhouse-connect
            # session; let it finish before propagating, or the next CH call collides
            # ("concurrent queries within the same session").
            with contextlib.suppress(BaseException):
                await engine_task
            raise

        try:
            values = await engine_task
        except _EngineReadError as err:
            if _ENGINE_STRICT:
                raise (err.__cause__ or err) from None
            client._engine_unavailable = True
            logger.warning(
                "engine-backed read failed; using the sequential path for the rest of the "
                "session (setup_ch_meta_engine() re-enables)",
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
            return _finish_read(pool, values, meta, unit=unit, output=output), meta.height

        if meta is None or meta.height == 0:
            return _empty()
        call = _td_call(td, relative=relative, kwargs=kwargs)
        return await _execute_read(pool, meta, call, unit=unit, output=output), meta.height

    meta = await _resolve_meta()
    if meta is None or meta.height == 0:
        return _empty()
    call = _td_call(td, relative=relative, kwargs=kwargs)
    return await _execute_read(pool, meta, call, unit=unit, output=output), meta.height


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
