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
from collections.abc import Callable
from datetime import UTC, datetime

import polars as pl
from timedb import PgEngineMeta, UnchangedScope, profiling

from energydb import runs as runs_mod
from energydb._fast_read import CH_ENGINE_TABLE
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

    async with pool.connection() as conn:
        with profiling._phase(profiling.PHASE_EDB_RESOLVE):
            # Writes drop path / edge meta before the CH insert anyway, so skip the
            # hierarchy JOIN. ``run`` folds the runs upsert into this same
            # query (path route → one round-trip; owner routes → a second statement).
            resolved, summary = await resolve_manifest(conn, df, attach_path=False, run=run)

        # OVERLAPPING contract: knowledge_time must be supplied (kwarg or column).
        # Raising here (before commit) rolls back the folded run upsert too — a bad
        # call records no run, same as before the fold.
        if summary.has_overlapping and knowledge_time is None and "knowledge_time" not in resolved.columns:
            raise ValueError(
                "knowledge_time is required for OVERLAPPING series; "
                "pass knowledge_time as a kwarg or as a 'knowledge_time' column on the manifest."
            )

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
    onto ``result`` by ``series_id``. Pure polars — no DB round-trip. Used by the today path
    (:func:`_execute_read`) and the ``concurrent`` path (:func:`execute_concurrent`).
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


async def execute_concurrent(
    pool,
    td,
    meta_awaitable,
    *,
    root_path: str,
    data_type: str | None,
    name: str | None,
    unit: str | None = None,
    start_valid: datetime | None = None,
    end_valid: datetime | None = None,
    start_known: datetime | None = None,
    end_known: datetime | None = None,
    include_updates: bool = False,
    include_knowledge_time: bool = False,
    output: str = "frame",
) -> tuple[pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame], int]:
    """The ``concurrent`` read: resolve labels in PG and read values from CH in parallel.

    ``meta_awaitable`` is the scope's PG label resolve; it runs concurrently with a CH value
    read whose ``series_id`` set is resolved server-side via the PostgreSQL engine table
    (:data:`CH_ENGINE_TABLE`). Joined on ``series_id`` client-side, so the result is
    byte-identical to today's path. Returns ``(result, n_series)`` (``n_series`` lets the caller
    apply the single-series identity strip).
    """
    ms = PgEngineMeta(table=CH_ENGINE_TABLE, root_path=root_path, data_type=data_type, name=name)
    call = _td_call(
        td,
        relative=False,
        meta_source=ms,
        kwargs={
            "start_valid": start_valid,
            "end_valid": end_valid,
            "start_known": start_known,
            "end_known": end_known,
            "include_updates": include_updates,
            "include_knowledge_time": include_knowledge_time,
        },
    )
    # series_ids/retention are unused when meta_source resolves them server-side.
    meta, result = await asyncio.gather(meta_awaitable, asyncio.to_thread(call, [], None))
    if meta is None or meta.height == 0:
        return ({} if output == "by_path" else pl.DataFrame()), 0
    return _finish_read(pool, result, meta, unit=unit, output=output), meta.height


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


async def execute_read(
    pool,
    td,
    *,
    meta: pl.DataFrame | None = None,
    manifest: pl.DataFrame | None = None,
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
) -> pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame]:
    """The one sequential read pipeline: obtain meta → CH value read → finish.

    Callers hand in exactly one of:

    * ``meta`` — pre-resolved per-series meta (scope reads, which resolve in a
      single PG round-trip via ``resolve_subtree_series_for_read`` /
      ``resolve_edge_series_for_read``), or
    * ``manifest`` — a routing manifest (``Client.read`` /
      ``Client.read_relative``), resolved here via :func:`resolve_manifest` +
      :func:`_project_meta`.

    ``relative=False`` reads values through ``td.read`` with the explicit
    bitemporal args; ``relative=True`` routes through ``td.read_relative`` with
    ``td_kwargs`` (see :meth:`timedb.TimeDBClient.read_relative` for accepted
    window arguments). All paths converge on :func:`_execute_read` →
    :func:`_finish_read`.
    """
    if (meta is None) == (manifest is None):
        raise ValueError("execute_read requires exactly one of meta= or manifest=.")
    if manifest is not None:
        is_edge = "edge_uuid" in manifest.columns
        with profiling._phase(profiling.PHASE_EDB_RESOLVE):
            async with pool.connection() as conn:
                resolved, _summary = await resolve_manifest(conn, manifest)
        with profiling._phase(profiling.PHASE_EDB_MANIFEST_BUILD):
            meta = _project_meta(resolved, is_edge=is_edge)
    assert meta is not None  # narrowed by the exactly-one guard above
    if relative:
        call = _td_call(td, relative=True, kwargs=td_kwargs or {})
    else:
        call = _td_call(
            td,
            relative=False,
            kwargs={
                "start_valid": start_valid,
                "end_valid": end_valid,
                "start_known": start_known,
                "end_known": end_known,
                "include_updates": include_updates,
                "include_knowledge_time": include_knowledge_time,
            },
        )
    return await _execute_read(pool, meta, call, unit=unit, output=output)


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
