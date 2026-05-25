"""Manifest I/O pipeline — bulk timeseries read and write.

A *manifest* is a polars DataFrame with a routing column (``node_uuid``,
``edge_uuid``, or ``path``), ``data_type``, ``name``, and (for writes) the
data columns (``valid_time``, ``value``, optional ``knowledge_time``,
optional ``unit``). Both ``client.read``/``client.write`` and the scope
single-series helpers route through here, so there is exactly one
read pipeline and one write pipeline in the library.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import polars as pl
from timedb import profiling

from energydb import runs as runs_mod
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


def write_manifest(
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
) -> int:
    """Resolve a manifest's routing → series_id and bulk-write.

    The manifest carries the data columns (``valid_time`` + ``value``)
    alongside the routing columns and ``data_type`` / ``name``. An optional
    ``unit`` column triggers per-row unit conversion to each series's
    canonical unit. Returns the ``run_id`` used.
    """
    rid = run_id if run_id is not None else runs_mod.generate_run_id()

    with pool.connection() as conn:
        with profiling._phase(profiling.PHASE_EDB_RESOLVE):
            # Writes drop path / edge meta before the CH insert anyway, so
            # skip the hierarchy JOIN — saves ~20ms PG-side at scale=200.
            resolved, summary = resolve_manifest(conn, df, attach_path=False)

        # OVERLAPPING contract: knowledge_time must be supplied (kwarg or column).
        if summary.has_overlapping and knowledge_time is None and "knowledge_time" not in resolved.columns:
            raise ValueError(
                "knowledge_time is required for OVERLAPPING series; "
                "pass knowledge_time as a kwarg or as a 'knowledge_time' column on the manifest."
            )

        with profiling._phase(profiling.PHASE_EDB_RUNS_UPSERT):
            runs_mod.upsert_run(
                conn,
                run_id=rid,
                workflow_id=workflow_id,
                model_name=model_name,
                run_start_time=run_start_time or datetime.now(UTC),
                run_finish_time=run_finish_time,
                run_params=run_params,
            )
            conn.commit()

    if "unit" in resolved.columns:
        with profiling._phase(profiling.PHASE_EDB_UNIT_CONVERT):
            resolved = apply_manifest_unit_conversion(resolved)

    with profiling._phase(profiling.PHASE_EDB_MANIFEST_BUILD):
        keep = [c for c in resolved.columns if c not in _ROUTING_AND_META_COLS]
        write_df = resolved.select(keep).with_columns(pl.lit(rid, dtype=pl.Int64).alias("run_id"))

    # PG state is committed; CH write happens after. A CH failure leaves an
    # orphaned runs row but no PG inconsistency — detectable by run_id.
    td.write(write_df, knowledge_time=knowledge_time)
    return rid


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


def _execute_read(
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
    — one row per series. The hierarchy-attach step borrows a PG connection
    from the pool to fetch joined paths.
    """
    if output not in {"frame", "by_path"}:
        raise ValueError(f"output must be 'frame' or 'by_path', got {output!r}")
    is_edge = "edge_uuid" in meta.columns

    series_ids = meta["series_id"].unique().to_list()
    retentions = meta["retention"].unique().to_list()
    result = td_call(series_ids, retentions)

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


def _read_pipeline(
    pool,
    manifest: pl.DataFrame,
    td_call: Callable[[list[int], list[str]], pl.DataFrame],
    *,
    unit: str | None,
    output: str = "frame",
) -> pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame]:
    """Manifest-driven read: resolve then execute. Used by ``Client.read`` and
    ``Client.read_relative`` (which both accept a routing manifest).

    Scope reads bypass this and call :func:`read_resolved` directly with
    meta they already have from the scope's PG resolve.
    """
    is_edge = "edge_uuid" in manifest.columns
    with pool.connection() as conn, profiling._phase(profiling.PHASE_EDB_RESOLVE):
        resolved, _summary = resolve_manifest(conn, manifest)
    with profiling._phase(profiling.PHASE_EDB_MANIFEST_BUILD):
        meta = _meta_from_resolved(resolved, is_edge=is_edge)
    return _execute_read(pool, meta, td_call, unit=unit, output=output)


def read_resolved(
    pool,
    td,
    meta: pl.DataFrame,
    *,
    unit: str | None = None,
    start_valid: datetime | None = None,
    end_valid: datetime | None = None,
    start_known: datetime | None = None,
    end_known: datetime | None = None,
    include_updates: bool = False,
    include_knowledge_time: bool = False,
    output: str = "frame",
) -> pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame]:
    """Read entry-point for callers who already have per-series meta.

    Skips :func:`resolve_manifest` entirely — scope reads compute ``meta``
    via :func:`series.resolve_for_read` (one PG round-trip) and feed it
    straight in, avoiding the manifest hash / unique / lookup-join pass.
    """

    def _call(series_ids: list[int], retentions: list[str]) -> pl.DataFrame:
        return td.read(
            series_ids=series_ids,
            retention=retentions,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            include_updates=include_updates,
            include_knowledge_time=include_knowledge_time,
        )

    return _execute_read(pool, meta, _call, unit=unit, output=output)


def read_relative_resolved(
    pool,
    td,
    meta: pl.DataFrame,
    *,
    unit: str | None = None,
    output: str = "frame",
    **td_kwargs,
) -> pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame]:
    """Like :func:`read_resolved` but routes through ``td.read_relative``."""

    def _call(series_ids: list[int], retentions: list[str]) -> pl.DataFrame:
        return td.read_relative(series_ids=series_ids, retention=retentions, **td_kwargs)

    return _execute_read(pool, meta, _call, unit=unit, output=output)


def _meta_from_resolved(resolved: pl.DataFrame, *, is_edge: bool) -> pl.DataFrame:
    """Project the per-series meta columns the read pipeline needs.

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


def read_manifest(
    pool,
    td,
    manifest: pl.DataFrame,
    *,
    unit: str | None = None,
    start_valid: datetime | None = None,
    end_valid: datetime | None = None,
    start_known: datetime | None = None,
    end_known: datetime | None = None,
    include_updates: bool = False,
    include_knowledge_time: bool = False,
    output: str = "frame",
) -> pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame]:
    """Bulk read via manifest. Detects edge vs node routing automatically.

    ``output="frame"`` returns a single long DataFrame; ``output="by_path"``
    returns a ``dict[(path, data_type, name), DataFrame]`` (or edge
    equivalent). See :meth:`Client.read` for the public contract.
    """

    def _call(series_ids: list[int], retentions: list[str]) -> pl.DataFrame:
        return td.read(
            series_ids=series_ids,
            retention=retentions,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            include_updates=include_updates,
            include_knowledge_time=include_knowledge_time,
        )

    return _read_pipeline(pool, manifest, _call, unit=unit, output=output)


def read_relative_manifest(
    pool,
    td,
    manifest: pl.DataFrame,
    *,
    unit: str | None = None,
    output: str = "frame",
    **td_kwargs,
) -> pl.DataFrame | dict[SeriesKey, pl.DataFrame] | dict[EdgeSeriesKey, pl.DataFrame]:
    """Bulk relative read via manifest.

    ``**td_kwargs`` are forwarded to :meth:`timedb.TimeDBClient.read_relative`;
    see that signature for accepted arguments. See :meth:`Client.read_relative`
    for the public contract of ``output``.
    """

    def _call(series_ids: list[int], retentions: list[str]) -> pl.DataFrame:
        return td.read_relative(series_ids=series_ids, retention=retentions, **td_kwargs)

    return _read_pipeline(pool, manifest, _call, unit=unit, output=output)


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
