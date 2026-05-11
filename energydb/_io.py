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

from energydb import runs as runs_mod
from energydb._join import join_edge_hierarchy, join_hierarchy, meta_from_resolved_manifest
from energydb._persist import apply_manifest_unit_conversion
from energydb.paths import resolve_manifest

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
    "timeseries_type",
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
        resolved = resolve_manifest(conn, df)

        # OVERLAPPING contract: knowledge_time must be supplied (kwarg or column).
        overlapping = resolved.filter(pl.col("timeseries_type") == "OVERLAPPING")
        if overlapping.height > 0 and knowledge_time is None and "knowledge_time" not in resolved.columns:
            sample_sid = overlapping["series_id"].to_list()[0]
            raise ValueError(
                f"knowledge_time is required for OVERLAPPING series (series_id={sample_sid}); "
                "pass knowledge_time as a kwarg or as a 'knowledge_time' column on the manifest."
            )

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
        resolved = apply_manifest_unit_conversion(resolved)

    keep = [c for c in resolved.columns if c not in _ROUTING_AND_META_COLS]
    write_df = resolved.select(keep).with_columns(pl.lit(rid, dtype=pl.Int64).alias("run_id"))

    # PG state is committed; CH write happens after. A CH failure leaves an
    # orphaned runs row but no PG inconsistency — detectable by run_id.
    td.write(write_df, knowledge_time=knowledge_time)
    return rid


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


def _read_pipeline(
    pool,
    manifest: pl.DataFrame,
    td_call: Callable[[list[int], list[str]], pl.DataFrame],
    *,
    unit: str | None,
) -> pl.DataFrame:
    """Shared read pipeline: resolve manifest → fetch from timedb → optional
    unit scaling → hierarchy join.

    ``td_call(series_ids, retentions)`` invokes the relevant ``td.read*``
    method with the read-specific kwargs already bound (start_valid,
    relative-window args, etc.).
    """
    is_edge = "edge_uuid" in manifest.columns
    with pool.connection() as conn:
        resolved = resolve_manifest(conn, manifest)
        meta = meta_from_resolved_manifest(resolved, is_edge=is_edge)
        series_ids = meta["series_id"].unique().to_list()
        retentions = meta["retention"].unique().to_list()
        result = td_call(series_ids, retentions)
        if result.is_empty():
            return result
        if unit is not None:
            result = apply_per_series_unit(result, meta, unit)
        if is_edge:
            return join_edge_hierarchy(conn, result, meta)
        return join_hierarchy(conn, result, meta)


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
) -> pl.DataFrame:
    """Bulk read via manifest. Detects edge vs node routing automatically."""

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

    return _read_pipeline(pool, manifest, _call, unit=unit)


def read_relative_manifest(
    pool,
    td,
    manifest: pl.DataFrame,
    *,
    unit: str | None = None,
    **td_kwargs,
) -> pl.DataFrame:
    """Bulk relative read via manifest."""

    def _call(series_ids: list[int], retentions: list[str]) -> pl.DataFrame:
        return td.read_relative(series_ids=series_ids, retention=retentions, **td_kwargs)

    return _read_pipeline(pool, manifest, _call, unit=unit)


def apply_per_series_unit(
    result: pl.DataFrame,
    meta: pl.DataFrame,
    requested_unit: str,
) -> pl.DataFrame:
    """Multiply value by the per-series canonical→requested factor.

    Single join over (series_id, canonical_unit). Factor computed once per
    unique canonical_unit.
    """
    from energydb.units import compute_unit_factor

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
