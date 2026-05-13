"""Compare SQL idioms for the three "winning-row per (sid, vt)" reads
in ``timedb/timedb/read.py``.

Hypothesis (from analysis):
    The current ``argMax(value, (knowledge_time, change_time)) GROUP BY
    series_id, valid_time`` builds a per-thread hash table holding tuple-
    argMax state for every distinct group, which scales as N_groups × N_threads
    × tuple_state_size. At scale=200 (1.728 M groups), this peaks ~2.3 GB on
    CH and consumes most of the wall time. Replacing with ``ORDER BY ...
    LIMIT 1 BY series_id, valid_time`` along the table's sort-key prefix
    should stream first-row-per-group with O(1) state per active group and
    cut both peak memory and wall time.

What this bench actually measures:
    For each of three read shapes (latest, latest_with_changes, relative)
    we run four SQL variants:
      1. baseline           — the production SQL as of this writing
      2. setting            — baseline + SETTINGS optimize_aggregation_in_order=1
      3. limit1by           — full rewrite to ORDER BY + LIMIT 1 BY
      4. limit1by_final     — same, plus FINAL on the table (sanity bound)

    For each (shape, variant) we run 1 warmup + 5 trials, tagged with a
    distinct log_comment so we can pull per-query peak/sum memory from
    system.query_log after the run. We also assert the result set
    matches the baseline (count + value sums match within float slop).

    Data: whatever is already in ClickHouse from the existing
    ``benchmarks/`` run (scale=200, kt_depth_fill applied, 30 M rows,
    37 830 series, 1.728 M (sid, vt) groups for the forecast subset).

Run:
    uv run python backend/libs/energydb/benchmark_argmax_vs_limit1by/bench.py
"""

from __future__ import annotations

import csv
import gc
import os
import statistics
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import clickhouse_connect
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv(Path(__file__).parents[1] / ".env")

CH_URL = os.environ["TIMEDB_CH_URL"]
PG_URL = os.environ.get("TIMEDB_PG_DSN") or os.environ["DATABASE_URL"]

SCALE = 200
ROOT_NAME = f"bench_root_{SCALE}"

# Same window the production bench uses for read_latest at scale=200.
BASE_VALID_TIME = datetime(2026, 1, 1, tzinfo=UTC)
POINTS_PER_FORECAST_RUN = 288  # 3 days @ 15-min
END_VALID = BASE_VALID_TIME + timedelta(minutes=15 * POINTS_PER_FORECAST_RUN)

# For the relative case we use a 1-day window with a 0-offset, which
# selects the most recent kt issued before the window's start. The
# exact knobs don't matter for what we're measuring (predicate
# behavior), as long as it's the same across all variants.
RELATIVE_WINDOW = timedelta(days=1)
RELATIVE_OFFSET = timedelta(hours=0)
RELATIVE_START_WINDOW = BASE_VALID_TIME - timedelta(days=1)

N_WARMUP = 1
N_TRIALS = 5

OUT_PATH = Path(__file__).parent / "results.csv"

# ---------------------------------------------------------------------------
# Series id resolution (so the SQL hits the same rows the prod read would)
# ---------------------------------------------------------------------------


def resolve_forecast_series_ids() -> list[int]:
    """Return the series_ids for ``forecast`` series under ``bench_root_200``.

    The prod read path resolves these through PG (node tree → asset uuids →
    energydb.series). We replicate that here so the variant SQLs hit
    exactly the same row set.
    """
    import psycopg

    with psycopg.connect(PG_URL) as conn:  # type: ignore[arg-type]
        row = conn.execute(
            "SELECT uuid FROM energydb.node WHERE parent_uuid IS NULL AND name = %s",
            (ROOT_NAME,),
        ).fetchone()
        if row is None:
            raise SystemExit(
                f"Root node {ROOT_NAME!r} not found in PG — run "
                f"backend/libs/energydb/benchmarks/generate_data.py first."
            )
        root_uuid = row[0]
        rows = conn.execute(
            """
            WITH RECURSIVE subtree AS (
                SELECT uuid FROM energydb.node WHERE uuid = %s
                UNION ALL
                SELECT n.uuid FROM energydb.node n
                JOIN subtree s ON n.parent_uuid = s.uuid
            ) CYCLE uuid SET _is_cycle USING _cycle_path
            SELECT series_id
            FROM energydb.series
            WHERE node_uuid IN (SELECT uuid FROM subtree WHERE NOT _is_cycle)
              AND data_type = 'forecast'
              AND name = 'forecast'
            """,
            (root_uuid,),
        ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# SQL variants
# ---------------------------------------------------------------------------


def where_clause() -> tuple[str, dict]:
    """Reproduces ``timedb.read._where`` for the scale=200 forecast scenario."""
    return (
        "WHERE series_id IN {series_ids:Array(UInt64)}"
        "  AND retention = {retention:String}"
        "  AND valid_time >= {start_valid:DateTime64(6, 'UTC')}"
        "  AND valid_time <  {end_valid:DateTime64(6, 'UTC')}",
        {
            "retention": "medium",
            "start_valid": BASE_VALID_TIME,
            "end_valid": END_VALID,
        },
    )


# --- _read_latest ----------------------------------------------------------


def sql_latest_baseline(where: str) -> str:
    return f"""
    SELECT series_id, valid_time, argMax(value, (knowledge_time, change_time)) AS value
    FROM series_values
    {where}
    GROUP BY series_id, valid_time
    ORDER BY series_id, valid_time
    """


def sql_latest_setting(where: str) -> str:
    return sql_latest_baseline(where) + "\n    SETTINGS optimize_aggregation_in_order = 1\n"


def sql_latest_limit1by(where: str) -> str:
    return f"""
    SELECT series_id, valid_time, value
    FROM series_values
    {where}
    ORDER BY series_id, valid_time, knowledge_time DESC, change_time DESC
    LIMIT 1 BY series_id, valid_time
    """


def sql_latest_limit1by_final(where: str) -> str:
    # FINAL forces the dedup merge on the ReplacingMergeTree (if applicable).
    # series_values isn't a ReplacingMergeTree — it's a plain MergeTree — so
    # FINAL is a noop on storage, but CH still routes through the FINAL code
    # path. Kept as the user-requested sanity bound.
    return f"""
    SELECT series_id, valid_time, value
    FROM series_values FINAL
    {where}
    ORDER BY series_id, valid_time, knowledge_time DESC, change_time DESC
    LIMIT 1 BY series_id, valid_time
    """


# --- _read_relative_sql -----------------------------------------------------

_RELATIVE_PRED = (
    "knowledge_time <= addSeconds("
    "toStartOfInterval(valid_time, toIntervalSecond({window_secs:Int64}), "
    "{start_window:DateTime64(6, 'UTC')}), {offset_secs:Int64})"
)


def sql_relative_baseline(where: str) -> str:
    return f"""
    SELECT series_id, valid_time, argMax(value, (knowledge_time, change_time)) AS value
    FROM series_values
    {where}
      AND {_RELATIVE_PRED}
    GROUP BY series_id, valid_time
    ORDER BY series_id, valid_time
    """


def sql_relative_setting(where: str) -> str:
    return sql_relative_baseline(where) + "\n    SETTINGS optimize_aggregation_in_order = 1\n"


def sql_relative_limit1by(where: str) -> str:
    return f"""
    SELECT series_id, valid_time, value
    FROM series_values
    {where}
      AND {_RELATIVE_PRED}
    ORDER BY series_id, valid_time, knowledge_time DESC, change_time DESC
    LIMIT 1 BY series_id, valid_time
    """


def sql_relative_limit1by_final(where: str) -> str:
    return f"""
    SELECT series_id, valid_time, value
    FROM series_values FINAL
    {where}
      AND {_RELATIVE_PRED}
    ORDER BY series_id, valid_time, knowledge_time DESC, change_time DESC
    LIMIT 1 BY series_id, valid_time
    """


# --- _read_latest_with_changes ---------------------------------------------
# Structurally different: inner ``IN (SELECT ..., max(knowledge_time) GROUP BY)``
# picks the winning kt, outer window scans the correction chain at that kt.
# Rewrite swaps the inner GROUP BY for an ORDER BY + LIMIT 1 BY pass.


def sql_latest_changes_baseline(where: str) -> str:
    return f"""
    SELECT series_id, valid_time, change_time, value, changed_by, annotation
    FROM (
        SELECT
            series_id, valid_time, change_time, value, changed_by, annotation,
            lagInFrame(tuple(value, annotation, changed_by)) OVER (
                PARTITION BY series_id, valid_time
                ORDER BY change_time ASC
            ) AS prev_state
        FROM series_values
        {where}
          AND (series_id, valid_time, knowledge_time) IN (
              SELECT series_id, valid_time, max(knowledge_time)
              FROM series_values
              {where}
              GROUP BY series_id, valid_time
          )
    )
    WHERE prev_state IS NULL
       OR tuple(value, annotation, changed_by) IS DISTINCT FROM prev_state
    ORDER BY series_id, valid_time, change_time
    """


def sql_latest_changes_setting(where: str) -> str:
    # Push the setting into the inner GROUP BY only — that's where the
    # hash blows up. The outer window scan doesn't aggregate.
    return f"""
    SELECT series_id, valid_time, change_time, value, changed_by, annotation
    FROM (
        SELECT
            series_id, valid_time, change_time, value, changed_by, annotation,
            lagInFrame(tuple(value, annotation, changed_by)) OVER (
                PARTITION BY series_id, valid_time
                ORDER BY change_time ASC
            ) AS prev_state
        FROM series_values
        {where}
          AND (series_id, valid_time, knowledge_time) IN (
              SELECT series_id, valid_time, max(knowledge_time)
              FROM series_values
              {where}
              GROUP BY series_id, valid_time
              SETTINGS optimize_aggregation_in_order = 1
          )
    )
    WHERE prev_state IS NULL
       OR tuple(value, annotation, changed_by) IS DISTINCT FROM prev_state
    ORDER BY series_id, valid_time, change_time
    """


def sql_latest_changes_limit1by(where: str) -> str:
    # Replace the inner GROUP BY with the same LIMIT 1 BY streaming idiom.
    return f"""
    SELECT series_id, valid_time, change_time, value, changed_by, annotation
    FROM (
        SELECT
            series_id, valid_time, change_time, value, changed_by, annotation,
            lagInFrame(tuple(value, annotation, changed_by)) OVER (
                PARTITION BY series_id, valid_time
                ORDER BY change_time ASC
            ) AS prev_state
        FROM series_values
        {where}
          AND (series_id, valid_time, knowledge_time) IN (
              SELECT series_id, valid_time, knowledge_time
              FROM series_values
              {where}
              ORDER BY series_id, valid_time, knowledge_time DESC
              LIMIT 1 BY series_id, valid_time
          )
    )
    WHERE prev_state IS NULL
       OR tuple(value, annotation, changed_by) IS DISTINCT FROM prev_state
    ORDER BY series_id, valid_time, change_time
    """


def sql_latest_changes_limit1by_final(where: str) -> str:
    return f"""
    SELECT series_id, valid_time, change_time, value, changed_by, annotation
    FROM (
        SELECT
            series_id, valid_time, change_time, value, changed_by, annotation,
            lagInFrame(tuple(value, annotation, changed_by)) OVER (
                PARTITION BY series_id, valid_time
                ORDER BY change_time ASC
            ) AS prev_state
        FROM series_values FINAL
        {where}
          AND (series_id, valid_time, knowledge_time) IN (
              SELECT series_id, valid_time, knowledge_time
              FROM series_values FINAL
              {where}
              ORDER BY series_id, valid_time, knowledge_time DESC
              LIMIT 1 BY series_id, valid_time
          )
    )
    WHERE prev_state IS NULL
       OR tuple(value, annotation, changed_by) IS DISTINCT FROM prev_state
    ORDER BY series_id, valid_time, change_time
    """


SHAPES: dict[str, dict[str, callable]] = {  # type: ignore[type-arg]
    "latest": {
        "baseline": sql_latest_baseline,
        "setting": sql_latest_setting,
        "limit1by": sql_latest_limit1by,
        "limit1by_final": sql_latest_limit1by_final,
    },
    "relative": {
        "baseline": sql_relative_baseline,
        "setting": sql_relative_setting,
        "limit1by": sql_relative_limit1by,
        "limit1by_final": sql_relative_limit1by_final,
    },
    "latest_with_changes": {
        "baseline": sql_latest_changes_baseline,
        "setting": sql_latest_changes_setting,
        "limit1by": sql_latest_changes_limit1by,
        "limit1by_final": sql_latest_changes_limit1by_final,
    },
}


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def make_params(shape: str, series_ids: list[int]) -> dict:
    p: dict = {"series_ids": series_ids, "retention": "medium"}
    p["start_valid"] = BASE_VALID_TIME
    p["end_valid"] = END_VALID
    if shape == "relative":
        p["window_secs"] = int(RELATIVE_WINDOW.total_seconds())
        p["offset_secs"] = int(RELATIVE_OFFSET.total_seconds())
        p["start_window"] = RELATIVE_START_WINDOW
    return p


def _fingerprint(ch, sql: str, params: dict) -> tuple[int, float] | None:
    """``(row_count, sum(value))`` for the variant — used to assert that
    all four variants of a shape return the same data. Returns ``None`` if
    the variant raises (e.g. ``FINAL`` on a plain MergeTree)."""
    wrapped = f"SELECT count(), sum(value) FROM ({sql})"
    try:
        rows = ch.query(wrapped, parameters=params).result_rows
    except Exception as e:  # noqa: BLE001 — variant unsupported on this engine
        msg = str(e).splitlines()[0][:120]
        print(f"      (skipping — {msg})")
        return None
    n, s = rows[0]
    return int(n), float(s if s is not None else 0.0)


def main() -> None:
    print("=" * 78)
    print("  read_latest SQL variants benchmark — scale=200 forecast subset")
    print("=" * 78)

    sids = resolve_forecast_series_ids()
    print(f"  resolved {len(sids):,} forecast series_ids under {ROOT_NAME}")
    where, where_params = where_clause()

    ch = clickhouse_connect.get_client(dsn=CH_URL)

    print("\n  Computing baseline fingerprints (correctness check) …")
    fingerprints: dict[str, tuple[int, float]] = {}
    for shape, variants in SHAPES.items():
        params = make_params(shape, sids) | where_params
        fp = _fingerprint(ch, variants["baseline"](where), params)
        assert fp is not None, f"baseline must succeed (shape={shape})"
        fingerprints[shape] = fp
        print(f"    {shape:<22}  rows={fp[0]:>10,}  sum(value)={fp[1]:,.4f}")

    print("\n  Checking that variants produce the same data …")
    skipped: set[tuple[str, str]] = set()
    for shape, variants in SHAPES.items():
        params = make_params(shape, sids) | where_params
        for name, builder in variants.items():
            if name == "baseline":
                continue
            fp = _fingerprint(ch, builder(where), params)
            if fp is None:
                skipped.add((shape, name))
                continue
            n, s = fp
            n0, s0 = fingerprints[shape]
            if n != n0 or abs(s - s0) > max(1e-6 * abs(s0), 1e-6):
                raise SystemExit(f"variant {shape}/{name} disagrees with baseline: rows={n} vs {n0}, sum={s} vs {s0}")
        active = [v for v in variants if (shape, v) not in skipped]
        print(f"    {shape:<22}  {len(active)}/{len(variants)} variants agree with baseline")

    print(f"\n  Running {N_WARMUP} warmup + {N_TRIALS} trial(s) for each (shape, variant) pair …")
    rows_out: list[dict] = []
    for shape, variants in SHAPES.items():
        params = make_params(shape, sids) | where_params
        for variant, builder in variants.items():
            if (shape, variant) in skipped:
                print(f"    {shape:<22} {variant:<16} SKIPPED (variant unsupported on this engine)")
                continue
            sql = builder(where)
            # warmup (no log_comment so query_log won't include it)
            ch.command("SET log_comment = ''")
            for _ in range(N_WARMUP):
                ch.query(sql, parameters=params)
                gc.collect()

            walls = []
            for trial in range(N_TRIALS):
                tag = f"variant_bench:{shape}:{variant}:{trial}"
                ch.command(f"SET log_comment = '{tag}'")
                t0 = time.perf_counter()
                res = ch.query(sql, parameters=params)
                wall = time.perf_counter() - t0
                walls.append(wall)
                rows_out.append(
                    {
                        "shape": shape,
                        "variant": variant,
                        "trial": trial,
                        "wall_s": wall,
                        "n_rows": res.row_count,
                        "log_comment": tag,
                    }
                )
                del res
                gc.collect()
            ch.command("SET log_comment = ''")
            med = statistics.median(walls)
            print(
                f"    {shape:<22} {variant:<16} median {med * 1000:8.1f} ms   "
                f"min {min(walls) * 1000:8.1f} ms   max {max(walls) * 1000:8.1f} ms"
            )

    # Attach CH-side per-query memory from system.query_log
    print("\n  Pulling per-query memory from system.query_log …", end="", flush=True)
    ch.command("SYSTEM FLUSH LOGS")
    log_rows = ch.query(
        """
        SELECT log_comment, max(memory_usage), sum(memory_usage), count()
        FROM system.query_log
        WHERE event_date >= today() - 1
          AND log_comment LIKE 'variant_bench:%'
          AND type = 'QueryFinish'
        GROUP BY log_comment
        """
    ).result_rows
    by_tag = {r[0]: (int(r[1]), int(r[2]), int(r[3])) for r in log_rows}
    for row in rows_out:
        peak, total, count = by_tag.get(row["log_comment"], (0, 0, 0))
        row["ch_peak_mb"] = peak / (1024 * 1024)
        row["ch_sum_mb"] = total / (1024 * 1024)
        row["ch_query_count"] = count
    print(" done")

    # Write CSV
    with open(OUT_PATH, "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "shape",
                "variant",
                "trial",
                "wall_s",
                "n_rows",
                "ch_peak_mb",
                "ch_sum_mb",
                "ch_query_count",
                "log_comment",
            ],
        )
        w.writeheader()
        w.writerows(rows_out)
    print(f"  wrote {len(rows_out)} rows → {OUT_PATH.relative_to(Path.cwd())}")

    # Summary table: median wall / median peak per (shape, variant) plus
    # speedup vs baseline.
    print("\n  Summary (median over trials):")
    print(
        f"    {'shape':<22} {'variant':<16} {'wall (ms)':>11} "
        f"{'CH peak (MB)':>13} {'CH sum (MB)':>13} {'wall ×':>8} {'peak ×':>8}"
    )
    for shape in SHAPES:
        baseline_wall = statistics.median(
            r["wall_s"] for r in rows_out if r["shape"] == shape and r["variant"] == "baseline"
        )
        baseline_peak = (
            statistics.median(r["ch_peak_mb"] for r in rows_out if r["shape"] == shape and r["variant"] == "baseline")
            or 1.0
        )
        for variant in SHAPES[shape]:
            walls = [r["wall_s"] for r in rows_out if r["shape"] == shape and r["variant"] == variant]
            if not walls:
                print(f"    {shape:<22} {variant:<16}      SKIPPED (engine doesn't support this variant)")
                continue
            peaks = [r["ch_peak_mb"] for r in rows_out if r["shape"] == shape and r["variant"] == variant]
            sums = [r["ch_sum_mb"] for r in rows_out if r["shape"] == shape and r["variant"] == variant]
            mw = statistics.median(walls)
            mp = statistics.median(peaks)
            ms = statistics.median(sums)
            speedup_wall = baseline_wall / mw if mw > 0 else float("nan")
            speedup_peak = baseline_peak / mp if mp > 0 else float("nan")
            print(
                f"    {shape:<22} {variant:<16} {mw * 1000:>11.1f} "
                f"{mp:>13.1f} {ms:>13.1f} {speedup_wall:>7.2f}× {speedup_peak:>7.2f}×"
            )


if __name__ == "__main__":
    main()
