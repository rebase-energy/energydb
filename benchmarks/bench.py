"""End-to-end benchmark for energydb → timedb.

Run after generate_data.py:

    python backend/libs/energydb/benchmarks/bench.py

Output: results/<timestamp>.csv with one row per (operation, scale, trial).

Scale strategy: per-scale subtrees are created at generation time, so
``bench_scan_reads`` actually scales its read work with ``nc``. Narrow
reads, flat seed, kt-depth, cross-retention, and correction chain all
target specific subtrees (see constants below).
"""

from __future__ import annotations

import csv
import gc
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import polars as pl
from energydb import EnergyDBClient

from generate_data import (
    ASSETS_PER_CUSTOMER,
    BASE_VALID_TIME,
    CUSTOMER_SCALES,
    DATA_DIR,
    FLAT_TARGET,
    FORECAST_SERIES_NAME,
    FORECAST_TARGET,
    MAX_ROOT,
    MAX_SCALE,
    POINTS_PER_FORECAST_RUN,
    SERIES_IDS_PATH,
    root_name,
)

# ──────────────────────────────────────────────────────────────────────

NUM_READ_TRIALS = 3
KT_DEPTH_HOURS = 11
CORRECTION_DEPTH = 3
CORRECTION_SCALE = 10          # must be in CUSTOMER_SCALES
CORRECTION_ROOT = root_name(CORRECTION_SCALE)

BASE_KT = BASE_VALID_TIME + timedelta(hours=6)
FORECAST_END_VALID = BASE_VALID_TIME + timedelta(minutes=15 * POINTS_PER_FORECAST_RUN)

RESULTS_DIR = Path(__file__).parent / "results"

# ──────────────────────────────────────────────────────────────────────


def load_id_map() -> pl.DataFrame:
    if not SERIES_IDS_PATH.exists():
        raise FileNotFoundError(f"{SERIES_IDS_PATH} missing — run generate_data.py first")
    return pl.read_parquet(SERIES_IDS_PATH)


def load_forecast(nc: int) -> pl.DataFrame:
    return pl.read_parquet(DATA_DIR / f"forecast_{nc}.parquet")


def load_flat_seed() -> pl.DataFrame:
    return pl.read_parquet(DATA_DIR / "flat_seed.parquet")


def _timed(fn, *args, **kwargs):
    gc.collect()
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    wall = time.perf_counter() - t0
    rows = len(result) if hasattr(result, "__len__") else 0
    del result
    return wall, rows


# ──────────────────────────────────────────────────────────────────────
# Benchmarks
# ──────────────────────────────────────────────────────────────────────


def bench_setup_flat_seed(edb: EnergyDBClient, rows_out: list[dict]) -> None:
    """Flat data exists only under MAX_ROOT (narrow reads target it)."""
    df = load_flat_seed()
    print(f"  setup_flat_seed rows={len(df):,}", end="", flush=True)
    wall, _ = _timed(edb.td.write, df, target_table=FLAT_TARGET)
    print(f" … {wall:6.2f}s")
    rows_out.append(
        {
            "operation": "setup_flat_seed",
            "scale": 0,
            "trial": 0,
            "n_series": df["series_id"].n_unique(),
            "n_rows": len(df),
            "wall_s": wall,
        }
    )


def bench_forecast_write(edb: EnergyDBClient, rows_out: list[dict]) -> None:
    for nc in CUSTOMER_SCALES:
        df = load_forecast(nc)
        n_series = nc * ASSETS_PER_CUSTOMER
        print(
            f"  forecast_write C={nc:<4} series={n_series:<5,} rows={len(df):<10,}",
            end="",
            flush=True,
        )
        wall, _ = _timed(edb.td.write, df, target_table=FORECAST_TARGET, knowledge_time=BASE_KT)
        print(f" … {wall:6.2f}s  ({len(df) / wall / 1e6:.1f} M rows/s)")
        rows_out.append(
            {
                "operation": "forecast_write",
                "scale": nc,
                "trial": 0,
                "n_series": n_series,
                "n_rows": len(df),
                "wall_s": wall,
            }
        )


def bench_kt_depth_fill(edb: EnergyDBClient, rows_out: list[dict]) -> None:
    """Fill corrections under MAX_ROOT so the scan reads at max scale see depth."""
    df = load_forecast(MAX_SCALE)
    print(f"  kt_depth_fill +{KT_DEPTH_HOURS}×{len(df):,} rows", end="", flush=True)
    t0 = time.perf_counter()
    for i in range(1, KT_DEPTH_HOURS + 1):
        edb.td.write(
            df, target_table=FORECAST_TARGET, knowledge_time=BASE_KT + timedelta(hours=i)
        )
    wall = time.perf_counter() - t0
    print(f" … {wall:6.2f}s")
    rows_out.append(
        {
            "operation": "kt_depth_fill",
            "scale": MAX_SCALE,
            "trial": 0,
            "n_series": MAX_SCALE * ASSETS_PER_CUSTOMER,
            "n_rows": KT_DEPTH_HOURS * len(df),
            "wall_s": wall,
        }
    )


def bench_narrow_reads(edb: EnergyDBClient, rows_out: list[dict]) -> None:
    """Narrow reads target MAX_ROOT / C0000 / A00 (where flat seed lives)."""
    print(f"  read_single_asset        (6 series under {MAX_ROOT}/C0000/A00)", end="", flush=True)
    for trial in range(NUM_READ_TRIALS):
        wall, n = _timed(
            edb.node(MAX_ROOT).node("C0000").node("A00").read,
            start_valid=BASE_VALID_TIME,
            end_valid=FORECAST_END_VALID,
        )
        rows_out.append(
            {
                "operation": "read_single_asset",
                "scale": 1,
                "trial": trial,
                "n_series": 6,
                "n_rows": n,
                "wall_s": wall,
            }
        )
    med = np.median([r["wall_s"] for r in rows_out if r["operation"] == "read_single_asset"])
    print(f" … median {med * 1000:7.1f} ms")

    print(f"  read_single_customer     (180 series under {MAX_ROOT}/C0000)", end="", flush=True)
    for trial in range(NUM_READ_TRIALS):
        wall, n = _timed(
            edb.node(MAX_ROOT).node("C0000").read,
            start_valid=BASE_VALID_TIME,
            end_valid=FORECAST_END_VALID,
        )
        rows_out.append(
            {
                "operation": "read_single_customer",
                "scale": 1,
                "trial": trial,
                "n_series": 180,
                "n_rows": n,
                "wall_s": wall,
            }
        )
    med = np.median(
        [r["wall_s"] for r in rows_out if r["operation"] == "read_single_customer"]
    )
    print(f" … median {med * 1000:7.1f} ms")


def bench_scan_reads(edb: EnergyDBClient, rows_out: list[dict]) -> None:
    """Scan reads target the per-scale subtree so work actually scales with nc."""
    for nc in CUSTOMER_SCALES:
        root = root_name(nc)
        for trial in range(NUM_READ_TRIALS):
            wall, n = _timed(
                edb.node(root).read,
                data_type="forecast",
                name=FORECAST_SERIES_NAME,
                start_valid=BASE_VALID_TIME,
                end_valid=FORECAST_END_VALID,
            )
            rows_out.append(
                {
                    "operation": "read_latest",
                    "scale": nc,
                    "trial": trial,
                    "n_series": nc * ASSETS_PER_CUSTOMER,
                    "n_rows": n,
                    "wall_s": wall,
                }
            )
        med = np.median(
            [r["wall_s"] for r in rows_out if r["operation"] == "read_latest" and r["scale"] == nc]
        )
        print(
            f"  read_latest              C={nc:<4} series={nc * ASSETS_PER_CUSTOMER:<5,} "
            f"… median {med * 1000:7.1f} ms"
        )

    for nc in CUSTOMER_SCALES:
        root = root_name(nc)
        for trial in range(NUM_READ_TRIALS):
            wall, n = _timed(
                edb.node(root).read,
                data_type="forecast",
                name=FORECAST_SERIES_NAME,
                start_valid=BASE_VALID_TIME,
                end_valid=FORECAST_END_VALID,
                include_knowledge_time=True,
            )
            rows_out.append(
                {
                    "operation": "read_overlapping_history",
                    "scale": nc,
                    "trial": trial,
                    "n_series": nc * ASSETS_PER_CUSTOMER,
                    "n_rows": n,
                    "wall_s": wall,
                }
            )
        med = np.median(
            [
                r["wall_s"]
                for r in rows_out
                if r["operation"] == "read_overlapping_history" and r["scale"] == nc
            ]
        )
        print(
            f"  read_overlapping_history C={nc:<4} series={nc * ASSETS_PER_CUSTOMER:<5,} "
            f"… median {med * 1000:7.1f} ms"
        )


def bench_cross_retention(edb: EnergyDBClient, rows_out: list[dict]) -> None:
    """Single customer, all 6 series (1 forecast + 5 flat). The current code
    fans out to two td.read calls (overlapping_medium + flat); the unified-
    events redesign collapses this to one CH query.
    """
    print(
        f"  read_cross_retention     (180 series under {MAX_ROOT}/C0000, all data_types)",
        end="",
        flush=True,
    )
    for trial in range(NUM_READ_TRIALS):
        wall, n = _timed(
            edb.node(MAX_ROOT).node("C0000").read,
            start_valid=BASE_VALID_TIME,
            end_valid=FORECAST_END_VALID,
        )
        rows_out.append(
            {
                "operation": "read_cross_retention",
                "scale": 1,
                "trial": trial,
                "n_series": 180,
                "n_rows": n,
                "wall_s": wall,
            }
        )
    med = np.median(
        [r["wall_s"] for r in rows_out if r["operation"] == "read_cross_retention"]
    )
    print(f" … median {med * 1000:7.1f} ms")


def bench_correction_chain(edb: EnergyDBClient, rows_out: list[dict]) -> None:
    """Inject synthetic corrections into the CORRECTION_ROOT subtree, then
    measure include_updates=True reads over it.
    """
    src_df = load_forecast(CORRECTION_SCALE)
    for i in range(CORRECTION_DEPTH):
        perturbed = src_df.with_columns(pl.col("value") + (i + 1) * 0.01)
        edb.td.write(perturbed, target_table=FORECAST_TARGET, knowledge_time=BASE_KT)

    print(
        f"  read_correction_chain    C={CORRECTION_SCALE:<4} under {CORRECTION_ROOT} "
        f"(+{CORRECTION_DEPTH} corrections/row)",
        end="",
        flush=True,
    )
    for trial in range(NUM_READ_TRIALS):
        wall, n = _timed(
            edb.node(CORRECTION_ROOT).read,
            data_type="forecast",
            name=FORECAST_SERIES_NAME,
            start_valid=BASE_VALID_TIME,
            end_valid=FORECAST_END_VALID,
            include_updates=True,
        )
        rows_out.append(
            {
                "operation": "read_correction_chain",
                "scale": CORRECTION_SCALE,
                "trial": trial,
                "n_series": CORRECTION_SCALE * ASSETS_PER_CUSTOMER,
                "n_rows": n,
                "wall_s": wall,
            }
        )
    med = np.median(
        [r["wall_s"] for r in rows_out if r["operation"] == "read_correction_chain"]
    )
    print(f" … median {med * 1000:7.1f} ms")


# ──────────────────────────────────────────────────────────────────────


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["operation", "scale", "trial", "n_series", "n_rows", "wall_s"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = RESULTS_DIR / f"{stamp}.csv"

    print("=" * 72)
    print(f"  energydb benchmark  →  {out_path.relative_to(Path(__file__).parent)}")
    print("=" * 72)

    assert CORRECTION_SCALE in CUSTOMER_SCALES, (
        f"CORRECTION_SCALE={CORRECTION_SCALE} must be one of {CUSTOMER_SCALES} "
        "so its subtree exists"
    )

    edb = EnergyDBClient()
    rows: list[dict] = []

    try:
        print("\n  [1/6] Setup")
        bench_setup_flat_seed(edb, rows)

        print("\n  [2/6] Forecast write throughput")
        bench_forecast_write(edb, rows)
        write_csv(rows, out_path)

        print("\n  [3/6] KT-depth fill (under max subtree)")
        bench_kt_depth_fill(edb, rows)
        write_csv(rows, out_path)

        print("\n  [4/6] Narrow cross-shape reads (under max subtree)")
        bench_narrow_reads(edb, rows)
        write_csv(rows, out_path)

        print("\n  [5/6] Scan reads (per-scale subtrees)")
        bench_scan_reads(edb, rows)
        write_csv(rows, out_path)

        print("\n  [6/6] Cross-retention + correction chain")
        bench_cross_retention(edb, rows)
        bench_correction_chain(edb, rows)
        write_csv(rows, out_path)

        print(f"\n  ✓ wrote {len(rows)} rows to {out_path}")
    finally:
        edb.close()


if __name__ == "__main__":
    main()
