"""Generate fixtures for the energydb benchmark.

Resets PostgreSQL + ClickHouse, then creates one independent subtree per
customer scale so that scan reads scaling in ``nc`` actually scale the
read work. Each subtree is a Portfolio → Customer → Asset tree with 6
series per asset (1 forecast + 5 flat).

    bench_root_1    → 1 customer  × 30 assets × 6 series = 180 series
    bench_root_10   → 10          × 30        × 6        = 1,800 series
    bench_root_50   → 50          × 30        × 6        = 9,000 series
    bench_root_200  → 200         × 30        × 6        = 36,000 series

Forecast parquets are generated per scale, each using the series_ids of
that scale's subtree. Flat seed data is only populated under the max
scale (where narrow-read and cross-retention benchmarks point); smaller
subtrees don't need flat data for the benchmarks that touch them.

The node tree is inserted directly via SQL rather than through the
EnergyDataModel serialization path — the benchmark isn't measuring EDM
serialization, it's measuring DB write/read, and the direct path is
dramatically faster at these scales.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import polars as pl
from energydb import EnergyDBClient
from energydb import series as series_mod

# ──────────────────────────────────────────────────────────────────────

CUSTOMER_SCALES = [1, 10, 50, 200]
ASSETS_PER_CUSTOMER = 30
FORECAST_SERIES_NAME = "forecast"
FLAT_SERIES_NAMES = ["actual", "capacity", "availability", "curtailment", "price"]
POINTS_PER_FORECAST_RUN = 288  # 3 days @ 15-min
POINTS_PER_FLAT_SEED = 96      # 1 day  @ 15-min

BASE_VALID_TIME = datetime(2024, 6, 1, tzinfo=timezone.utc)

DATA_DIR = Path(__file__).parent / "data"
SERIES_IDS_PATH = DATA_DIR / "series_ids.parquet"

FORECAST_TARGET = "overlapping_medium"
FLAT_TARGET = "flat"


def root_name(nc: int) -> str:
    return f"bench_root_{nc}"


MAX_SCALE = max(CUSTOMER_SCALES)
MAX_ROOT = root_name(MAX_SCALE)

# ──────────────────────────────────────────────────────────────────────


def _datetime_range(n_points: int, base: datetime = BASE_VALID_TIME) -> pl.Series:
    return pl.datetime_range(
        start=base,
        end=base + timedelta(minutes=15 * (n_points - 1)),
        interval="15m",
        time_unit="us",
        time_zone="UTC",
        eager=True,
    )


def create_subtree(edb: EnergyDBClient, root_name_: str, num_customers: int) -> dict[tuple[str, str], int]:
    """Insert the Portfolio → Customer → Asset subtree for one scale.

    Returns ``{(customer_name, asset_name): asset_node_id}``.
    """
    asset_ids: dict[tuple[str, str], int] = {}
    with edb._pool.connection() as conn:
        row = conn.execute(
            "INSERT INTO energydb.node (node_type, name, parent_id, data) "
            "VALUES (%s, %s, NULL, '{}'::jsonb) "
            "ON CONFLICT ON CONSTRAINT node_child_uniq "
            "DO UPDATE SET updated_at = now() RETURNING node_id",
            ("Portfolio", root_name_),
        ).fetchone()
        root_id = row[0]

        customer_ids: list[int] = []
        for c in range(num_customers):
            row = conn.execute(
                "INSERT INTO energydb.node (node_type, name, parent_id, data) "
                "VALUES ('Customer', %s, %s, '{}'::jsonb) "
                "ON CONFLICT ON CONSTRAINT node_child_uniq "
                "DO UPDATE SET updated_at = now() RETURNING node_id",
                (f"C{c:04d}", root_id),
            ).fetchone()
            customer_ids.append(row[0])

        for c, customer_id in enumerate(customer_ids):
            for a in range(ASSETS_PER_CUSTOMER):
                row = conn.execute(
                    "INSERT INTO energydb.node (node_type, name, parent_id, data) "
                    "VALUES ('Asset', %s, %s, '{}'::jsonb) "
                    "ON CONFLICT ON CONSTRAINT node_child_uniq "
                    "DO UPDATE SET updated_at = now() RETURNING node_id",
                    (f"A{a:02d}", customer_id),
                ).fetchone()
                asset_ids[(f"C{c:04d}", f"A{a:02d}")] = row[0]

        conn.commit()
    return asset_ids


def register_series_for(
    edb: EnergyDBClient, asset_ids: dict[tuple[str, str], int], root_label: str
) -> pl.DataFrame:
    """Register 6 series per asset (1 forecast + 5 flat). Returns id_map DataFrame."""
    rows: list[tuple[str, str, str, str, int, str]] = []
    with edb._pool.connection() as conn:
        for (customer, asset), node_id in asset_ids.items():
            sid = series_mod.register_series(
                conn,
                node_id=node_id,
                edge_id=None,
                data_type="forecast",
                name=FORECAST_SERIES_NAME,
                canonical_unit="MW",
                target_table=FORECAST_TARGET,
            )
            rows.append((root_label, customer, asset, FORECAST_SERIES_NAME, sid, FORECAST_TARGET))
            for name in FLAT_SERIES_NAMES:
                sid = series_mod.register_series(
                    conn,
                    node_id=node_id,
                    edge_id=None,
                    data_type="actual",
                    name=name,
                    canonical_unit="MW",
                    target_table=FLAT_TARGET,
                )
                rows.append((root_label, customer, asset, name, sid, FLAT_TARGET))
        conn.commit()

    return pl.DataFrame(
        {
            "root": [r[0] for r in rows],
            "customer": [r[1] for r in rows],
            "asset": [r[2] for r in rows],
            "series_name": [r[3] for r in rows],
            "series_id": [r[4] for r in rows],
            "target_table": [r[5] for r in rows],
        },
        schema={
            "root": pl.Utf8,
            "customer": pl.Utf8,
            "asset": pl.Utf8,
            "series_name": pl.Utf8,
            "series_id": pl.Int64,
            "target_table": pl.Utf8,
        },
    )


def generate_forecast(id_map: pl.DataFrame, num_customers: int) -> pl.DataFrame:
    """Forecast rows for the subtree at scale *num_customers*."""
    sub = (
        id_map.filter(
            (pl.col("root") == root_name(num_customers))
            & (pl.col("series_name") == FORECAST_SERIES_NAME)
        )
        .sort(["customer", "asset"])
    )
    sids = sub["series_id"].to_numpy()
    n_points = POINTS_PER_FORECAST_RUN
    n_series = len(sids)
    n_total = n_series * n_points

    times = _datetime_range(n_points)
    valid_time_col = pl.concat([times] * n_series)
    series_id_arr = np.repeat(sids, n_points)
    value_arr = np.random.default_rng(42).uniform(0, 100, n_total)

    return pl.DataFrame(
        {
            "series_id": pl.Series(series_id_arr, dtype=pl.Int64),
            "valid_time": valid_time_col,
            "value": pl.Series(value_arr, dtype=pl.Float64),
        }
    )


def generate_flat_seed(id_map: pl.DataFrame) -> pl.DataFrame:
    """Flat-seed rows under the max-scale subtree only (where narrow reads live)."""
    sub = (
        id_map.filter(
            (pl.col("root") == MAX_ROOT)
            & (pl.col("target_table") == FLAT_TARGET)
        )
        .sort(["customer", "asset", "series_name"])
    )
    sids = sub["series_id"].to_numpy()
    n_points = POINTS_PER_FLAT_SEED
    n_series = len(sids)
    n_total = n_series * n_points

    times = _datetime_range(n_points)
    valid_time_col = pl.concat([times] * n_series)
    series_id_arr = np.repeat(sids, n_points)
    value_arr = np.random.default_rng(123).uniform(0, 100, n_total)

    return pl.DataFrame(
        {
            "series_id": pl.Series(series_id_arr, dtype=pl.Int64),
            "valid_time": valid_time_col,
            "value": pl.Series(value_arr, dtype=pl.Float64),
        }
    )


def _file_size(path: Path) -> str:
    sz = path.stat().st_size
    return f"{sz / 1024:.0f} KB" if sz < 1024 * 1024 else f"{sz / (1024 * 1024):.1f} MB"


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    total_series = sum(nc * ASSETS_PER_CUSTOMER * (1 + len(FLAT_SERIES_NAMES)) for nc in CUSTOMER_SCALES)

    print("=" * 72)
    print(f"  energydb benchmark fixtures — per-scale subtrees (total series={total_series:,})")
    print("=" * 72)

    print("  Resetting PG + CH schemas ", end="", flush=True)
    edb = EnergyDBClient()
    edb.delete()
    edb.create()
    print("done")

    id_map_parts: list[pl.DataFrame] = []
    for nc in CUSTOMER_SCALES:
        t0 = time.perf_counter()
        print(f"  Subtree {root_name(nc):<18} ", end="", flush=True)
        asset_ids = create_subtree(edb, root_name(nc), nc)
        part = register_series_for(edb, asset_ids, root_label=root_name(nc))
        id_map_parts.append(part)
        print(
            f"{len(asset_ids):>6,} assets, {len(part):>6,} series "
            f"in {time.perf_counter() - t0:.1f}s"
        )

    id_map = pl.concat(id_map_parts)
    id_map.write_parquet(SERIES_IDS_PATH)
    print(
        f"  id map → {SERIES_IDS_PATH.name} "
        f"({len(id_map):,} rows, {_file_size(SERIES_IDS_PATH)})"
    )

    for nc in CUSTOMER_SCALES:
        t0 = time.perf_counter()
        df = generate_forecast(id_map, nc)
        path = DATA_DIR / f"forecast_{nc}.parquet"
        df.write_parquet(path)
        elapsed = time.perf_counter() - t0
        print(
            f"  C={nc:<4} series={nc * ASSETS_PER_CUSTOMER:<6,} "
            f"rows={len(df):<12,} → {path.name} ({_file_size(path)}) in {elapsed:.1f}s"
        )

    t0 = time.perf_counter()
    df = generate_flat_seed(id_map)
    path = DATA_DIR / "flat_seed.parquet"
    df.write_parquet(path)
    elapsed = time.perf_counter() - t0
    print(
        f"  flat seed  rows={len(df):<12,} (under {MAX_ROOT}) → "
        f"{path.name} ({_file_size(path)}) in {elapsed:.1f}s"
    )

    edb.close()
    print()


if __name__ == "__main__":
    main()
