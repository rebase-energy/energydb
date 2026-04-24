# energydb benchmarks

End-to-end benchmark of the energydb → timedb stack. Uses
`EnergyDBClient`, hierarchy resolution, series metadata, unit
conversion — the real production path.

## Scenario

Mirrors `backend/libs/timedb/benchmarks/` scales, so direct comparison
is possible:

- `CUSTOMER_SCALES = [1, 10, 50, 200]`
- 30 assets/customer, 1 forecast + 5 flat series/asset
- Forecast: 3 days ahead at 15-min resolution (288 points/run)
- Flat seed: 1 day at 15-min resolution (96 points/series)
- kt-depth fill at max scale: 11 extra hourly issues

Worst-case Python-side memory (`C=200` overlapping history read):
~600 MB. Runs comfortably on 16GB.

## Run

```bash
cd backend/libs/energydb/benchmarks
python generate_data.py   # resets PG + CH schemas, registers hierarchy + series
python bench.py
```

## Compare before/after

```bash
python compare.py results/baseline.csv results/redesign.csv
```

## Layout

- `generate_data.py` — resets PG+CH, creates node tree + series rows,
  writes forecast/flat parquet fixtures with real series_ids.
- `bench.py` — runs every operation via `EnergyDBClient`.
- `compare.py` — diffs two CSVs (shared with the timedb bench).

## What this measures vs. the timedb bench

The timedb bench measures raw ClickHouse throughput. This bench adds:
- PostgreSQL series metadata resolution on every call.
- Unit conversion in Polars post-read.
- Cross-retention / cross-shape reads (the
  `partition_by("target_table")` fan-out).
- Hierarchy join in `join_hierarchy`.

The delta between the two benches tells you how much of a
production read/write is CH and how much is the energydb orchestration.
