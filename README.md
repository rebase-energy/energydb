# EnergyDB

Energy database that stores both energy asset metadata and time series in PostgreSQL.

EnergyDB extends [TimeDB](https://github.com/rebase-energy/timedb) with persistent storage for [EnergyDataModel](https://github.com/rebase-energy/EnergyDataModel) hierarchies — portfolios, sites, and assets — and links them to time series with full auditability.

## How it works

EnergyDB bridges two libraries:

- **EnergyDataModel** defines energy assets in Python (wind turbines, solar PV, batteries, etc.) organized into hierarchies (Portfolio → Site → Asset → TimeSeries).
- **TimeDB** stores time series in PostgreSQL with three-dimensional temporal tracking (valid time, knowledge time, change time).

EnergyDB adds asset tables to the same PostgreSQL database and links them to TimeDB's time series, enabling SQL joins across both.

```
Portfolio
  └── Site "Offshore-1"
        ├── WindTurbine "T01"  ←  static: capacity, hub_height, ...
        │     ├── TimeSeries "active_power"  ←  stored in TimeDB
        │     └── TimeSeries "wind_speed"    ←  stored in TimeDB
        └── WindTurbine "T02"
```

## Quick start

```python
import energydatamodel as edm
from energydb import EnergyDataClient

# Connect to any PostgreSQL database
edb = EnergyDataClient(conninfo="postgresql://user:pass@host/db")
edb.create()

# Define assets with time series attached
ts = edm.TimeSeries(name="active_power", df=pd.DataFrame({
    "valid_time": pd.date_range("2025-01-01", periods=24, freq="h", tz="UTC"),
    "value": [2.5, 3.1, ...],
}))
turbine = edm.WindTurbine(name="T01", capacity=3.5, hub_height=80, timeseries=[ts])
site = edm.Site(name="Offshore-1", assets=[turbine], latitude=55.0, longitude=3.0)
portfolio = edm.Portfolio(name="My Portfolio", collections=[site])

# Save everything in one call — assets, hierarchy, and time series
edb.save(portfolio)

# Read back the asset with its time series from the database
turbine = edb.get_asset("T01")
print(turbine.capacity)      # 3.5
print(turbine.timeseries)    # TimeSeries loaded from TimeDB

# Query across the hierarchy
edb.query_assets(portfolio="My Portfolio", asset_type="WindTurbine")

# Reconstruct the full tree
edb.get_portfolio("My Portfolio")
```

See [`examples/notebook.ipynb`](examples/notebook.ipynb) for a complete walkthrough.

## Installation

```bash
pip install energydb
```

Requires a PostgreSQL database (e.g., [Neon](https://neon.tech), local Postgres, or any hosted provider).

## Related projects

| Project | Description |
|---------|-------------|
| [TimeDB](https://github.com/rebase-energy/timedb) | Time series database with auditability and overlapping forecast support |
| [TimeDataModel](https://github.com/rebase-energy/TimeDataModel) | Pythonic data model for time series |
| [EnergyDataModel](https://github.com/rebase-energy/EnergyDataModel) | Data model for energy assets (solar, wind, battery, grid, ...) |
