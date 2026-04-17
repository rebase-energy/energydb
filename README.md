<div align="center">
  <h1>⚡ EnergyDB</h1>
  <p><b>Persistent storage for energy asset hierarchies and time series, built on PostgreSQL.</b></p>

  <a href="https://pypi.org/project/energydb/"><img alt="PyPI" src="https://img.shields.io/pypi/v/energydb?color=blue&style=flat-square"></a>
  <a href="https://pypi.org/project/energydb/"><img alt="Python Versions" src="https://img.shields.io/pypi/pyversions/energydb?style=flat-square"></a>
  <a href="https://github.com/rebase-energy/energydb/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/badge/License-Apache%202.0-green.svg?style=flat-square"></a>
  <a href="https://www.rebase.energy/join-slack"><img alt="Slack" src="https://img.shields.io/badge/Slack-Join%20Community-4A154B?logo=slack&style=flat-square"></a>
</div>

<br/>

**EnergyDB** extends [TimeDB](https://github.com/rebase-energy/timedb) with persistent storage for [EnergyDataModel](https://github.com/rebase-energy/EnergyDataModel) hierarchies — portfolios, sites, and assets — links them to time series with full auditability, and models grid topology via typed edges.

---

## 🏗️ How It Works

EnergyDB bridges two libraries:

- **EnergyDataModel** defines energy assets in Python (wind turbines, solar PV, batteries, etc.) organized into hierarchies (Portfolio → Site → Asset → TimeSeries), plus grid topology (JunctionPoint → Line → JunctionPoint).
- **TimeDB** stores time series in PostgreSQL with three-dimensional temporal tracking (valid time, knowledge time, change time).

EnergyDB adds node and edge tables to the same PostgreSQL database and links them to TimeDB's time series, enabling SQL joins across both.

```
Portfolio
  └── Site "Offshore-1"
        ├── WindTurbine "T01"  ←  static: capacity, hub_height, ...
        │     ├── TimeSeries "active_power"  ←  stored in TimeDB
        │     └── TimeSeries "wind_speed"    ←  stored in TimeDB
        ├── WindTurbine "T02"
        ├── JunctionPoint "BusA"
        └── JunctionPoint "BusB"
              └── Line "Cable-1" (BusA → BusB)  ←  edge with own TimeSeries
```

---

## 🚀 Quick Start

### 1. Installation

```bash
pip install energydb
```

Requires Python 3.9+ and a PostgreSQL database (e.g., [Neon](https://neon.tech), local Postgres, or any hosted provider).

### 2. Usage Example

Build the entire tree in Python — structure, series schemas, and data — then persist it in one call.

```python
import energydb as edb
import polars as pl
from datetime import datetime, timezone, timedelta
from shapely.geometry import Point
from timedb import TimeDataClient

td = TimeDataClient()
client = edb.EnergyDataClient(td)
client.create()   # creates the energydb schema + tables

# Helper to build a simple hourly DataFrame
base = datetime(2025, 1, 1, tzinfo=timezone.utc)
index = [base + timedelta(hours=i) for i in range(24)]

def hourly(values):
    return pl.DataFrame({"valid_time": index, "value": values})

# 1. Declare the full portfolio as an EDM tree
#    TimeSeries carries data inline, TimeSeriesDescriptor is schema-only
portfolio = edb.Portfolio(name="My Portfolio", members=[
    edb.Site(name="Offshore-1", geometry=Point(3.0, 55.0), members=[
        edb.WindTurbine(name="T01", capacity=3.5, hub_height=80, timeseries=[
            edb.TimeSeries(hourly([2.5 + 0.1*h for h in range(24)]),
                           name="power", unit="MW", data_type=edb.DataType.ACTUAL),
            edb.TimeSeriesDescriptor(name="power", unit="MW", data_type=edb.DataType.FORECAST,
                                     timeseries_type=edb.TimeSeriesType.OVERLAPPING),
        ]),
        edb.WindTurbine(name="T02", capacity=3.5, hub_height=80, timeseries=[
            edb.TimeSeries(hourly([2.8 + 0.1*h for h in range(24)]),
                           name="power", unit="MW", data_type=edb.DataType.ACTUAL),
        ]),
    ]),
    edb.Site(name="Rooftop-1", geometry=Point(4.5, 52.0), members=[
        edb.PVSystem(name="PV01", capacity=10, surface_tilt=25, surface_azimuth=180, timeseries=[
            edb.TimeSeries(hourly([5.0 + 0.2*h for h in range(24)]),
                           name="power", unit="MW", data_type=edb.DataType.ACTUAL),
        ]),
        edb.Battery(name="B01", storage_capacity=1000, max_charge=500),
    ]),
])

# 2. Persist it — one call
#    Upserts every node, registers every series, bulk-writes all data. Idempotent.
root_id = client.write(portfolio)

# 3. Read with the fluent API
client.node("My Portfolio").node("Offshore-1").node("T01").read(
    data_type="actual", name="power",
    start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
)

# Subtree read — all actuals for 'power' across the portfolio
client.node("My Portfolio").read(data_type="actual", name="power")

# Filter descendants by EDM type
client.node("My Portfolio").find(type="WindTurbine").read(data_type="actual", name="power")

# 4. Reconstruct the full EDM tree from the database
tree = client.get_tree("My Portfolio", include_series=True)
```

---

## 🧪 Try It in Google Colab

Want to try EnergyDB without a local setup? Open our Quickstart in Colab.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/energydb/blob/main/examples/quickstart.ipynb)

> **Note:** Data persists only within the active Colab session. Additional notebooks are available in the `examples/` directory.

---

## 📦 Related Projects

| Project | Description |
| :------ | :---------- |
| [TimeDB](https://github.com/rebase-energy/timedb) | Time series database with auditability and overlapping forecast support |
| [TimeDataModel](https://github.com/rebase-energy/TimeDataModel) | Pythonic data model for time series |
| [EnergyDataModel](https://github.com/rebase-energy/EnergyDataModel) | Data model for energy assets (solar, wind, battery, grid, ...) |

---

## 🤝 Contributing

Contributions are welcome! If you're interested in improving EnergyDB, please open an issue or pull request.

---

<div align="center">
<p>Licensed under the <a href="LICENSE">Apache-2.0 License</a>.</p>
<p>Find a bug or have a feature request? <a href="https://github.com/rebase-energy/energydb/issues">Open an Issue</a>.</p>
</div>
