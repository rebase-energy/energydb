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

Structure and data live in two separate calls so each can be re-run on its own
cadence. Identity is a UUID7 generated on every `Element` at construction; that same
UUID is the row PK in Postgres, so renames / moves / property edits round-trip in
place.

```python
from datetime import UTC, datetime

import energydb as edb
import pandas as pd

client = edb.Client()  # reads TIMEDB_PG_DSN / TIMEDB_CH_URL from env
client.create()                # PG schema + CH series_values table

# 1. Declare each WindTurbine with its TimeSeriesDescriptors.
#    Descriptors are structure-only (schema); data lands later via write().
t01 = edb.wind.WindTurbine(
    name="T01", lat=55.01, lon=3.02, capacity=3.5, hub_height=80,
    timeseries=[
        edb.TimeSeriesDescriptor(name="power", unit="MW", data_type=edb.DataType.ACTUAL),
        edb.TimeSeriesDescriptor(
            name="power", unit="MW",
            data_type=edb.DataType.FORECAST,
            timeseries_type=edb.TimeSeriesType.OVERLAPPING,
        ),
    ],
)
t02 = edb.wind.WindTurbine(
    name="T02", lat=55.01, lon=3.04, capacity=3.5, hub_height=80,
    timeseries=[
        edb.TimeSeriesDescriptor(name="power", unit="MW", data_type=edb.DataType.ACTUAL),
    ],
)

# 2. Group turbines under a Site.
offshore_1 = edb.Site(name="Offshore-1", lat=55.0, lon=3.0, members=[t01, t02])

# 3. Same pattern for the second site: PV system + battery.
pv01 = edb.solar.PVSystem(
    name="PV01", capacity=10, surface_tilt=25, surface_azimuth=180,
    timeseries=[
        edb.TimeSeriesDescriptor(name="power", unit="MW", data_type=edb.DataType.ACTUAL),
    ],
)
b01 = edb.battery.Battery(name="B01", storage_capacity=1000, max_charge=500)
rooftop_1 = edb.Site(name="Rooftop-1", lat=52.0, lon=4.5, members=[pv01, b01])

# 4. Assemble the portfolio.
portfolio = edb.Portfolio(name="my-portfolio", members=[offshore_1, rooftop_1])

# 5. Persist the structure + series schemas. Idempotent.
client.register_tree(portfolio)

# 6. Write a day of hourly values for one series.
start = datetime(2026, 1, 1, tzinfo=UTC)
hours = pd.date_range(start, periods=24, freq="1h", tz="UTC")
df = pd.DataFrame({"valid_time": hours, "value": [2.5 + 0.05 * i for i in range(24)]})
client.get_node("my-portfolio", "Offshore-1", "T01").write(
    df, name="power", data_type="actual",
)

# 7. Read with the fluent API — single asset, single series.
client.get_node("my-portfolio", "Offshore-1", "T01").read(
    data_type="actual", name="power",
)

# Subtree read — all actuals for 'power' across the portfolio.
client.get_node("my-portfolio").read(data_type="actual", name="power")

# Filter descendants by EDM type.
client.get_node("my-portfolio").where(type="WindTurbine").read(data_type="actual", name="power")

# 8. Reconstruct the full EDM tree from the database.
tree = client.get_tree("my-portfolio", include_series=True)
```

### 3. Read → modify → write back

Because identity is the UUID, the round-trip preserves it. Renames, moves, and
property edits become silent UPDATEs; explicit confirmation gates destructive ops.

```python
tree = client.get_tree("my-portfolio")     # uuids populated from PG
tree.members[0].name = "Renamed-Site"      # silent rename
tree.members[0].members[0].capacity = 4.0  # silent property edit
del tree.members[0].members[1]             # remove a turbine

# Preview before applying
diff = client.register_tree(
    tree, mode="replace_subtree", allow_delete=True, dry_run=True,
)
diff.print()

# Apply
client.register_tree(tree, mode="replace_subtree", allow_delete=True)
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
