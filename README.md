<div align="center">
  <h1>⚡ EnergyDB</h1>
  <p><b>Persistent storage for energy portfolios — assets, grid topology, and bitemporal time series, in one connected database.</b></p>

  <a href="https://pypi.org/project/energydb/"><img alt="PyPI" src="https://img.shields.io/pypi/v/energydb?color=blue&style=flat-square"></a>
  <a href="https://pypi.org/project/energydb/"><img alt="Python Versions" src="https://img.shields.io/pypi/pyversions/energydb?style=flat-square"></a>
  <a href="https://github.com/rebase-energy/energydb/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/badge/License-Apache%202.0-green.svg?style=flat-square"></a>
  <a href="https://www.rebase.energy/join-slack"><img alt="Slack" src="https://img.shields.io/badge/Slack-Join%20Community-4A154B?logo=slack&style=flat-square"></a>
</div>

<br/>

**EnergyDB** extends [TimeDB](https://github.com/rebase-energy/timedb) with persistent storage for [EnergyDataModel](https://github.com/rebase-energy/EnergyDataModel) hierarchies — portfolios, sites, and assets — links them to bitemporal time series with full auditability, and models grid topology via typed edges. Round-trip a portfolio between Python and Postgres without losing identity: every `Element` keeps its UUID end-to-end.

Most time-series systems are agnostic about what their series represent. EnergyDB knows it is a portfolio: assets, sites, and grid topology, with the bitemporal series that describe them living alongside.

---

## 🏗️ The Connected Portfolio Model

EnergyDB stores three kinds of objects in one connected database:

| Layer | Description | Real-World Example |
| :---- | :--- | :--- |
| 🌳&nbsp;**Hierarchy** | Arbitrary-depth tree of portfolios, sites, and assets | *"Offshore-1 → WindTurbine T01 → power"* |
| 🔗&nbsp;**Topology** | Typed edges (Line, Link, Pipe) connect any two nodes | *"Cable-1: BusA → BusB"* |
| ⏱️&nbsp;**Bitemporal series** | Forecast revisions and audit trails — owned by a node *or* edge | *"power_flow on Cable-1, valid Wed 12:00, known Mon 18:00"* |

> **Identity & Round-Trip:** Every `Element` carries a UUID7. Read → modify → write back works in place — renames, moves, and property edits become silent `UPDATE`s.

---

## ✨ Why Choose EnergyDB?

- 🌳 **Asset hierarchies:** Declare your portfolio in Python (EnergyDataModel) and persist arbitrary depth in one call.
- 🔗 **Grid topology:** Typed edges for lines, links, pipes — with their own time series and endpoint navigation.
- 🔁 **Round-trip persistence:** UUID identity from in-memory `Element` to Postgres row PK; no delete-then-insert dance.
- ⏱️ **Bitemporal series:** Forecast revisions, corrections, and time-of-knowledge queries powered by [TimeDB](https://github.com/rebase-energy/timedb).
- 🧭 **Fluent, lazy navigation:** `client.get_node("Portfolio", "Site", "T01").read(...)` resolves to one indexed CTE.

---

## 🚀 Quick Start

### 1. Installation

```bash
pip install energydb
```

Requires Python 3.12+, PostgreSQL (asset hierarchy + series catalog), and ClickHouse (time-series values).

### 2. Usage Example

```python
from datetime import UTC, datetime

import energydb as edb
import pandas as pd

client = edb.Client()  # reads TIMEDB_PG_DSN / TIMEDB_CH_URL from env
client.create()        # PG schema + CH series_values table

# 1. Declare a turbine and the series it will hold (descriptors only).
t01 = edb.wind.WindTurbine(
    name="T01", lat=55.01, lon=3.02, capacity=3.5, hub_height=80,
    timeseries=[
        edb.TimeSeriesDescriptor(name="power", unit="MW",
                                 data_type=edb.DataType.ACTUAL),
    ],
)

# 2. Wrap it in a site and a portfolio.
site = edb.Site(name="Offshore-1", lat=55.0, lon=3.0, members=[t01])
portfolio = edb.Portfolio(name="my-portfolio", members=[site])

# 3. Persist structure (nodes, edges, descriptors). Idempotent.
client.register_tree(portfolio)

# 4. Write a day of hourly values for the turbine's power series.
start = datetime(2026, 1, 1, tzinfo=UTC)
df = pd.DataFrame({
    "valid_time": pd.date_range(start, periods=24, freq="1h", tz="UTC"),
    "value": [2.5 + 0.05 * i for i in range(24)],
})
client.get_node("my-portfolio", "Offshore-1", "T01").write(
    df, name="power", data_type="actual",
)

# 5. Read back — single asset, or across the whole portfolio.
client.get_node("my-portfolio", "Offshore-1", "T01").read(name="power", data_type="actual")
client.get_node("my-portfolio").read(name="power", data_type="actual")

# 6. Reconstruct the full EDM tree from the database.
tree = client.get_tree("my-portfolio", include_series=True)
```

---

## 🧪 Try it in Google Colab

Want to try EnergyDB without a local setup? Open our Quickstart in Colab — the first cell automatically installs PostgreSQL + ClickHouse inside the VM.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/energydb/blob/main/examples/quickstart.ipynb)

> **Note:** Data persists only within the active Colab session. Additional notebooks are available in the `examples/` directory.

---

## 📚 Documentation & Resources

- [📖 Official Documentation](https://energydb.readthedocs.io)
- [⚙️ Installation Guide](https://energydb.readthedocs.io/en/latest/installation.html)
- [🐍 Python SDK Documentation](https://energydb.readthedocs.io/en/latest/sdk.html)
- [🌐 Reference](https://energydb.readthedocs.io/en/latest/reference.html)
- [💡 Examples & Notebooks](examples/)

---

## 📦 Related Projects

| Project | Description |
| :------ | :---------- |
| [TimeDB](https://github.com/rebase-energy/timedb) | Bitemporal time-series database with auditability and overlapping-forecast support |
| [TimeDataModel](https://github.com/rebase-energy/TimeDataModel) | Pythonic data model for time series |
| [EnergyDataModel](https://github.com/rebase-energy/EnergyDataModel) | Data model for energy assets (solar, wind, battery, grid, ...) |

---

## 🤝 Contributing

Contributions are welcome! If you're interested in improving EnergyDB, please see our [Development Guide](DEVELOPMENT.md) for local setup instructions.

---

<div align="center">
<p>Licensed under the <a href="LICENSE">Apache-2.0 License</a>.</p>
<p>Find a bug or have a feature request? <a href="https://github.com/rebase-energy/energydb/issues">Open an Issue</a>.</p>
</div>
