<div align="center">
  <h1>⚡ EnergyDB</h1>
  <p><b>Persistent storage for energy portfolios — assets, grid topology, and bitemporal time series, in one connected database.</b></p>

  <a href="https://pypi.org/project/energydb/"><img alt="PyPI" src="https://img.shields.io/pypi/v/energydb?color=blue&style=flat-square"></a>
  <a href="https://pypi.org/project/energydb/"><img alt="Python Versions" src="https://img.shields.io/pypi/pyversions/energydb?style=flat-square"></a>
  <a href="https://github.com/rebase-energy/energydb/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/badge/License-Apache%202.0-green.svg?style=flat-square"></a>
  <a href="https://www.rebase.energy/join-slack"><img alt="Slack" src="https://img.shields.io/badge/Slack-Join%20Community-4A154B?logo=slack&style=flat-square"></a>
</div>

<br/>

## 🏗️ What is EnergyDB?

EnergyDB is a database for energy portfolios. It stores three things together in one connected system:

| Layer | Description | Real-World Example |
| :---- | :--- | :--- |
| 🌳&nbsp;**Asset hierarchy** | Arbitrary-depth tree of portfolios, sites, and assets | *"Offshore-1 → WindTurbine T01 → power"* |
| 🔗&nbsp;**Grid topology** | Typed edges (lines, transformers, pipes, interconnections) connecting any two assets | *"Cable-1: BusA → BusB"* |
| ⏱️&nbsp;**Bitemporal time series** | Actuals and versioned forecasts attached to any node or edge, queryable as-of any point in time | *"power_flow on Cable-1, valid Wed 12:00, known Mon 18:00"* |

Structure lives in PostgreSQL, values live in ClickHouse, and stable UUID identity lets Python objects round-trip to the database without losing any structural state. (A separate `change_time` audit field tracks corrections without polluting the bitemporal model.)

EnergyDB extends [TimeDB](https://github.com/rebase-energy/timedb) with persistent storage for [EnergyDataModel](https://github.com/rebase-energy/EnergyDataModel) hierarchies.

---

## ✨ Why EnergyDB?

Most time-series systems are agnostic about what their series represent — they treat data as opaque `(series_id, timestamp, value)` triples. EnergyDB knows it is a portfolio, and links every series back to the asset or grid edge it describes.

- 🔁 **Round-trip persistence:** Every `Element` keeps its UUID7 from in-memory object to row primary key — renames, moves, and property edits become silent `UPDATE`s, never delete-then-insert.
- 📋 **Diffable structural changes:** `dry_run=True` previews every insert, rename, move, and delete before you apply — no surprise data loss on `replace_subtree`.
- ⏱️ **Bitemporal queries:** Forecast revisions, corrections, and time-of-knowledge backtests, powered by [TimeDB](https://github.com/rebase-energy/timedb).
- 🧭 **Lazy fluent navigation:** `client.get_node("Portfolio", "Site", "T01").read(...)` resolves to one indexed SQL query, regardless of subtree size.
- ⚖️ **Unit conversion at the boundary:** Declare canonical units once; pint rescales every read and write automatically.

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
client.create()        # PostgreSQL schema + ClickHouse series_values table

# 1. Declare a turbine and the series it will hold (metadata only).
t01 = edb.wind.WindTurbine(
    name="T01", lat=55.01, lon=3.02, capacity=3.5, hub_height=80,
    timeseries=[
        edb.TimeSeries(name="power", unit="MW",
                       data_type=edb.DataType.ACTUAL),
    ],
)

# 2. Wrap it in a site and a portfolio.
site = edb.Site(name="Offshore-1", lat=55.0, lon=3.0, members=[t01])
portfolio = edb.Portfolio(name="my-portfolio", members=[site])

# 3. Persist structure (nodes, edges, series declarations). Idempotent.
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
