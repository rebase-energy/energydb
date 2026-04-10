# EnergyDB

Energy database that stores both energy asset metadata and time series in PostgreSQL.

EnergyDB extends [TimeDB](https://github.com/rebase-energy/timedb) with persistent storage for [EnergyDataModel](https://github.com/rebase-energy/EnergyDataModel) hierarchies — portfolios, sites, and assets — links them to time series with full auditability, and models grid topology via typed edges.

## How it works

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

## Quick start

```python
import energydb as edb
import polars as pl
from datetime import datetime, timezone
from shapely.geometry import Point
from timedb import TimeDataClient

td = TimeDataClient()
client = edb.EnergyDataClient(td)
client.create()   # creates the energydb schema + tables

# Build the hierarchy imperatively. Series declared inline on an EDM object
# via TimeSeriesDescriptors are auto-registered with the node.
portfolio_id = client.create_node(edb.Portfolio(name="My Portfolio"))

site_id = client.create_node(
    edb.Site(name="Offshore-1", geometry=Point(3.0, 55.0)),
    parent=portfolio_id,
)

t01_id = client.create_node(
    edb.WindTurbine(name="T01", capacity=3.5, hub_height=80, timeseries=[
        edb.TimeSeriesDescriptor(name="power", unit="MW", data_type=edb.DataType.ACTUAL),
        edb.TimeSeriesDescriptor(name="power", unit="MW", data_type=edb.DataType.FORECAST,
                                 timeseries_type=edb.TimeSeriesType.OVERLAPPING),
    ]),
    parent=site_id,
)

# Register a series after the fact using kwargs
client.node(id=t01_id).register_series(name="wind_speed", unit="m/s", data_type="actual")

# Write time series via the fluent API
df = pl.DataFrame({
    "valid_time": [datetime(2025, 1, 1, h, tzinfo=timezone.utc) for h in range(24)],
    "value":      [2.5 + 0.1 * h for h in range(24)],
})
client.node("My Portfolio").node("Offshore-1").node("T01").write(
    df, name="power", data_type="actual",
)

# Read a single asset
client.node("My Portfolio").node("Offshore-1").node("T01").read(
    data_type="actual", name="power",
    start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
)

# Subtree read — all actuals for 'power' across the portfolio
client.node("My Portfolio").read(data_type="actual", name="power")

# Filter descendants by EDM type
client.node("My Portfolio").find(type="WindTurbine").read(data_type="actual", name="power")

# Hierarchy queries — return EDM objects
turbine  = client.get_node("T01")
tree     = client.get_tree("My Portfolio", include_series=True)
turbines = client.query_nodes(type="WindTurbine", within="My Portfolio")
```

## Edges

Edges model typed links between nodes — lines, transformers, pipes, interconnections.

```python
from energydb import Line, JunctionPoint

bus_a_id = client.create_node(JunctionPoint(name="BusA"), parent=site_id)
bus_b_id = client.create_node(JunctionPoint(name="BusB"), parent=site_id)

line_id = client.create_edge(
    Line(name="Cable-1", capacity=500),
    from_node=bus_a_id,
    to_node=bus_b_id,
)

# Read back, query, and attach time series
line = client.get_edge(id=line_id)
client.edge(id=line_id).register_series(name="power_flow", unit="MW", data_type="actual")
client.edge(id=line_id).write(df, name="power_flow", data_type="actual")

# Navigate from an edge to its endpoints
client.edge(id=line_id).from_node().read(data_type="actual")
client.edge(id=line_id).to_node().read(data_type="actual")
```

See [`examples/quickstart.ipynb`](examples/quickstart.ipynb) for a complete walkthrough.

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
