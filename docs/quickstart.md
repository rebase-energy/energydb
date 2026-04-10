# Quickstart

EnergyDB extends [TimeDB](https://github.com/rebase-energy/timedb) with persistent storage for
[EnergyDataModel](https://github.com/rebase-energy/EnergyDataModel) hierarchies and links them
to time series with full auditability.

## Installation

```bash
pip install energydb
```

EnergyDB requires a PostgreSQL database (e.g. [Neon](https://neon.tech), local Postgres,
or any hosted provider) and a running ClickHouse instance for TimeDB's series storage.

## Connecting

`EnergyDataClient` uses composition: it holds a reference to a `TimeDataClient` and shares
its connection pool. Schema management happens via `.create()` / `.delete()`.

```python
from timedb import TimeDataClient
from energydb import EnergyDataClient

td = TimeDataClient()
client = EnergyDataClient(td)
client.create()   # CREATE SCHEMA + Base.metadata.create_all
```

## Building the hierarchy

Setup is **imperative** and **idempotent** — every `create_node` call is an upsert.
Series declared inline on an EDM object via `TimeSeriesDescriptor` are auto-registered.

```python
import energydb as edb
from shapely.geometry import Point

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
```

## Writing and reading time series

Terminal operations on a `NodeScope` resolve the lazy path in a single query and dispatch
to TimeDB.

```python
import polars as pl
from datetime import datetime, timezone

df = pl.DataFrame({
    "valid_time": [datetime(2025, 1, 1, h, tzinfo=timezone.utc) for h in range(24)],
    "value":      [2.5 + 0.1 * h for h in range(24)],
})

# Single-series write
client.node("My Portfolio").node("Offshore-1").node("T01").write(
    df, name="power", data_type="actual",
)

# Single-series read
client.node("My Portfolio").node("Offshore-1").node("T01").read(
    data_type="actual", name="power",
    start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
)

# Subtree read — every matching series below the scope
client.node("My Portfolio").read(data_type="actual", name="power")

# Filter descendants by EDM type
client.node("My Portfolio").find(type="WindTurbine").read(
    data_type="actual", name="power",
)
```

Read results always include full ancestor context (`path`, `node_id`, `node`, `node_type`,
`data_type`, `name`, `unit`, `series_id`, `valid_time`, `value`).

## Hierarchy queries

```python
# Single node (no children), reconstructed as an EDM object
turbine = client.get_node("T01")

# Same thing via the fluent API
turbine = client.node("My Portfolio").node("Offshore-1").node("T01").get()

# Full EDM tree with descendants; optionally attach TimeSeriesDescriptors
tree = client.get_tree("My Portfolio", include_series=True)

# Flat list — EDM objects filtered by type / subtree / properties
turbines = client.query_nodes(type="WindTurbine", within="My Portfolio")
```

## Edges

Edges model typed links between nodes (lines, transformers, pipes, interconnections).
They carry their own properties and can have time series attached (e.g. power flow).

```python
# Create grid nodes
bus_a_id = client.create_node(edb.JunctionPoint(name="BusA"), parent=site_id)
bus_b_id = client.create_node(edb.JunctionPoint(name="BusB"), parent=site_id)

# Create an edge between them
line_id = client.create_edge(
    edb.Line(name="Cable-1", capacity=500),
    from_node=bus_a_id,
    to_node=bus_b_id,
)

# Read it back — from/to are Reference objects with node paths
line = client.get_edge(id=line_id)

# Same thing via the fluent API
line = client.edge(id=line_id).get()

# Query edges within a subtree
lines = client.query_edges(type="Line", within="Offshore-1")

# Register and write time series on an edge
client.edge(id=line_id).register_series(name="power_flow", unit="MW", data_type="actual")
client.edge(id=line_id).write(df, name="power_flow", data_type="actual")
client.edge(id=line_id).read(data_type="actual", name="power_flow")

# Navigate from an edge to its endpoints
client.edge(id=line_id).from_node().read(data_type="actual")
client.edge(id=line_id).to_node().read(data_type="actual")
```

## Bulk I/O

`client.read()` and `client.write()` accept a Polars manifest keyed by `node_id`, `path`,
or `edge_id`. The modes are mutually exclusive and autodetected by column name.

```python
# Read by node_id (best performance, what platform uses)
manifest = pl.DataFrame({
    "node_id":   [t01_id, t02_id],
    "data_type": ["forecast", "forecast"],
    "name":    ["power", "power"],
})
df = client.read(manifest, start_valid=dt1, end_valid=dt2)

# Read by path (human-readable, always unambiguous)
manifest = pl.DataFrame({
    "path":      ["My Portfolio/Offshore-1/T01", "My Portfolio/Offshore-1/T02"],
    "data_type": ["forecast", "forecast"],
    "name":    ["power", "power"],
})
df = client.read(manifest, start_valid=dt1, end_valid=dt2)
```

Write mirrors the same modes; the only extra requirement is the `valid_time` and
`value` columns per row.

See [`examples/quickstart.ipynb`](https://github.com/rebase-energy/energydb/blob/main/examples/quickstart.ipynb)
for a complete walkthrough.
