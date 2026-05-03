# Quickstart

EnergyDB extends [TimeDB](https://github.com/rebase-energy/timedb) with persistent
storage for [EnergyDataModel](https://github.com/rebase-energy/EnergyDataModel) trees,
links them to time series, and models grid topology via typed edges. Every node and
edge is identified by a UUID — the same UUID lives on the in-memory `Element`, in the
JSON wire format, and as the row PK in Postgres.

## Installation

```bash
pip install energydb
```

EnergyDB requires a PostgreSQL database (e.g. [Neon](https://neon.tech), local Postgres,
or any hosted provider) and a running ClickHouse instance for TimeDB's series storage.

## Connecting

```python
from energydb import EnergyDBClient

client = EnergyDBClient()  # reads TIMEDB_PG_DSN / TIMEDB_CH_URL from env
client.create()            # CREATE SCHEMA + Base.metadata.create_all + CH series_values
```

## Building the hierarchy with `register_tree`

`register_tree(tree)` is the single entry point for structure. Build the whole portfolio
top-down as one nested expression and persist it in one call. Idempotent.

```python
import energydb as edb

portfolio = edb.Portfolio(
    name="My Portfolio",
    members=[
        edb.Site(
            name="Offshore-1",
            lat=55.0, lon=3.0,
            members=[
                edb.WindTurbine(
                    name="T01", capacity=3.5, hub_height=80,
                    timeseries=[
                        edb.TimeSeriesDescriptor(name="power", unit="MW",
                                                 data_type=edb.DataType.ACTUAL),
                        edb.TimeSeriesDescriptor(name="power", unit="MW",
                                                 data_type=edb.DataType.FORECAST,
                                                 timeseries_type=edb.TimeSeriesType.OVERLAPPING),
                    ],
                ),
                edb.WindTurbine(name="T02", capacity=3.5, hub_height=80, timeseries=[
                    edb.TimeSeriesDescriptor(name="power", unit="MW",
                                             data_type=edb.DataType.ACTUAL),
                ]),
            ],
        ),
    ],
)

root_uuid = client.register_tree(portfolio)
```

Every `Element` got its `uuid` at construction; `register_tree` writes them straight
into the row PK. Re-running with the same tree is a no-op.

## `register_tree` modes

```python
client.register_tree(tree)                                            # additive (default)
client.register_tree(tree, mode="additive")                           # explicit
client.register_tree(tree, mode="replace_subtree", allow_delete=True) # authoritative
client.register_tree(tree, mode="replace_subtree", allow_delete=True, dry_run=True)
```

| Mode | Rows under the subtree root not in the target tree |
|------|-----------------------------------------------------|
| `"additive"` | Left untouched. Re-running with a smaller tree never deletes. |
| `"replace_subtree"` | Candidates for deletion. `allow_delete=True` applies; otherwise raises. |

With UUID identity, **renames / moves / property edits all upsert in place** (same uuid,
different `name` / `parent_uuid` / `data`). Type changes raise — element type is
immutable for a given id.

```python
# Read existing tree, modify, write back authoritatively.
tree = client.get_tree("My Portfolio")
tree.members[0].name = "Renamed-Site"          # silent rename
tree.members[0].members[0].capacity = 4.0      # silent property edit
del tree.members[0].members[1]                  # remove a turbine

# Preview before applying.
diff = client.register_tree(tree, mode="replace_subtree",
                            allow_delete=True, dry_run=True)
diff.print()

# Apply.
client.register_tree(tree, mode="replace_subtree", allow_delete=True)
```

## Bulk timeseries write

`client.write(manifest)` loads timeseries via a Polars manifest. Routing column is one
of `node_uuid`, `edge_uuid`, or `path` (`List(Utf8)`).

```python
import polars as pl
from datetime import datetime, timezone, timedelta

base = datetime(2026, 1, 1, tzinfo=timezone.utc)
hours = [base + timedelta(hours=h) for h in range(24)]

manifest = pl.DataFrame({
    "path":       [["My Portfolio", "Offshore-1", "T01"]] * 24,
    "data_type":  ["actual"] * 24,
    "name":       ["power"] * 24,
    "valid_time": hours,
    "value":      [2.5 + 0.1 * h for h in range(24)],
})
client.write(manifest)
```

## Reading

```python
# Single-series read (path-based fluent CLI)
df = client.node("My Portfolio", "Offshore-1", "T01").read(
    data_type="actual", name="power",
    start_valid=base,
)

# Subtree read — every actual `power` across the whole portfolio
df = client.node("My Portfolio").read(data_type="actual", name="power")

# Filter descendants by EDM type
df = client.node("My Portfolio").where(type="WindTurbine").read(
    data_type="actual", name="power",
)
```

Read results include `path` (List(Utf8)), `node` (name), `node_type`, `node_uuid`,
`data_type`, `name`, `series_id`, `valid_time`, `value`.

## Tree reconstruction

```python
# Single node reconstructed as an EDM object — uuid populated from the DB
turbine = client.get_node("T01")           # by name (within scope)
turbine = client.node("My Portfolio", "Offshore-1", "T01").get()
turbine = client.node(uuid=...).get()      # by uuid

# Full subtree as an EDM tree
tree = client.get_tree("My Portfolio", include_series=True)

# Flat list filtered by type / subtree / properties
turbines = client.query_nodes(type="WindTurbine", within="My Portfolio")
```

## Edges

Edges model typed cross-tree links — lines, transformers, pipes, interconnections.

```python
from energydatamodel.reference import Reference

bus_a = edb.JunctionPoint(name="BusA")
bus_b = edb.JunctionPoint(name="BusB")
line = edb.Line(
    name="Cable-1",
    capacity=500,
    from_element=Reference(bus_a),
    to_element=Reference(bus_b),
)

# Persist the topology in one call — register_tree handles nodes then edges.
client.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b, line]))

# Lookup by uuid or by triple
e = client.get_edge(uuid=line.id)
e = client.get_edge(("Grid", "BusA"), ("Grid", "BusB"), type="Line")

# Series on an edge
scope = client.edge(uuid=line.id)
scope.register_series(name="power_flow", canonical_unit="MW",
                      data_type="actual", timeseries_type="FLAT")
scope.write_series(df, name="power_flow", data_type="actual")
scope.read(name="power_flow", data_type="actual")
```

## Manifest routing — three forms

```python
# By uuid (programmatic)
manifest = pl.DataFrame({
    "node_uuid": [str(t01.id), str(t02.id)],
    "data_type": ["forecast"] * 2,
    "name":      ["power"] * 2,
})

# By path (human-readable; List(Utf8) preserves names with `/`, `.`, spaces)
manifest = pl.DataFrame([
    {"path": ("My Portfolio", "Offshore-1", "T01"), "data_type": "forecast", "name": "power"},
    {"path": ("My Portfolio", "Offshore-1", "T02"), "data_type": "forecast", "name": "power"},
])

# By edge_uuid (for edge-attached series)
manifest = pl.DataFrame({
    "edge_uuid": [str(line.id)],
    "data_type": ["actual"],
    "name":      ["power_flow"],
})
```

Routing modes are mutually exclusive — autodetected by column name.

## Imperative single-element ops

For surgical edits, fluent scope ops:

```python
client.node("My Portfolio", "Offshore-1", "T01").rename("T01-A")
client.node("My Portfolio", "Offshore-1", "T01-A").update(data={"capacity": 4.5})
client.node("My Portfolio", "Offshore-1", "T01-A").move_to(client.node("My Portfolio", "Onshore-1"))
client.node("My Portfolio", "Offshore-1", "T01-A").delete()
```

See [`examples/quickstart.ipynb`](https://github.com/rebase-energy/energydb/blob/main/examples/quickstart.ipynb)
for a complete walkthrough.
