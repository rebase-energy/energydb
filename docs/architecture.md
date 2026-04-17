# Architecture

EnergyDB is a facade over [TimeDB](https://github.com/rebase-energy/timedb). It adds an
arbitrary-depth hierarchy (adjacency list) and typed edges with series routing on top
of TimeDB's three-dimensional temporal storage. Users only import from `energydb`; TimeDB
is internal.

## Module layout

| Module | Responsibility |
|--------|---------------|
| `energydb.models` | SQLAlchemy ORM tables (source of truth for schema) |
| `energydb.serialization` | EDM object ↔ DB row conversion (nodes + edges) |
| `energydb._resolve` | Internal: subtree CTE, series-id / path resolution, hierarchy join-back, edge resolution |
| `energydb.scope` | `NodeScope` — fluent node navigation, CRUD, and time-series I/O (lazy); `EdgeScope` — edge CRUD, time-series I/O, and endpoint navigation |
| `energydb.client` | `EnergyDataClient` — schema, imperative CRUD, bulk I/O, tree reads, edge management |
| `energydb.__init__` | Public exports: `EnergyDataClient`, `NodeScope`, `EdgeScope`, EDM types |

Dependency flow: `client → scope → _resolve → models`. No cycles.

## Schema

All tables live in the `energydb` Postgres schema. Cross-schema FKs point at
`series.series_id`. SQLAlchemy models are the single source of truth — no raw
SQL files. Platform imports `energydb.models.Base` for Alembic.

### Node tables

**`energydb.node`** — any EDM Node type in one table (adjacency list):

| Column | Type | Notes |
|--------|------|-------|
| `node_id` | `BIGINT` identity PK | |
| `node_type` | `TEXT NOT NULL` | Portfolio, Site, WindFarm, WindTurbine, PVSystem, Battery, JunctionPoint, Meter, … |
| `name` | `TEXT NOT NULL` | |
| `parent_id` | FK → `node` `ON DELETE CASCADE`, nullable | `NULL` = root |
| `properties` | `JSONB NOT NULL DEFAULT '{}'` | domain-specific fields |
| `latitude`, `longitude`, `altitude` | `DOUBLE PRECISION` nullable | |
| `timezone` | `TEXT` nullable | |
| `created_at`, `updated_at` | `TIMESTAMPTZ NOT NULL` | server default `now()` |

Constraints / indexes:
- `UNIQUE (name, node_type, parent_id)` — child uniqueness. Root names collide freely
  (NULL != NULL in SQL) so the platform can scope multi-tenant isolation by node id.
- `INDEX (parent_id)`
- `GIN INDEX (properties)`

**`energydb.node_series`** — links nodes to TimeDB series:

| Column | Type | Notes |
|--------|------|-------|
| `node_id` | FK → `node` `ON DELETE CASCADE`, PK part | |
| `series_id` | FK → `series`, PK part | |
| `data_type` | `TEXT NOT NULL` | `"forecast"`, `"actual"`, `"observation"`, … |
| `name` | `TEXT NOT NULL` | denormalized copy of `series.name` for fast local queries |

### Edge tables

**`energydb.edge`** — typed edges between two nodes (grid topology, power flow, etc.):

| Column | Type | Notes |
|--------|------|-------|
| `edge_id` | `BIGINT` identity PK | |
| `edge_type` | `TEXT NOT NULL` | Line, Link, Transformer, Pipe, Interconnection |
| `name` | `TEXT` nullable | human-readable name |
| `from_node_id` | FK → `node` `ON DELETE CASCADE` | source node |
| `to_node_id` | FK → `node` `ON DELETE CASCADE` | target node |
| `properties` | `JSONB NOT NULL DEFAULT '{}'` | domain-specific fields (capacity, medium, …) |
| `directed` | `BOOLEAN NOT NULL DEFAULT true` | |
| `created_at`, `updated_at` | `TIMESTAMPTZ NOT NULL` | server default `now()` |

Constraints:
- `UNIQUE (edge_type, from_node_id, to_node_id)` — upsert key

**`energydb.edge_series`** — links edges to TimeDB series (power flow, thermal data, etc.):

| Column | Type | Notes |
|--------|------|-------|
| `edge_id` | FK → `edge` `ON DELETE CASCADE`, PK part | |
| `series_id` | FK → `series`, PK part | |
| `data_type` | `TEXT NOT NULL` | |
| `name` | `TEXT NOT NULL` | |

## Series architecture

### Single source of truth, no duplication

Each fact lives in exactly one authoritative location:

| Fact | Authoritative source | Denormalized copy |
|------|----------------------|-------------------|
| Series name (`"power"`) | `series.name` | `node_series.name` / `edge_series.name` |
| Unit (`"MW"`) | `series.unit` | — |
| Data type (`"forecast"`) | `node_series.data_type` | `series.labels["data_type"]` |
| Overlapping? | `series.overlapping` | — |
| Retention | `series.retention` | — |
| Description | `series.description` | — |
| Hierarchy context | `energydb.node` tree | `series.labels` |
| Node identity | `energydb.node.node_id` | `series.labels["node_id"]` |
| Edge identity | `energydb.edge.edge_id` | `series.labels["edge_id"]` |
| Asset ↔ series link | `energydb.node_series` | — |
| Edge ↔ series link | `energydb.edge_series` | — |
| Actual data | ClickHouse (via TimeDB) | — |

### `TimeSeriesDescriptor`

Structure-only metadata (no data) — a frozen dataclass imported from `timedatamodel`:

```python
from timedatamodel import TimeSeriesDescriptor, DataType, TimeSeriesType

TimeSeriesDescriptor(
    name="power",
    unit="MW",
    data_type=DataType.FORECAST,
    timeseries_type=TimeSeriesType.OVERLAPPING,
)
```

EDM's `Node.timeseries` accepts `TimeSeriesDescriptor | TimeSeries | TimeSeriesTable`.
When EnergyDB sees a descriptor on an EDM object during `create_node()`, it auto-registers
the corresponding series.

### Retention is a TimeDB concern

Retention is not in the descriptor or the energydb schema — it's passed through at
registration:

```python
client.node("T01").register_series(descriptor, retention="long")
```

TimeDB's default is `"medium"`.

## `NodeScope` resolution

All `.node()` and `.find()` calls are **lazy** — they accumulate a path / filter without
hitting the DB. Resolution happens in a single query when a terminal operation runs
(`.read()`, `.write()`, `.get()`, `.children()`, `.rename()`, `.delete()`, …):

```
client.node("Europe").node("Sweden").node("Lillgrund").read(data_type="forecast")
       │              │               │                      │
       └── lazy ──────┴── lazy ───────┴── lazy               └── terminal: resolves full path in one query
```

Ambiguity is rejected at resolution time: `node("name").read()` raises
`ValueError("Multiple nodes named 'X' (ids: [...]). Use node(id=...) to disambiguate.")`.

## `EdgeScope`

`EdgeScope` provides CRUD, time-series I/O, and endpoint navigation for a single edge.
Unlike `NodeScope`, edges are flat (no hierarchy navigation). Resolution is by
`edge_id` or `name`.

```python
client.edge(id=42).read(data_type="actual", name="power_flow")
client.edge(name="Cable-1").write(df, name="power_flow", data_type="actual")
client.edge(id=42).update(properties={"capacity": 600})
client.edge(id=42).delete()

# Get the reconstructed EDM Edge object
line = client.edge(id=42).get()

# Navigate to endpoint nodes
client.edge(id=42).from_node().read(data_type="actual")
client.edge(id=42).to_node().read(data_type="actual")
```

## Two-pass tree write

When `client.write(tree)` receives an EDM tree that contains both Node and Edge
children, it uses a two-pass approach:

1. **Pass 1 (nodes):** Walk `node.children()`, create all Nodes (skip Edges)
2. **Pass 2 (edges):** Walk again, create all Edges (from/to References now resolvable)

Both passes collect pending series data; a single bulk timedb write happens at the end.

## Transaction boundaries

- `create_node()`, `create_edge()`, `register_series()`, `rename()`, `update()`,
  `delete()`, `create_child()` — one transaction each.
- `read()` / `write()` — delegate to TimeDB, which manages its own transactions.
- Bulk `client.read()` / `client.write()` — resolve the manifest in the energydb connection,
  then hand off to TimeDB.

## Key decisions

| Decision | Choice |
|----------|--------|
| Hierarchy | Arbitrary-depth tree, adjacency list (`parent_id`) |
| Edges | Full CRUD + series I/O + endpoint navigation. Typed, directed, with JSONB properties. |
| Setup | Imperative CRUD only, all upsert (idempotent) |
| Series descriptor | `TimeSeriesDescriptor` lives in `timedatamodel` (upstream) |
| `data_type` vs. `role` | `data_type` everywhere — `role` eliminated |
| Retention | TimeDB concern; default `"medium"`, override at registration |
| Fluent scope | `NodeScope`: full CRUD + series + `get()`. `EdgeScope`: CRUD + series + `get()` + `from_node()` / `to_node()`. |
| Multi-tenancy | Tenant-blind. No unique on root name. Platform scopes via node id. |
| Renames | Supported. Label sync deferred (node_id in labels provides an immutable anchor). |
| TimeDB labels | Include `node_id` / `edge_id` as immutable anchor alongside human-readable hierarchy names |
| Read results | Include full ancestor `path` and `node_id` columns |
| Schema source of truth | SQLAlchemy models. No raw SQL files. |
| Manifest routing | `node_id`, `path`, or `edge_id` — mutually exclusive, autodetected |
