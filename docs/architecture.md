# Architecture

EnergyDB is a facade over [TimeDB](https://github.com/rebase-energy/timedb). It adds an
arbitrary-depth hierarchy (adjacency list) and typed edges with series routing on top
of TimeDB's three-dimensional temporal storage. Users only import from `energydb`; TimeDB
is internal.

## Identity model — UUID end-to-end

Every EDM `Element` carries a `uuid: UUID` (UUID7) generated at construction. That same
UUID is the primary key on `energydb.node` (or `energydb.edge`). FKs and references all
hold UUIDs directly — no separate bigint identifier, no translation step at any boundary:

| Layer | Identifier |
|-------|------------|
| EDM in-memory | `Element.id: UUID` |
| EDM JSON wire format | `{"id": "<uuid>"}`; refs as `{"__ref__": "<uuid>"}` |
| Postgres (`node`, `edge`) | `uuid UUID PRIMARY KEY` |
| FKs (`parent_uuid`, `from_node_uuid`, …) | `UUID REFERENCES …(uuid)` |
| Series ownership | `series.node_uuid` / `series.edge_uuid` |

Path-based addressing (`client.get_node("Europe", "Sweden", "Lillgrund")`) is preserved as a
user-friendly fluent CLI; resolution walks `(parent_uuid, name)` via one indexed
recursive CTE. Names stay convenient for navigation and remain unique under a parent
(`UNIQUE (parent_uuid, name)`), but identity is the UUID.

## Module layout

| Module | Responsibility |
|--------|---------------|
| `energydb.models` | SQLAlchemy ORM tables (source of truth for schema) |
| `energydb.serialization` | EDM object ↔ DB row conversion (nodes + edges) |
| `energydb._persist` | `create_node` / `create_edge` upserts + diff-aware `register_tree_under` |
| `energydb.diff` | `TreeDiff`, `NodeChange`, `EdgeChange`, `NodeSnapshot`, `EdgeSnapshot` |
| `energydb.paths` | Path → uuid resolution (recursive CTE on `(parent_uuid, name)`) for the fluent CLI |
| `energydb._io`, `energydb._join` | Manifest read/write pipeline + post-read hierarchy hydration |
| `energydb.scope` | `NodeScope` / `EdgeScope` — fluent navigation, CRUD, timeseries I/O |
| `energydb.client` | `Client` — schema, register_tree, queries, bulk I/O |
| `energydb.__init__` | Public exports |

## Schema

All tables live in the `energydb` Postgres schema. SQLAlchemy models are the single
source of truth — no raw SQL files. Platform imports `energydb.models.Base` for Alembic.

### `energydb.node`

| Column | Type | Notes |
|--------|------|-------|
| `uuid` | `UUID PRIMARY KEY` | EDM `Element.id` round-tripped as the row PK |
| `node_type` | `TEXT NOT NULL` | `Portfolio`, `Site`, `WindFarm`, `WindTurbine`, `JunctionPoint`, … |
| `name` | `TEXT NOT NULL` | Mutable display label |
| `parent_uuid` | `UUID FK` → `node(uuid)` `ON DELETE CASCADE`, nullable | `NULL` = root |
| `data` | `JSONB NOT NULL DEFAULT '{}'` | All non-structural fields (geometry, tz, capacity, …) |
| `created_at`, `updated_at` | `TIMESTAMPTZ NOT NULL` | server default `now()` |

Constraints / indexes:
- `UNIQUE (parent_uuid, name)` — children of a parent are uniquely addressable by name
  (the fluent CLI's contract).
- partial unique index `(name) WHERE parent_uuid IS NULL` — root names are unique too.
- `INDEX (parent_uuid)`
- `GIN INDEX (data)`

### `energydb.edge`

| Column | Type | Notes |
|--------|------|-------|
| `uuid` | `UUID PRIMARY KEY` | EDM edge `Element.id` |
| `edge_type` | `TEXT NOT NULL` | `Line`, `Link`, `Pipe`, `Interconnection` |
| `label` | `TEXT` nullable | Human label (the EDM `Edge.name`) |
| `from_node_uuid` | `UUID FK` → `node(uuid)` `ON DELETE CASCADE` | Source endpoint |
| `to_node_uuid` | `UUID FK` → `node(uuid)` `ON DELETE CASCADE` | Target endpoint |
| `data` | `JSONB NOT NULL DEFAULT '{}'` | All non-structural fields (capacity, medium, directed, …) |
| `created_at`, `updated_at` | `TIMESTAMPTZ NOT NULL` | server default `now()` |

Constraints:
- `UNIQUE (edge_type, from_node_uuid, to_node_uuid)` — natural key for human-addressed
  edges (the `(from_path, to_path, type)` triple in `client.get_edge`).

### `energydb.series`

Polymorphic series owned by exactly one of `node_uuid` / `edge_uuid` (DB CHECK enforces).

| Column | Type | Notes |
|--------|------|-------|
| `series_id` | `BIGINT identity PK` | timedb-internal handle; stays bigint |
| `node_uuid` | `UUID FK` → `node(uuid)` `ON DELETE CASCADE`, nullable | |
| `edge_uuid` | `UUID FK` → `edge(uuid)` `ON DELETE CASCADE`, nullable | |
| `data_type` | `TEXT NOT NULL` | `"forecast"`, `"actual"`, `"observation"`, … |
| `name`, `canonical_unit`, `timeseries_type`, `retention`, `description` | | |

Constraints:
- `(node_uuid IS NULL) <> (edge_uuid IS NULL)` — exclusive ownership.
- `UNIQUE (node_uuid, data_type, name)` / `UNIQUE (edge_uuid, data_type, name)`.
- Trigger `_series_guard_immutable`: `retention`, `canonical_unit`, and the owner
  columns can't change after insert.

### `energydb.runs`

Run metadata. `run_id BIGINT` is client-generated (uuid7 truncated to 63 bits) so writes
don't wait on a PG allocation round-trip.

## `register_tree` modes

The single entry point for structure persistence — every node, every edge, every series
descriptor — driven by an in-memory EDM tree.

### Modes

```python
client.register_tree(tree)                                        # additive (default)
client.register_tree(tree, mode="additive")                       # explicit
client.register_tree(tree, mode="replace_subtree", allow_delete=True)
client.register_tree(tree, mode="replace_subtree", allow_delete=True, dry_run=True)
```

| Mode | What happens to rows under the subtree root that aren't in the target tree |
|------|----------------------------------------------------------------------------|
| `"additive"` (default) | Left untouched. Re-running with a smaller tree does not delete anything. |
| `"replace_subtree"` | Candidates for deletion. Pass `allow_delete=True` to apply; otherwise raises with the orphan list. |

### How identity choices play out

With UUID identity, conflicts on the in-memory tree resolve in one statement at the DB
layer. `create_node` uses `ON CONFLICT (uuid) DO UPDATE` so:

- **Renames** (same uuid, different `name`) → silent UPDATE.
- **Moves** (same uuid, different `parent_uuid`) → silent UPDATE.
- **Property edits** (same uuid, different `data`) → silent UPDATE.
- **Type changes** (same uuid, different `node_type`) → rejected. Element type is
  immutable for a given id.
- **Cross-tree edge endpoints** (an edge whose endpoint uuid is not in the tree) →
  rejected pre-write.

### Dry run

`dry_run=True` returns a `TreeDiff` and rolls back. The diff has flat `node_changes` /
`edge_changes` lists plus binned views (`node_inserts`, `node_renames`, `node_moves`,
`node_data_edits`, `node_deletes`, `edge_inserts`/`updates`/`deletes`). Each change
exposes `kind` (`insert` / `update` / `delete`) and convenience flags (`renamed`,
`moved`, `data_changed`, `endpoints_changed`).

`TreeDiff.print()` renders a tree-shaped textual preview:

```
Portfolio 'P'
├── ~ Site 'NewName'                          [rename 'OldName' → 'NewName']
│   ├── + WindTurbine 'T03'                   [insert]
│   ├──   WindTurbine 'T01'
│   ├── ~ WindTurbine 'T02'                   [capacity: 3.5 → 4.0]
│   └── - Battery 'B1'                        [delete] (allow_delete required)
└── → Site 'Other'                            [moved (parent <a> → <b>)]
edges:
  + Line 'Cable-1' <a-uuid> → <b-uuid>        [insert]
```

### Application order

When the diff is applied:

1. Edge deletes — so node deletes don't trip FK constraints (although `ON DELETE
   CASCADE` would handle it, this is cleaner).
2. Node upserts in DFS order — parent_uuid FKs always resolve.
3. Edge upserts — endpoints exist now.
4. Node deletes — `ON DELETE CASCADE` handles descendants and any remaining attached
   edges + series.

The whole walk runs in one Postgres transaction so partial application can't leak.

## `NodeScope` resolution

`.get_node()` and `.where()` calls are **lazy** — they accumulate path / filter
without hitting the DB. Resolution happens in one query when a terminal operation runs
(`.read()`, `.write()`, `.get()`, `.children()`, `.rename()`, `.delete()`, …):

```
client.get_node("Europe").get_node("Sweden").get_node("Lillgrund").read(data_type="forecast")
       │                  │                   │                          │
       └── lazy ──────────┴── lazy ───────────┴── lazy                   └── terminal: 1 CTE on (parent_uuid, name)
```

Identity-form lookup is `client.get_node(uuid=...)`. Path-form is the user-friendly
default; both produce the same `NodeScope`.

## `EdgeScope`

`EdgeScope` provides CRUD, timeseries I/O, and endpoint navigation for a single edge.
Identity is `edge_uuid` or the `(from_path, to_path, edge_type)` triple.

```python
client.get_edge(uuid=...).read(data_type="actual", name="power_flow")
client.get_edge(("Grid", "BusA"), ("Grid", "BusB"), type="Line").get()
client.get_edge(uuid=...).update(data={"capacity": 600})
client.get_edge(uuid=...).delete()
client.get_edge(uuid=...).from_node().read(data_type="actual")
client.get_edge(uuid=...).to_node().read(data_type="actual")
```

## Manifest I/O

`client.read()` and `client.write()` accept a Polars manifest. Routing column is one of
`node_uuid`, `edge_uuid`, or `path` (`List(Utf8)`); detected automatically. Same
pipeline for both single-series and bulk operations — the scope helpers
(`scope.read_series` etc.) build a one-row manifest and delegate.

## Transaction boundaries

- `register_tree()`, `register_tree(dry_run=True)`, `create_edge()`, `register_series()`,
  `rename()`, `update()`, `delete()` — one transaction each.
- `read()` / `write()` — delegate to TimeDB, which manages its own transactions.
- Bulk `client.read()` / `client.write()` — resolve the manifest in the energydb
  connection, then hand off to TimeDB.

## Key decisions

| Decision | Choice |
|----------|--------|
| Identity | `UUID` end-to-end. `Element.id` is the row PK. No bigint shadow. |
| Hierarchy | Arbitrary-depth tree, adjacency list (`parent_uuid`) |
| Edges | Full CRUD + series I/O + endpoint navigation. Typed, directed, with JSONB `data`. |
| Setup | `register_tree(tree)` is the single entry point — declarative, idempotent, mode-aware |
| Mutation | Renames / moves / property edits via the same uuid-keyed upsert. Imperative single-element ops on `NodeScope` for surgical changes. |
| Series descriptor | `TimeSeriesDescriptor` lives in `timedatamodel` (upstream) |
| Retention | TimeDB concern; default by series shape (FLAT → forever, OVERLAPPING → medium), override at registration |
| Fluent scope | `NodeScope`: full CRUD + series + `get()`. `EdgeScope`: CRUD + series + `get()` + `from_node()` / `to_node()`. |
| Multi-tenancy | Tenant-blind. No unique on root name. Platform scopes via uuid. |
| Read results | Include full ancestor `path` and `node_uuid` columns |
| Schema source of truth | SQLAlchemy models. No raw SQL files. |
| Manifest routing | `node_uuid`, `path`, or `edge_uuid` — mutually exclusive, autodetected |
