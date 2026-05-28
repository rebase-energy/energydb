# 🚀 v0.4.0 — EnergyDB

**Breaking changes — imperative-first API.** Modifications to existing nodes/edges now go through scope mutators (`rename`, `update`, `move_to`, `delete`); `register_tree` is create-only and raises if any UUID in the payload already exists. New `client.transaction()` context manager batches mutations atomically with `preview()`. Every mutator accepts `dry_run=True` returning a `TreeDiff`.

Other breaks: DB column `energydb.edge.label` renamed to `name` (EDM already used `name`); `NodeScope.update` / `EdgeScope.update` drop the `name`/`label` kwarg (use `.rename()`) and take `data` positionally with new `replace_data` option; `register_tree` drops `mode` and `allow_delete`.

# 🚀 v0.3.2 — EnergyDB

EnergyDB is an open-source library for persisting full energy portfolios — assets, grid topology, and bitemporal time series — in one connected database backed by PostgreSQL and ClickHouse.

It extends [TimeDB](https://github.com/rebase-energy/timedb) with persistent storage for [EnergyDataModel](https://github.com/rebase-energy/energydatamodel) hierarchies, links every node and edge to its time series with stable UUID identity, and lets you round-trip a portfolio between Python and Postgres without losing any structural state.

## 🏗️ The Connected Portfolio Model

EnergyDB stores three kinds of objects in one connected database:

- 🌳 **Hierarchy** — arbitrary-depth tree of portfolios, sites, and assets
- 🔗 **Topology** — typed edges (Line, Link, Pipe) connect any two nodes and can carry their own time series
- ⏱️ **Bitemporal series** — forecast revisions and audit trails, owned by a node or an edge

## ✨ Key Features

- **Create-only `register_tree`**: `client.register_tree(portfolio)` persists every node, edge, and series declaration in one call; raises if any UUID is already present
- **UUID identity end-to-end**: `Element.id` is the row primary key — renames, moves, and property edits become silent `UPDATE`s
- **Imperative mutations with preview**: `scope.rename(...)` / `.update(data, ...)` / `.move_to(...)` / `.delete()` on every node and edge, each with `dry_run=True` returning a `TreeDiff`
- **Atomic batches**: `with client.transaction() as txn: ... txn.preview(); txn.commit()` — one connection, one transaction, preview-before-commit
- **Fluent, lazy navigation**: `client.get_node("Portfolio", "Site", "T01").read(...)` accumulates path/filters and resolves in one indexed CTE
- **Bulk manifest I/O**: write or read across many series in one call via a polars manifest with `node_uuid`, `edge_uuid`, or `path` routing
- **Unit handling**: per-row or broadcast `unit=` triggers pint-driven conversion to each series' canonical unit before write
- **Run provenance**: every write is captured in `energydb.runs` with workflow / model / params metadata

## 🛠️ Getting Started

Install via `pip`:

```bash
pip install energydb
```

Don't want to set up databases yet? Jump into our Google Colab Quickstart — it spins up a temporary PostgreSQL + ClickHouse instance for you to play with.

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rebase-energy/energydb/blob/main/examples/quickstart.ipynb)

## 📚 Resources

- **Documentation**: <https://energydb.readthedocs.io>
- **Community**: [Join us on Slack](https://www.rebase.energy/join-slack)
- **License**: Apache-2.0

**Full Changelog**: <https://github.com/rebase-energy/energydb/commits/v0.3.2>

Are you using EnergyDB in your work? We'd love to hear your feedback. Open an issue or join our Slack community to help us build the future of energy-portfolio data.
