# Development Setup

## 1) Clone the repository

If you have not cloned the project yet:

```bash
git clone https://github.com/rebase-energy/energydb.git
cd energydb
```

## 2) Installation

Set up your preferred Python virtual environment first, then install the package in editable mode with development dependencies.

Create a virtual environment (choose one):

```bash
uv venv
```

```bash
python -m venv .venv
```

Activate it:

```bash
# Bash/Zsh
source .venv/bin/activate

# Fish
source .venv/bin/activate.fish
```

Using standard `pip`:

```bash
pip install -e ".[docs]"
```

Using `uv` (optional, faster dependency management):

```bash
uv pip install -e ".[docs]"
```

If you skip virtual environment activation, `uv` will fail with `No virtual environment found`.

To run scripts inside your environment:

```bash
python file.py
# or
uv run file.py
```

## 3) Database Environment

EnergyDB uses two databases:

- **PostgreSQL** — stores the asset hierarchy (`energydb.node`, `energydb.edge`), the series catalog (`energydb.series`), and run metadata (`energydb.runs`)
- **ClickHouse** — stores all time-series values (via [TimeDB](https://github.com/rebase-energy/timedb))

The simplest local setup is the same Docker stack distributed with TimeDB:

```bash
git clone https://github.com/rebase-energy/timedb.git
cd timedb/local-db
docker compose up -d
```

This boots `local_postgres` (port 5433) and `local_clickhouse` (port 8123).

Verify the containers are running:

```bash
docker ps
```

## 4) Configuration

EnergyDB reads `TIMEDB_PG_DSN` and `TIMEDB_CH_URL` from the environment.

Set them directly in your shell:

```bash
# Bash/Zsh
export TIMEDB_PG_DSN='postgresql://postgres:devpassword@127.0.0.1:5433/devdb'
export TIMEDB_CH_URL='http://default:@localhost:8123/default'
```

```fish
# Fish
set -x TIMEDB_PG_DSN postgresql://postgres:devpassword@127.0.0.1:5433/devdb
set -x TIMEDB_CH_URL http://default:@localhost:8123/default
```

Or use a `.env` file in the repository root:

```text
TIMEDB_PG_DSN=postgresql://postgres:devpassword@127.0.0.1:5433/devdb
TIMEDB_CH_URL=http://default:@localhost:8123/default
```

## 5) Next Steps

Now you can try the examples in `examples/`, run the tests in `tests/`, or build your own script using the SDK. Create the schema once before running anything:

```python
import energydb as edb

client = edb.Client()
client.create()
```

## 6) Running Tests

The test suite uses `pytest` and assumes the local PostgreSQL + ClickHouse stack is reachable.

```bash
pytest
```

## 7) Building Documentation

Generate HTML documentation with Sphinx:

```bash
sphinx-build -b html docs/ docs/_build/html
```

The built site will be available at `docs/_build/html/`. Notebooks under
`examples/*.ipynb` are auto-copied into `docs/notebooks/` at build time.
