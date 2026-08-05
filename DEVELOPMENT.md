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

- **PostgreSQL** (port `5433`) — stores the asset hierarchy (`energydb.node`, `energydb.edge`), the series catalog (`energydb.series`), and run metadata (`energydb.runs`)
- **ClickHouse** (port `8123`) — stores all time-series values (via [TimeDB](https://github.com/rebase-energy/timedb))

Spin both up locally using Docker:

```bash
cd local-db/
docker compose up -d
```

Verify the containers are running:

```bash
docker ps
```

You should see `energydb_postgres` (port 5433) and `energydb_clickhouse` (port 8123).

> **Note:** the ClickHouse container shares ports 8123/9000 with the standalone TimeDB stack. If you previously started TimeDB's `local-db/` containers, stop them first (`cd <timedb>/local-db && docker compose down`) before bringing up EnergyDB's stack.

## 4) Configuration

EnergyDB reads `TIMEDB_PG_DSN` and `TIMEDB_CH_URL` from the environment.

Fastest option (recommended): from the repository root, copy the example environment file.

```bash
cp .env.example .env
```

Both variables are already set correctly for the local Docker setup:

```text
TIMEDB_PG_DSN=postgresql://postgres:devpassword@127.0.0.1:5433/devdb
TIMEDB_CH_URL=http://default:devpassword@localhost:8123/default
```

Alternatively, export the variables directly in your shell:

```bash
# Bash/Zsh
export TIMEDB_PG_DSN='postgresql://postgres:devpassword@127.0.0.1:5433/devdb'
export TIMEDB_CH_URL='http://default:devpassword@localhost:8123/default'
```

```fish
# Fish
set -x TIMEDB_PG_DSN postgresql://postgres:devpassword@127.0.0.1:5433/devdb
set -x TIMEDB_CH_URL http://default:devpassword@localhost:8123/default
```

## 5) Next Steps

Now you can try the examples in `examples/`, run the tests in `tests/`, or build your own script using the SDK. Create the schema once before running anything:

```python
import energydb as edb

client = edb.Client()
client.create()
```

## 6) Database Management & Tools

### Helper scripts (Bash and Fish)

If you are using the local Docker setup, use scripts in `local-db/`:

- `./restart-db.sh` or `./restart-db.fish`: Restarts containers while preserving existing data.
- `./clean-restart-db.sh` or `./clean-restart-db.fish`: Removes containers, volumes, and data, then starts fresh.

### Manual inspection

Connect to PostgreSQL with `psql`:

```bash
psql postgresql://postgres:devpassword@127.0.0.1:5433/devdb
```

Connect to ClickHouse with the HTTP interface:

```bash
curl http://localhost:8123/ping
```

Or with the native client:

```bash
docker exec -it energydb_clickhouse clickhouse-client
```

## 7) Running Tests

The test suite uses `pytest`:

```bash
pytest
```

### Live tests skip silently

A large share of the suite talks to real databases and is gated on
`TIMEDB_PG_DSN` / `TIMEDB_CH_URL` being set. Without them those tests are
**skipped, not failed** — so a green run does not mean the suite passed.
Always check the skip count:

```bash
pytest -q          # look at the summary line: "N passed, M skipped"
```

If `M` is non-zero, the databases were not reachable. Bring up the stack
(step 3) and re-run; with the local Docker setup and `.env` in place the whole
suite runs with zero skips.

### The ClickHouse meta-engine tests need one extra variable

The parallel read path uses a ClickHouse `PostgreSQL()` engine table, so
**ClickHouse itself** dials PostgreSQL. The DSN in `.env` addresses PostgreSQL
from *your machine* (`127.0.0.1:5433`), which the ClickHouse container cannot
resolve. Point ClickHouse at PostgreSQL's address on the Docker network:

```bash
# Bash/Zsh — container name and internal port, not the host port-map
export ENERGYDB_CH_PG_HOST='energydb_postgres:5432'
```

```fish
# Fish
set -x ENERGYDB_CH_PG_HOST energydb_postgres:5432
```

Only the network path changes; database, user, and password still come from
the DSN. Without this, engine provisioning succeeds but engine reads fail and
degrade to the sequential path — which the tests assert against.

Two more variables are useful when running tests:

- `ENERGYDB_ENGINE_STRICT=1` — an engine-read failure raises instead of
  silently degrading, so a broken engine is loud. Recommended locally.
- `ENERGYDB_SCHEMA` — the schema under test (default `public`). The
  schema-diagnostics tests exercise a named schema.

## 8) Building Documentation

Generate HTML documentation with Sphinx:

```bash
sphinx-build -b html docs/ docs/_build/html
```

The built site will be available at `docs/_build/html/`. Notebooks under
`examples/*.ipynb` are auto-copied into `docs/notebooks/` at build time.
