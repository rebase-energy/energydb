# Local databases (Postgres + ClickHouse)

A one-command Postgres + ClickHouse stack for developing against energydb locally.

```sh
docker compose up -d
```

Gives you:

- `energydb_postgres` on port **5433** (db `devdb`, user `postgres`, password `devpassword`)
- `energydb_clickhouse` on port **8123** (user `default`, password `devpassword`)

Point energydb at it (env or a `.env`):

```sh
TIMEDB_PG_DSN=postgresql://postgres:devpassword@127.0.0.1:5433/devdb
TIMEDB_CH_URL=http://default:devpassword@localhost:8123/default
```

Helpers: `./restart-db.sh` (recreate, keep data), `./clean-restart-db.sh` (wipe volumes then recreate);
stop with `docker compose down`. See [../DEVELOPMENT.md](../DEVELOPMENT.md) for the full walkthrough.

> The ClickHouse container shares ports 8123/9000 with the standalone TimeDB stack; stop that first
> if it is running (`cd <timedb>/local-db && docker compose down`).
