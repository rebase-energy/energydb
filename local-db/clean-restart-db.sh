#!/usr/bin/env bash
# Clean restart: stop, remove volumes and local data dirs, then start

# Change to script directory
BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BASEDIR" || exit 1

echo "Stopping containers and removing volumes..."
docker compose down -v 2>/dev/null || true

echo "Removing local ./pgdata (if present)..."
if [ -d ./pgdata ]; then
    sudo rm -rf ./pgdata
fi

echo "Removing local ./chdata (if present)..."
if [ -d ./chdata ]; then
    sudo rm -rf ./chdata
fi

echo "Starting containers..."
docker compose up -d

echo "Waiting for databases to be ready..."
sleep 10

echo ""
echo "Container status:"
docker ps | grep -E "energydb_postgres|energydb_clickhouse"

echo ""
echo "PostgreSQL initialization status:"
docker logs energydb_postgres 2>&1 | grep -E '(ready|error|listening)' | tail -3

echo ""
echo "ClickHouse health:"
curl -sS http://127.0.0.1:8123/ping || echo "ClickHouse not reachable yet"
