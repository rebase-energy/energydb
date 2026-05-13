"""
Colab setup script — installs PostgreSQL + ClickHouse + energydb.

Downloaded and executed by the first cell of each example notebook.
"""

import os
import subprocess
import sys


def _run(cmd):
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if result.returncode != 0:
        print(result.stderr[-2000:] or result.stdout[-2000:])
        raise RuntimeError(f"Command failed (exit {result.returncode}): {cmd}")


print("1/4  Installing energydb …")
# Pinned to a commit that has the new mutator API (positional `update(data)`,
# `add()`, `dry_run=`, `transaction()`). PyPI 0.5.0 still ships the pre-0.4.0 API;
# revert to `pip install -q energydb` once a fixed wheel is published.
_ENERGYDB_REV = "3242669615b39d8833f94367419e1a443e55b176"
_run(f'"{sys.executable}" -m pip install -q git+https://github.com/rebase-energy/energydb.git@{_ENERGYDB_REV}')

print("2/4  Configuring and starting PostgreSQL …")
_run("apt-get -qq update")
_run("DEBIAN_FRONTEND=noninteractive apt-get -qq install -y postgresql postgresql-contrib")
_run("service postgresql start")
with open("/tmp/init_energydb.sql", "w") as f:
    f.write("ALTER USER postgres PASSWORD 'energydb';\nCREATE DATABASE energydb;\n")
_run('su - postgres -c "psql -f /tmp/init_energydb.sql"')

print("3/4  Installing and starting ClickHouse …")
_run("apt-get -qq install -y apt-transport-https ca-certificates")
_run(
    "curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key"
    " | gpg --dearmor --yes --batch -o /usr/share/keyrings/clickhouse-keyring.gpg"
)
_run(
    'echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg]'
    ' https://packages.clickhouse.com/deb stable main"'
    " | tee /etc/apt/sources.list.d/clickhouse.list"
)
_run("apt-get -qq update")
_run("DEBIAN_FRONTEND=noninteractive apt-get -qq install -y clickhouse-server clickhouse-client")
_run("service clickhouse-server start")

print("4/4  Setting environment variables …")
os.environ["TIMEDB_PG_DSN"] = "postgresql://postgres:energydb@localhost/energydb"
os.environ["TIMEDB_CH_URL"] = "http://default:@localhost:8123/default"
print("✓  Ready — PostgreSQL + ClickHouse running in this Colab session.")
