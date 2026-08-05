"""``AsyncClient.list_series`` — the series catalog for one owner.

The method had no coverage; this file is it. The behaviour under test that is
actually new is ``series_id`` in each row, which makes ``list_series`` the
reverse lookup from ``(owner, data_type, name)`` to the timedb handle. That
identifier is an *input* to timedb reads and is *returned* by
``register_series`` — it is not secret. Read **results** still never carry it,
and ``test_output_modes.py`` is what pins that.

Skipped if ``TIMEDB_PG_DSN`` / ``TIMEDB_CH_URL`` are not set.
"""

from __future__ import annotations

import os
from uuid import uuid4

import energydb as edb
import pytest
from energydatamodel import Reference
from energydb import Client
from energydb.errors import ValidationError

if not (os.environ.get("TIMEDB_PG_DSN") and os.environ.get("TIMEDB_CH_URL")):
    pytest.skip("TIMEDB_PG_DSN / TIMEDB_CH_URL not set", allow_module_level=True)

_EXPECTED_KEYS = {"series_id", "name", "data_type", "canonical_unit", "timeseries_type", "description"}


@pytest.fixture
def client():
    c = Client()
    c.delete()
    c.create()
    yield c
    c.delete()
    c.close()


def test_series_id_is_the_id_register_series_returned(client):
    """The reverse lookup: register by name, get the handle back by owner. Without
    this, recovering a series_id meant raw SQL against the catalog."""
    client.register_tree(edb.Portfolio(name="P", members=[edb.wind.WindTurbine(name="T1", capacity=3.0)]))
    node = client.get_node("P", "T1")
    sid = node.register_series(
        name="power", canonical_unit="MW", data_type="actual", timeseries_type="FLAT", retention="forever"
    )
    node_uuid = node.get_raw()["uuid"]

    rows = client.list_series(node_uuid)

    assert len(rows) == 1
    assert rows[0]["series_id"] == sid
    assert set(rows[0]) == _EXPECTED_KEYS
    assert rows[0]["name"] == "power"
    assert rows[0]["canonical_unit"] == "MW"
    assert rows[0]["timeseries_type"] == "FLAT"


def test_every_row_carries_its_own_id_in_catalog_order(client):
    """Several series on one owner: each id must track its own row, not just be
    present somewhere. Ordering stays ``(data_type, name)`` as before."""
    client.register_tree(edb.Portfolio(name="P", members=[edb.wind.WindTurbine(name="T1", capacity=3.0)]))
    node = client.get_node("P", "T1")
    ids = {
        ("actual", "power"): node.register_series(
            name="power", canonical_unit="MW", data_type="actual", timeseries_type="FLAT", retention="forever"
        ),
        ("actual", "wind_speed"): node.register_series(
            name="wind_speed", canonical_unit="m/s", data_type="actual", timeseries_type="FLAT", retention="forever"
        ),
        ("forecast", "power"): node.register_series(
            name="power", canonical_unit="MW", data_type="forecast", timeseries_type="OVERLAPPING", retention="medium"
        ),
    }

    rows = client.list_series(node.get_raw()["uuid"])

    assert [(r["data_type"], r["name"]) for r in rows] == [
        ("actual", "power"),
        ("actual", "wind_speed"),
        ("forecast", "power"),
    ]
    assert {(r["data_type"], r["name"]): r["series_id"] for r in rows} == ids


def test_edge_owned_series_get_the_id_too(client):
    """``owner_col="edge_uuid"`` is the other half of the catalog and shares the
    SELECT, but assert it — an edge-owned series is registered through a different
    scope class."""
    bus_a, bus_b = edb.grid.JunctionPoint(name="BusA"), edb.grid.JunctionPoint(name="BusB")
    client.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b]))
    line = edb.grid.Line(name="L1", capacity=500, from_element=Reference(bus_a), to_element=Reference(bus_b))
    edge_uuid = client.create_edge(line)
    sid = client.get_edge(uuid=edge_uuid).register_series(
        name="flow", canonical_unit="MW", data_type="actual", timeseries_type="FLAT", retention="forever"
    )

    rows = client.list_series(edge_uuid, owner_col="edge_uuid")

    assert len(rows) == 1
    assert rows[0]["series_id"] == sid
    assert set(rows[0]) == _EXPECTED_KEYS
    assert rows[0]["name"] == "flow"


def test_an_owner_with_no_series_lists_nothing(client):
    client.register_tree(edb.Portfolio(name="P", members=[edb.wind.WindTurbine(name="T1", capacity=3.0)]))

    assert client.list_series(client.get_node("P", "T1").get_raw()["uuid"]) == []


def test_an_invalid_owner_col_is_rejected(client):
    """``owner_col`` is interpolated into the SQL, so the allowlist is load-bearing
    rather than cosmetic. The check precedes any DB work, hence the throwaway uuid."""
    with pytest.raises(ValidationError, match="owner_col must be"):
        client.list_series(uuid4(), owner_col="node_uuid; DROP TABLE series")
