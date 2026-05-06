.. energydb documentation master file

Welcome to EnergyDB
===================

**EnergyDB** is an open-source library for persisting full energy portfolios — assets, grid topology, and bitemporal time series — in one connected database backed by PostgreSQL and ClickHouse.

It extends `TimeDB <https://github.com/rebase-energy/timedb>`_ with persistent storage for `EnergyDataModel <https://github.com/rebase-energy/EnergyDataModel>`_ hierarchies, links every node and edge to its time series with stable UUID identity, and lets you round-trip a portfolio between Python and Postgres without losing any structural state.


Why EnergyDB?
-------------

Most time-series systems are agnostic about what their series represent — they treat data as an opaque ``(series_id, timestamp, value)`` triple. EnergyDB knows it is a portfolio: assets, sites, and grid topology, with the bitemporal series that describe them living alongside.

- 🌳 **Asset hierarchies**: declare your portfolio in Python (Portfolio → Site → asset) and persist arbitrary depth in one call
- 🔗 **Grid topology**: typed edges (Line, Link, Pipe) connect any two nodes and can carry their own time series
- 🔁 **Round-trip persistence**: every ``Element`` keeps its UUID7 from in-memory object to row PK — renames, moves, and property edits become silent ``UPDATE``\ s
- ⏱️ **Bitemporal series**: forecast revisions, corrections, and time-of-knowledge queries powered by TimeDB
- 🧭 **Fluent, lazy navigation**: ``client.get_node("Portfolio", "Site", "T01").read(...)`` resolves to one indexed CTE


Quick Start
-----------

.. code-block:: bash

   pip install energydb

.. code-block:: python

   from datetime import UTC, datetime

   import energydb as edb
   import pandas as pd

   client = edb.Client()  # reads TIMEDB_PG_DSN / TIMEDB_CH_URL from env
   client.create()        # PG schema + CH series_values table

   # 1. Declare a turbine with the series it will hold (descriptors only).
   t01 = edb.wind.WindTurbine(
       name="T01", lat=55.01, lon=3.02, capacity=3.5, hub_height=80,
       timeseries=[
           edb.TimeSeriesDescriptor(name="power", unit="MW",
                                    data_type=edb.DataType.ACTUAL),
       ],
   )

   # 2. Wrap it in a site and a portfolio.
   site = edb.Site(name="Offshore-1", lat=55.0, lon=3.0, members=[t01])
   portfolio = edb.Portfolio(name="my-portfolio", members=[site])

   # 3. Persist structure (nodes, edges, descriptors). Idempotent.
   client.register_tree(portfolio)

   # 4. Write a day of hourly values for the turbine's power series.
   start = datetime(2026, 1, 1, tzinfo=UTC)
   df = pd.DataFrame({
       "valid_time": pd.date_range(start, periods=24, freq="1h", tz="UTC"),
       "value": [2.5 + 0.05 * i for i in range(24)],
   })
   client.get_node("my-portfolio", "Offshore-1", "T01").write(
       df, name="power", data_type="actual",
   )

   # 5. Read back — single asset.
   client.get_node("my-portfolio", "Offshore-1", "T01").read(
       name="power", data_type="actual",
   )

   # Or across the whole portfolio in one fluent call.
   client.get_node("my-portfolio").read(name="power", data_type="actual")


Release Notes
-------------

For version-by-version changes and migration notes, see:

- `Changelog <https://github.com/rebase-energy/energydb/blob/main/CHANGELOG.md>`_


Documentation
-------------

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   installation
   sdk
   reference
   examples


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
