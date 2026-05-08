.. energydb documentation master file

Welcome to EnergyDB
===================

**EnergyDB** is an open-source library for persisting full energy portfolios — assets, grid topology, and bitemporal time series — in one connected database backed by PostgreSQL and ClickHouse.


What is EnergyDB?
-----------------

EnergyDB is a database for energy portfolios. It stores three things together in one connected system:

- 🌳 **An asset hierarchy** — your fleet modeled as a tree (Portfolio → Site → WindTurbine, Battery, …) of arbitrary depth.
- 🔗 **Grid topology** — typed edges (lines, transformers, pipes, interconnections) connecting any two assets, including across portfolios.
- ⏱️ **Bitemporal time series** — actuals and versioned forecasts attached to any node or edge, queryable as-of any point in time. (A separate ``change_time`` audit field tracks corrections without polluting the bitemporal model.)

Structure lives in PostgreSQL, values live in ClickHouse, and stable UUID identity lets Python objects round-trip to the database without losing any structural state.

It extends `TimeDB <https://github.com/rebase-energy/timedb>`_ with persistent storage for `EnergyDataModel <https://github.com/rebase-energy/EnergyDataModel>`_ hierarchies.


Why EnergyDB?
-------------

Most time-series systems are agnostic about what their series represent — they treat data as opaque ``(series_id, timestamp, value)`` triples. EnergyDB knows it is a portfolio, and links every series back to the asset or grid edge it describes.

- 🔁 **Round-trip persistence**: every ``Element`` keeps its UUID7 from in-memory object to row primary key — renames, moves, and property edits become silent ``UPDATE``\ s, never delete-then-insert.
- 📋 **Diffable structural changes**: ``dry_run=True`` previews every insert, rename, move, and delete before you apply — no surprise data loss on ``replace_subtree``.
- ⏱️ **Bitemporal queries**: forecast revisions, corrections, and time-of-knowledge backtests, powered by TimeDB.
- 🧭 **Lazy fluent navigation**: ``client.get_node("Portfolio", "Site", "T01").read(...)`` resolves to one indexed SQL query, regardless of subtree size.
- ⚖️ **Unit conversion at the boundary**: declare canonical units once; pint rescales every read and write automatically.


Quick Start
-----------

.. code-block:: bash

   pip install energydb

.. code-block:: python

   from datetime import UTC, datetime

   import energydb as edb
   import pandas as pd

   client = edb.Client()  # reads TIMEDB_PG_DSN / TIMEDB_CH_URL from env
   client.create()        # PostgreSQL schema + ClickHouse series_values table

   # 1. Declare a turbine with the series it will hold (metadata only).
   t01 = edb.wind.WindTurbine(
       name="T01", lat=55.01, lon=3.02, capacity=3.5, hub_height=80,
       timeseries=[
           edb.TimeSeries(name="power", unit="MW",
                          data_type=edb.DataType.ACTUAL),
       ],
   )

   # 2. Wrap it in a site and a portfolio.
   site = edb.Site(name="Offshore-1", lat=55.0, lon=3.0, members=[t01])
   portfolio = edb.Portfolio(name="my-portfolio", members=[site])

   # 3. Persist structure (nodes, edges, series declarations). Idempotent.
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
   development


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
