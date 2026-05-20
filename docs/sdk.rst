SDK Usage
=========

The energydb SDK is a single class — :class:`~energydb.Client` — that owns a
PostgreSQL connection pool and constructs a :class:`timedb.TimeDBClient` for
ClickHouse I/O. Around it sit two fluent scopes (:class:`~energydb.NodeScope`
and :class:`~energydb.EdgeScope`) that let you navigate the hierarchy and
operate on a single node or edge in idiomatic Python.

Overview
--------

energydb stores three kinds of objects, all in the same PostgreSQL ``energydb``
schema:

- **Nodes** — Portfolio, Site, WindTurbine, Battery, JunctionPoint, …
  Identified by a UUID7 generated when the ``Element`` is constructed in
  Python; that same UUID is the row primary key in Postgres.
- **Edges** — typed cross-tree links (Line, Link, Pipe, Interconnection)
  between two nodes, also UUID-keyed.
- **Series** — time series owned by exactly one node *or* edge. The
  catalog row lives in PostgreSQL; the values themselves live in TimeDB's
  ClickHouse ``series_values`` table.

Time series in energydb fall into two categories — a property of each
series:

- ``FLAT`` — actuals / measurements, one value per ``valid_time``
- ``OVERLAPPING`` — versioned forecasts, multiple ``knowledge_time`` per ``valid_time``

The same ``client.write`` / ``client.read`` pipeline handles both. OVERLAPPING
series additionally require a ``knowledge_time`` (kwarg or column) on every
write.


Getting Started
---------------

Import the package and instantiate the client:

.. code-block:: python

   import energydb as edb

   client = edb.Client()  # reads TIMEDB_PG_DSN / TIMEDB_CH_URL from env

The constructor accepts explicit ``pg_conninfo=`` and ``ch_url=`` kwargs for
custom connections; environment variables are the default.

energydb re-exports the EnergyDataModel public API under ``edb.*`` (see
``edb.wind``, ``edb.solar``, ``edb.battery``, ``edb.grid``, ``edb.Site``,
``edb.Portfolio``, …) and the TimeDB types
(:class:`~timedatamodel.TimeSeries`, :class:`~timedatamodel.DataType`,
:class:`~timedatamodel.TimeSeriesType`).


Database Connection
-------------------

The client reads its connection settings from environment variables by default:

- ``TIMEDB_PG_DSN`` (or ``DATABASE_URL``) — PostgreSQL DSN
- ``TIMEDB_CH_URL`` — ClickHouse HTTP URL

You can also use a ``.env`` file in your project root (see
:doc:`installation`).

For programmatic use, instantiate the client with explicit settings:

.. code-block:: python

   client = edb.Client(
       pg_conninfo="postgresql://user:pw@localhost:5432/energydb",
       ch_url="http://default:devpassword@localhost:8123/default",
   )


Schema Management
-----------------

Creating the Schema
~~~~~~~~~~~~~~~~~~~

Before using the client, create the database schema:

.. code-block:: python

   client.create()

This runs ``CREATE SCHEMA energydb`` and ``Base.metadata.create_all`` against
PostgreSQL, then delegates to TimeDB to create the ClickHouse
``series_values`` table. Safe to run repeatedly.

Deleting the Schema
~~~~~~~~~~~~~~~~~~~

To drop both databases (use with caution):

.. code-block:: python

   client.delete()

**WARNING**: ``DROP SCHEMA energydb CASCADE`` removes every node, edge, series
declaration, and run; ``td.delete()`` removes every value in ClickHouse.


Hierarchies and Topology
------------------------

Everything below covers writing, reading, and editing the portfolio
structure itself — nodes, edges, and series declarations. Time-series
I/O for the values that flow through that structure is covered in the
next section.

Building with ``register_tree``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The single entry point for **creating** structure — every node, edge, and
series declaration — is :meth:`~energydb.Client.register_tree`. It is
declarative and atomic: build the entire portfolio top-down as one nested
expression in Python, then persist it in one call.

.. code-block:: python

   import energydb as edb

   t01 = edb.wind.WindTurbine(
       name="T01", lat=55.01, lon=3.02, capacity=3.5, hub_height=80,
       timeseries=[
           edb.TimeSeries(name="power", unit="MW",
                          data_type=edb.DataType.ACTUAL),
           edb.TimeSeries(
               name="power", unit="MW",
               data_type=edb.DataType.FORECAST,
               timeseries_type=edb.TimeSeriesType.OVERLAPPING,
           ),
       ],
   )
   t02 = edb.wind.WindTurbine(name="T02", capacity=3.5, hub_height=80, timeseries=[
       edb.TimeSeries(name="power", unit="MW",
                      data_type=edb.DataType.ACTUAL),
   ])

   portfolio = edb.Portfolio(
       name="my-portfolio",
       members=[edb.Site(name="Offshore-1", lat=55.0, lon=3.0, members=[t01, t02])],
   )

   root_uuid = client.register_tree(portfolio)

Every ``Element`` got its ``id`` (UUID7) at construction; ``register_tree``
writes those UUIDs straight into the row primary keys in PostgreSQL.

.. note::

   ``register_tree`` is **create-only and structure-only**.

   * Any UUID in the payload that already exists in the DB raises
     ``ValueError`` — modify existing rows through scope mutators
     (:meth:`NodeScope.rename`, ``.update``, ``.delete``, ``.move_to``,
     ``.add``) instead, optionally batched in a :meth:`Client.transaction`.
   * Names must be non-empty and must not contain ``/`` (the path
     separator). Violations raise ``ValueError`` before any SQL runs and
     are also rejected by PostgreSQL ``CHECK`` constraints.
   * If any node/edge in the tree carries non-empty inline
     ``TimeSeries.df`` data, the call raises ``ValueError`` — write data
     separately via :meth:`~energydb.Client.write` or the scope helpers
     (see below).

Grafting onto an existing tree
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Pass ``under=`` to attach the new tree's root under an existing parent.
The parent path (or uuid) must resolve to an existing node:

.. code-block:: python

   # Add a new site under an existing portfolio.
   client.register_tree(
       edb.Site(name="Offshore-2", lat=55.5, lon=3.5, members=[t03]),
       under=("my-portfolio",),
   )

Dry run
^^^^^^^

Pass ``dry_run=True`` to preview the diff before applying. The call
returns a :class:`~energydb.TreeDiff` and rolls back — no DB state
changes.

.. code-block:: python

   diff = client.register_tree(portfolio, dry_run=True)
   diff.render()

   # Looks good — apply.
   root_uuid = client.register_tree(portfolio)

The diff carries flat ``node_changes`` / ``edge_changes`` lists and
binned views (``node_inserts``, ``edge_inserts``).

``TreeDiff.render()`` renders a tree-shaped textual preview::

   Portfolio 'my-portfolio'
   ├── + Site 'Offshore-1'                         [insert]
   │   ├── + WindTurbine 'T01'                     [insert]
   │   └── + WindTurbine 'T02'                     [insert]
   edges:
     + Line 'Cable-1' <a-uuid> → <b-uuid>          [insert]


Reconstructing trees
~~~~~~~~~~~~~~~~~~~~

Pull a node or a whole subtree back as a regular EnergyDataModel object —
same UUIDs, ready for inspection or in-memory edits.

.. code-block:: python

   # Single node, eager
   turbine = client.get_node("my-portfolio", "Offshore-1", "T01").get()
   turbine = client.get_node(uuid=t01.id).get()

   # Full subtree as an EDM tree
   tree = client.get_tree("my-portfolio")
   tree_with_series = client.get_tree("my-portfolio", include_series=True)

With ``include_series=True``, every reconstructed node carries its registered
series as metadata-only :class:`~timedatamodel.TimeSeries` entries (``df=None``)
on ``timeseries``.

Flat queries by type / subtree / properties:

.. code-block:: python

   turbines = client.query_nodes(type="WindTurbine", within="my-portfolio")
   lines = client.query_edges(type="Line", within="my-portfolio")


Editing single elements
~~~~~~~~~~~~~~~~~~~~~~~

With UUID identity, mutations resolve in one statement at the database layer:

- **Rename** (same uuid, different ``name``) → silent ``UPDATE``
- **Move** (same uuid, different ``parent_uuid``) → silent ``UPDATE``
- **Property edit** (same uuid, different ``data``) → silent ``UPDATE``
- **Type change** (same uuid, different ``node_type``) → rejected; element
  type is immutable for a given id

Address the node by path or uuid and use the fluent scope ops:

.. code-block:: python

   t01 = client.get_node("my-portfolio", "Offshore-1", "T01")
   t01.rename("T01-A")
   t01.update(data={"capacity": 4.5})
   t01.move_to(client.get_node("my-portfolio", "Onshore-1"))
   t01.delete()

``move_to`` rejects re-parenting into self or any descendant (cycle
detection). ``rename`` and ``update`` are idempotent and round-trip safe.

The same surface exists on edges:

.. code-block:: python

   e = client.get_edge(uuid=line.id)
   e.update(data={"capacity": 600})
   e.delete()


Declaring series
~~~~~~~~~~~~~~~~

The recommended way to register series is to declare them as metadata-only
:class:`~timedatamodel.TimeSeries` entries on each ``Element`` and let
``register_tree`` persist them with the rest of the structure.

For surgical additions on an existing node or edge, scopes expose
``register_series``:

.. code-block:: python

   client.get_node("my-portfolio", "Offshore-1", "T01").register_series(
       name="wind_speed",
       canonical_unit="m/s",
       data_type="actual",
       timeseries_type="FLAT",
   )

Or pass a metadata-only TimeSeries directly:

.. code-block:: python

   ts = edb.TimeSeries(
       name="wind_speed", unit="m/s", data_type=edb.DataType.ACTUAL,
   )
   client.get_node(uuid=t01.id).register_series(ts)

``retention``, ``canonical_unit``, and the owner columns are immutable after
insert (enforced by a Postgres trigger). Reclassifying a series means
registering a new one — this preserves ClickHouse-side data integrity. When
``retention`` is omitted it is derived from ``timeseries_type``: ``FLAT``
(actuals) → ``forever``, ``OVERLAPPING`` (forecasts) → ``medium``.


Edges and Grid Topology
~~~~~~~~~~~~~~~~~~~~~~~

Edges model typed cross-tree links — lines, transformers, pipes,
interconnections. They have full CRUD, can carry their own time series, and
support endpoint navigation.

.. code-block:: python

   bus_a = edb.grid.JunctionPoint(name="BusA")
   bus_b = edb.grid.JunctionPoint(name="BusB")
   line = edb.grid.Line(
       name="Cable-1", capacity=500,
       from_element=bus_a, to_element=bus_b,
   )

   # Persist topology in one call — register_tree handles nodes then edges.
   client.register_tree(edb.Portfolio(name="Grid", members=[bus_a, bus_b, line]))

For standalone edges between nodes that already exist in the database, use
:meth:`~energydb.Client.create_edge` directly:

.. code-block:: python

   client.create_edge(line)

Look an edge up by uuid or by the ``(from_path, to_path, type)`` triple:

.. code-block:: python

   e = client.get_edge(uuid=line.id).get()
   e = client.get_edge(("Grid", "BusA"), ("Grid", "BusB"), type="Line").get()

Series on an edge:

.. code-block:: python

   scope = client.get_edge(uuid=line.id)
   scope.register_series(
       name="power_flow", canonical_unit="MW",
       data_type="actual", timeseries_type="FLAT",
   )
   scope.write(df, name="power_flow", data_type="actual")
   scope.read(name="power_flow", data_type="actual")


Targeted Time-Series I/O
------------------------

Use :meth:`NodeScope.write <energydb.NodeScope.write>` /
:meth:`EdgeScope.write <energydb.EdgeScope.write>` when you have a single
known series — patching a bad segment, backfilling a gap, exploring data
interactively, or driving a small ETL. The series is resolved exactly once
via the scope's path or uuid before any data reaches the database.

.. list-table::
   :header-rows: 1
   :widths: 28 36 36

   * -
     - Targeted I/O (scope ``write`` / ``read``)
     - Bulk I/O (``client.write`` / ``client.read``)
   * - **Targets**
     - One series at a time
     - Many series across many nodes / edges
   * - **Typical use**
     - Patching, backfilling, exploration
     - ETL pipelines, scheduled loads, cross-portfolio reads
   * - **Routing**
     - Implicit (the scope's resolved uuid)
     - Manifest column: ``node_uuid``, ``edge_uuid``, or ``path``

Writing
~~~~~~~

Build a small DataFrame with ``valid_time`` and ``value`` (and optionally
``knowledge_time`` for OVERLAPPING series), then write it through the
scope:

.. code-block:: python

   from datetime import UTC, datetime
   import pandas as pd

   start = datetime(2026, 1, 1, tzinfo=UTC)
   df = pd.DataFrame({
       "valid_time": pd.date_range(start, periods=24, freq="1h", tz="UTC"),
       "value": [2.5 + 0.1 * h for h in range(24)],
   })

   client.get_node("my-portfolio", "Offshore-1", "T01").write(
       df, data_type="actual", name="power",
   )

A pandas or polars DataFrame is accepted; everything is converted to polars
internally. ``write()`` returns the ``run_id`` used for this batch — all
writes are recorded in the ``energydb.runs`` table, keyed by a client-side
UUID7-derived integer.

Optional kwargs:

- ``unit`` — declare the incoming unit; if it differs from the series'
  registered ``canonical_unit``, pint computes the scalar factor and
  rescales every value before writing
- ``knowledge_time`` — broadcast a single ``knowledge_time`` (required for
  OVERLAPPING series unless a ``knowledge_time`` column is on the DataFrame)
- ``run_id``, ``workflow_id``, ``model_name``, ``run_start_time``,
  ``run_finish_time``, ``run_params`` — provenance metadata stored in
  ``energydb.runs``

Reading
~~~~~~~

A scope's ``.read()`` reads every series that matches under the resolved
subtree. Pass ``data_type=`` and ``name=`` to narrow:

.. code-block:: python

   # Single-series read
   df = client.get_node("my-portfolio", "Offshore-1", "T01").read(
       data_type="actual", name="power", start_valid=start,
   )

   # Subtree read — every actual 'power' across the whole portfolio
   df = client.get_node("my-portfolio").read(data_type="actual", name="power")

   # Filter descendants by EDM type
   df = client.get_node("my-portfolio").where(type="WindTurbine").read(
       data_type="actual", name="power",
   )

The read returns a polars DataFrame by default; pass ``backend="pandas"`` for
pandas. Default columns are ``path`` (``Utf8``, joined with ``/``),
``data_type``, ``name``, ``valid_time``, ``value``. ``knowledge_time`` and
``change_time`` appear when the corresponding ``include_*`` kwargs are
set. Internal identifiers (``series_id``, ``node_uuid``, ``edge_uuid``)
are never exposed on the result.

For edge reads, the hierarchy columns are ``from_path``, ``to_path`` (both
``Utf8``, joined with ``/``) and ``edge_type``.

.. note::

   **Scope auto-strip.** When a scope ``.read()`` resolves to a single
   series (e.g. fully qualified path + ``data_type=`` + ``name=``) and
   ``output="frame"``, the path/data_type/name columns are stripped —
   you only get the data columns (``valid_time``, ``value``, plus opt-in
   ``knowledge_time`` / ``change_time``). The caller already knows the
   identity through the scope expression; re-broadcasting it on every row
   is pure noise.

Time-range filters mirror TimeDB:

.. code-block:: python

   df = scope.read(
       data_type="actual", name="power",
       start_valid=datetime(2026, 1, 1, tzinfo=UTC),
       end_valid=datetime(2026, 2, 1, tzinfo=UTC),
       start_known=datetime(2026, 1, 1, tzinfo=UTC),  # OVERLAPPING only
       end_known=datetime(2026, 1, 15, tzinfo=UTC),
       include_updates=False,                          # correction chain off
       include_knowledge_time=False,                   # collapse to latest
   )

Per-window cutoffs (for backtesting / day-ahead simulation) are exposed via
:meth:`NodeScope.read_relative <energydb.NodeScope.read_relative>` — same
window-length / issue-offset / daily-shorthand semantics as
:meth:`timedb.TimeDBClient.read_relative`.


Bulk Manifest I/O
-----------------

For production pipelines that touch many series across many nodes or edges
in one call, use :meth:`Client.write <energydb.Client.write>` and
:meth:`Client.read <energydb.Client.read>` with a *manifest DataFrame*. The
same engine drives the scope helpers, so guarantees are identical.

The manifest carries one routing column plus ``data_type``, ``name``, and
the data columns. The routing column is autodetected from the column names
— exactly one of:

- ``node_uuid`` — programmatic routing by UUID
- ``edge_uuid`` — programmatic routing for edge-attached series
- ``path`` — human-readable, ``Utf8`` joined with ``/``
  (e.g. ``"my-portfolio/Offshore-1/T01"``). ``/`` is reserved as the
  separator; names containing ``/`` are rejected at registration. The
  manifest must use ``Utf8`` — ``List(Utf8)`` from earlier API versions
  is rejected with an explicit migration message.

write() — long-format multi-series ingestion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import polars as pl
   from datetime import UTC, datetime, timedelta

   base = datetime(2026, 1, 1, tzinfo=UTC)
   hours = [base + timedelta(hours=h) for h in range(24)]

   manifest = pl.DataFrame({
       "path":       ["my-portfolio/Offshore-1/T01"] * 24,
       "data_type":  ["actual"] * 24,
       "name":       ["power"] * 24,
       "valid_time": hours,
       "value":      [2.5 + 0.1 * h for h in range(24)],
   })
   client.write(manifest)

The other two routing forms are equivalent:

.. code-block:: python

   # By node uuid (programmatic)
   pl.DataFrame({
       "node_uuid":  [str(t01.id)] * 24,
       "data_type":  ["actual"] * 24,
       "name":       ["power"] * 24,
       "valid_time": hours,
       "value":      [2.5 + 0.1 * h for h in range(24)],
   })

   # By edge uuid (for edge-attached series)
   pl.DataFrame({
       "edge_uuid":  [str(line.id)] * 24,
       "data_type":  ["actual"] * 24,
       "name":       ["power_flow"] * 24,
       "valid_time": hours,
       "value":      [200.0 + h for h in range(24)],
   })

Routing modes are mutually exclusive — passing more than one routing column
raises ``ValueError``.

Series must already be registered (typically via
:meth:`~energydb.Client.register_tree`) — unresolved
``(owner, data_type, name)`` triples raise before any data reaches
ClickHouse.

Optional columns and kwargs:

- ``unit`` (column or kwarg) — incoming unit, auto-converted to each series'
  canonical unit; mutually exclusive when both forms are supplied
- ``knowledge_time`` (column or kwarg) — required for OVERLAPPING series
- ``run_id``, ``workflow_id``, ``model_name``, ``run_start_time``,
  ``run_finish_time``, ``run_params`` — provenance metadata; default
  ``run_id`` is one client-generated UUID7-derived integer per call

Returns the ``run_id`` used.

read() — manifest-based multi-series read
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The read manifest is the same shape, minus the data columns:

.. code-block:: python

   manifest = pl.DataFrame([
       {"path": "my-portfolio/Offshore-1/T01", "data_type": "actual", "name": "power"},
       {"path": "my-portfolio/Offshore-1/T02", "data_type": "actual", "name": "power"},
   ])
   df = client.read(
       manifest,
       start_valid=datetime(2026, 1, 1, tzinfo=UTC),
       end_valid=datetime(2026, 2, 1, tzinfo=UTC),
   )

Returns a polars DataFrame by default; pass ``backend="pandas"`` for pandas.

Optional kwargs:

- ``unit`` — request a specific unit; per-series scalar factor applied
- ``start_valid`` / ``end_valid`` — valid_time range (UTC)
- ``start_known`` / ``end_known`` — knowledge_time range (OVERLAPPING only)
- ``include_updates`` — expose correction chain
- ``include_knowledge_time`` — return one row per (knowledge_time, valid_time)

The result columns mirror the scope read: ``path``, ``data_type``,
``name``, ``valid_time``, ``value`` for node manifests; ``from_path``,
``to_path``, ``edge_type``, ``data_type``, ``name``, ``valid_time``,
``value`` for edge manifests. ``path`` / ``from_path`` / ``to_path`` are
``Utf8`` joined with ``/``. Internal identifiers (``series_id``,
``node_uuid``, ``edge_uuid``) are never on the result.

Per-window relative reads use :meth:`Client.read_relative
<energydb.Client.read_relative>`, with the same parameters as TimeDB's
:meth:`~timedb.TimeDBClient.read_relative`.

Output modes
~~~~~~~~~~~~

Both :meth:`Client.read <energydb.Client.read>` and the scope reads accept
an ``output=`` kwarg that controls return shape:

- ``output="frame"`` (default) — one DataFrame with the identity columns
  broadcast on every row. Good for ETL and ad-hoc analysis where you want
  to ``group_by(path)`` or filter further downstream.
- ``output="by_path"`` — a ``dict`` keyed by ``(path, data_type, name)``
  (or ``(from_path, to_path, edge_type, data_type, name)`` for edge
  reads), one DataFrame per series. Sub-frames carry only the data
  columns (``valid_time``, ``value``, plus opt-in ``knowledge_time`` /
  ``change_time``). Each sub-frame is sorted by ``valid_time`` ascending.

Use ``by_path`` when downstream code naturally operates per-series — model
training, plotting, per-asset writes back. Series that resolve but have
no rows in ClickHouse still appear as keys with an empty sub-frame, so
callers can index by key without ``KeyError``.

.. code-block:: python

   by_series = client.read(manifest, output="by_path")
   for key, sub in by_series.items():
       path, data_type, name = key
       train_one_model(path, sub)

The ``backend=`` kwarg is orthogonal: ``backend="polars"`` (default)
returns polars frames in both modes; ``backend="pandas"`` converts every
frame at the boundary.


Atomic batches with ``transaction()``
-------------------------------------

For a sequence of mutations that must apply (or roll back) as a unit, open
a :meth:`Client.transaction`. Mutations executed through the txn's scope
factories share one borrowed pool connection and stay uncommitted until
:meth:`Transaction.commit` is called explicitly. Exiting the
``with``-block without committing raises and rolls back.

.. code-block:: python

   with client.transaction() as txn:
       txn.get_node("my-portfolio", "Offshore-1", "T01").update({"hub_height": 95})
       txn.get_node("my-portfolio", "Offshore-1", "T02").rename("T02b")
       txn.get_node("my-portfolio", "Rooftop-1", "B01").move_to(
           ("my-portfolio", "Offshore-1")
       )
       txn.preview().render()  # aggregate diff of everything queued so far
       txn.commit()

The transaction supports every scope mutator (``rename``, ``update``,
``move_to``, ``delete``, ``add``, ``register_series``) plus
:meth:`Transaction.register_tree` for create-only inserts. Mid-transaction
reads on the same connection see the transaction's own uncommitted writes.

.. warning::

   **Time-series I/O does not participate in the PG transaction.**
   ``scope.write(df, ...)``, ``scope.read(...)``, and
   ``scope.read_relative(...)`` on a txn-bound scope raise
   ``RuntimeError`` — the ClickHouse writes and the ``energydb.runs``
   inserts go through their own connection and would not roll back with
   the PG transaction. Call :meth:`Client.write` / :meth:`Client.read`
   directly outside the ``with``-block when you need to mix structure
   mutations and time-series I/O.


Resolve cache
-------------

Every :class:`Client` keeps an in-process cache (the *series registry*)
of resolved series, node, and edge metadata. The cache is read-through
on the resolve hot path and write-through on every local mutation —
``register_tree``, ``register_series``, ``rename``, ``move_to``,
``delete``, etc. all keep it consistent transparently. There is nothing
to invalidate by hand under normal use.

The one case the cache cannot observe is **another process** mutating
schema state this client has already cached — registering a new series,
deleting a node, or flipping a series's ``timeseries_type``. Reads from
the cached client will continue to serve stale metadata until you call:

.. code-block:: python

   client.invalidate_series_cache()                 # clear everything
   client.invalidate_series_cache(owner_uuid=...)   # evict one owner only

A focused eviction by ``owner_uuid`` is cheaper than a full clear when
you know exactly which node/edge changed.

Use :meth:`Client.resolve_cache_stats` to see hit/miss counters and the
current cache size if you are tuning read latency.


Run History
-----------

Every write creates one run row in ``energydb.runs``. To list runs that wrote
data for a given series:

.. code-block:: python

   runs = client.read_runs_for_series(series_id=42)
   # [
   #   {"run_id": 123..., "workflow_id": "nightly-forecast",
   #    "model_name": "ECMWF",
   #    "run_start_time": ..., "run_finish_time": ...,
   #    "run_params": {"horizon": 48}, "inserted_at": ...},
   #   ...
   # ]

Run ids are client-side BIGINTs (top 63 bits of a UUID7), time-sortable, and
fit cleanly in ``Int64``.


Error Handling
--------------

Common errors and how to handle them:

.. code-block:: python

   from energydb import IncompatibleUnitError

   try:
       client.register_tree(portfolio)
   except ValueError as e:
       if "contains '/'" in str(e) or "must be non-empty" in str(e):
           # Illegal node/edge/series name.
           ...
       else:
           raise

   try:
       client.write(manifest)
   except ValueError as e:
       if "Series not registered" in str(e):
           # Register series via register_tree first.
           ...
       elif "knowledge_time is required for OVERLAPPING" in str(e):
           ...
       else:
           raise
   except IncompatibleUnitError as e:
       print(f"Unit mismatch: {e}")

Key exceptions:

- ``ValueError`` — empty or ``/``-containing names, missing routing
  columns, unresolved series, missing ``knowledge_time`` for OVERLAPPING,
  illegal type changes, cycle-creating ``move_to``, ``List(Utf8)``
  manifest paths (use ``Utf8`` joined with ``/``), uuid-already-exists on
  ``register_tree``
- ``RuntimeError`` — time-series ``read`` / ``write`` /
  ``read_relative`` on a txn-bound scope (call them outside the
  ``with``-block)
- :class:`~energydb.IncompatibleUnitError` — unit conversion failed due to
  dimensionality mismatch


Best Practices
--------------

1. **Always use timezone-aware UTC datetimes.** Naive timestamps raise.

   .. code-block:: python

      from datetime import UTC, datetime
      good = datetime(2026, 1, 1, 12, tzinfo=UTC)

2. **Declare series with the tree, not later.** Inline metadata-only
   :class:`~timedatamodel.TimeSeries` entries on every ``Element``;
   ``register_tree`` registers them in the same transaction. Use
   ``scope.register_series`` only for surgical additions.

3. **Use the imperative scope ops for one-off edits.** UUID identity makes
   ``rename``, ``update``, ``move_to``, and ``delete`` silent ``UPDATE``\ s —
   no delete-then-insert dance, no full tree round-trip.

4. **Batch related mutations in a transaction.** Use
   :meth:`Client.transaction` so a sequence of ``rename`` / ``update`` /
   ``move_to`` / ``delete`` / ``add`` / ``register_tree`` calls either
   all apply together or all roll back. Time-series I/O does not
   participate — call :meth:`Client.write` / :meth:`Client.read`
   outside the ``with``-block.

5. **Pick a routing column per pipeline.** Mixing ``path`` and ``node_uuid``
   in the same manifest raises. Use ``path`` for human-readable ETL,
   ``node_uuid`` / ``edge_uuid`` once you have the ids. ``path`` values
   are ``Utf8`` joined with ``/`` (e.g. ``"my-portfolio/Offshore-1/T01"``).

6. **Use ``output="by_path"`` when downstream code is per-series.**
   Training one model per asset, plotting per-series, or computing
   per-series statistics is cleaner against the keyed dict than against
   one long-format frame.

7. **Tag writes with ``workflow_id`` / ``model_name``.** Provenance lives in
   ``energydb.runs`` and is recoverable via
   :meth:`~energydb.Client.read_runs_for_series`.

8. **Use ``where(type=...)`` for type-filtered subtree reads.** A single
   fluent call replaces N targeted reads — the manifest pipeline batches
   resolution and the join in one round-trip.

9. **Call ``invalidate_series_cache()`` only after a cross-process
   schema change.** In-process registrations and deletions are tracked
   automatically.


Complete Example
----------------

A complete workflow from setup to analysis:

.. code-block:: python

   from datetime import UTC, datetime, timedelta
   import energydb as edb
   import pandas as pd
   import polars as pl

   client = edb.Client()
   client.delete()
   client.create()

   # 1. Declare the portfolio (asset hierarchy + series declarations).
   t01 = edb.wind.WindTurbine(
       name="T01", lat=55.01, lon=3.02, capacity=3.5, hub_height=80,
       timeseries=[
           edb.TimeSeries(name="power", unit="MW",
                          data_type=edb.DataType.ACTUAL),
           edb.TimeSeries(
               name="power", unit="MW",
               data_type=edb.DataType.FORECAST,
               timeseries_type=edb.TimeSeriesType.OVERLAPPING,
           ),
       ],
   )
   t02 = edb.wind.WindTurbine(name="T02", capacity=3.5, timeseries=[
       edb.TimeSeries(name="power", unit="MW",
                      data_type=edb.DataType.ACTUAL),
   ])
   site = edb.Site(name="Offshore-1", lat=55.0, lon=3.0, members=[t01, t02])
   portfolio = edb.Portfolio(name="my-portfolio", members=[site])

   # 2. Persist structure (idempotent).
   client.register_tree(portfolio)

   # 3. Targeted write — actual power for T01.
   start = datetime(2026, 1, 1, tzinfo=UTC)
   hours = pd.date_range(start, periods=24, freq="1h", tz="UTC")
   df = pd.DataFrame({"valid_time": hours, "value": [2.5 + 0.05 * i for i in range(24)]})
   client.get_node("my-portfolio", "Offshore-1", "T01").write(
       df, data_type="actual", name="power",
   )

   # 4. Bulk write — actual power for both turbines via a manifest.
   long_df = pl.DataFrame({
       "path":       ["my-portfolio/Offshore-1/T01"] * 24
                   + ["my-portfolio/Offshore-1/T02"] * 24,
       "data_type":  ["actual"] * 48,
       "name":       ["power"] * 48,
       "valid_time": list(hours) * 2,
       "value":      [2.5 + 0.05 * i for i in range(24)]
                   + [2.7 + 0.05 * i for i in range(24)],
   })
   run_id = client.write(long_df)
   print(f"wrote run_id={run_id}")

   # 5. Subtree read — every actual 'power' across the portfolio.
   result = client.get_node("my-portfolio").read(
       data_type="actual", name="power", start_valid=start,
   )
   print(result.head())

   # 5b. Same read, partitioned per-series for downstream loops.
   by_series = client.get_node("my-portfolio").read(
       data_type="actual", name="power", output="by_path",
   )
   for (path, dt, name), sub in by_series.items():
       print(f"{path}: {len(sub)} rows")

   # 6. Surgical edits — batched in a transaction for atomicity.
   with client.transaction() as txn:
       txn.get_node("my-portfolio", "Offshore-1").rename("Offshore-Renamed")
       txn.get_node("my-portfolio", "Offshore-Renamed", "T01").update(
           {"capacity": 4.0},
       )
       txn.get_node("my-portfolio", "Offshore-Renamed", "T02").delete()
       txn.commit()

   # 7. Cleanup.
   client.delete()
