SDK Usage
=========

The energydb SDK is built around one client that owns a PostgreSQL connection
pool and constructs a :class:`timedb.TimeDBClient` for ClickHouse I/O. It ships
in two flavors:

- :class:`~energydb.Client` — the synchronous facade. Every method blocks; the
  connection pool is opened eagerly on construction. This is what the examples
  below use.
- :class:`~energydb.AsyncClient` — the ``async``/``await`` client the sync
  facade wraps. ``await client.open()`` once before use (or use it as an async
  context manager). Every ``Client`` method shown here has an identical
  ``AsyncClient`` coroutine.

Around either client sit two fluent scopes (:class:`~energydb.NodeScope` and
:class:`~energydb.EdgeScope`) that let you navigate the hierarchy and operate
on a single node or edge in idiomatic Python.

Overview
--------

energydb stores three kinds of objects, all in one PostgreSQL schema — named by
the ``ENERGYDB_SCHEMA`` environment variable, defaulting to ``public``:

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
custom connections; environment variables are the default. Both are
keyword-only. The synchronous ``Client`` opens its pool on construction and
should be closed with ``client.close()`` (or used as a ``with`` block); the
:class:`~energydb.AsyncClient` requires ``await client.open()`` before its
first call.

energydb re-exports the EnergyDataModel public API under ``edb.*`` (see
``edb.wind``, ``edb.solar``, ``edb.battery``, ``edb.grid``, ``edb.Site``,
``edb.Portfolio``, …) and the TimeDB types
(:class:`~timedatamodel.TimeSeries`, :class:`~timedatamodel.DataType`,
:class:`~timedatamodel.TimeSeriesType`).


Database Connection
-------------------

The client reads its connection settings from environment variables by default:

- ``TIMEDB_PG_DSN`` (or ``DATABASE_URL``) — PostgreSQL DSN (must be a URI)
- ``TIMEDB_CH_URL`` — ClickHouse HTTP URL
- ``ENERGYDB_SCHEMA`` — PostgreSQL schema for energydb's tables (default
  ``public``)

You can also use a ``.env`` file in your project root (see
:doc:`installation`). The ClickHouse client honors TimeDB's own
``TIMEDB_CH_TIMEOUT`` / ``TIMEDB_CH_CONNECT_TIMEOUT`` tunables.

For programmatic use, instantiate the client with explicit settings:

.. code-block:: python

   client = edb.Client(
       pg_conninfo="postgresql://user:pw@localhost:5432/energydb",
       ch_url="http://default:devpassword@localhost:8123/default",
   )

Schema resolution
~~~~~~~~~~~~~~~~~

energydb never sets, reads, or depends on the connection's ``search_path``.
Every statement it issues names its relations in the SQL text itself:
``energydb.node`` under ``ENERGYDB_SCHEMA=energydb``, and plain ``node`` under
the default ``public`` — where it resolves alongside the host application's own
unqualified tables through the server's own default search path.

Resolution is therefore a property of the query, not of per-connection session
state, which is what makes energydb correct behind a **transaction-mode
connection pooler** (PgBouncer, Neon's pooled ``-pooler`` endpoints). Such a
pooler may serve each transaction from a different server connection, and
rejects the libpq ``options`` startup parameter outright — so any scheme that
carries the schema in session state either fails to connect or resolves against
state some other client set. Point energydb at a pooled endpoint or a direct
one; both behave identically.

``SHOW search_path`` reports whatever your server, role, or proxy configures.
energydb does not change it and does not depend on what it says.


Schema Management
-----------------

Creating the Schema
~~~~~~~~~~~~~~~~~~~

Before using the client, create the database schema:

.. code-block:: python

   client.create()

This runs ``Base.metadata.create_all`` against PostgreSQL — creating the
``node``, ``edge``, ``series``, and ``runs`` tables plus the ``series_meta``
view — then delegates to TimeDB to create the ClickHouse ``series_values`` and
``run_series`` tables. When ``ENERGYDB_SCHEMA`` names a non-default schema, it
also issues ``CREATE SCHEMA IF NOT EXISTS`` first; the default ``public``
schema is used as-is. Finally it *best-effort* provisions the ClickHouse
``PostgreSQL()`` meta-engine table used by the concurrent read path — a failure
there is logged, not raised (reads fall back to the sequential path). Safe to
run repeatedly.

Use :meth:`~energydb.Client.setup_ch_meta_engine` as the explicit, *raising*
alternative that (re)creates the ``series_meta`` view and the engine table and
clears the session's engine-degraded flag — call it to re-enable ``concurrent``
reads after fixing engine infrastructure.

Deleting the Schema
~~~~~~~~~~~~~~~~~~~

To drop energydb's tables and all ClickHouse values (use with caution):

.. code-block:: python

   client.delete()

**WARNING**: this is destructive. With a named ``ENERGYDB_SCHEMA`` it runs
``DROP SCHEMA … CASCADE``, removing every node, edge, series declaration, and
run. With the default ``public`` schema it drops only energydb's own four
tables (``series``, ``runs``, ``edge``, ``node``) — never the shared ``public``
schema, which would take the host application's tables with it. Either way it
then drops every value in ClickHouse.


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
     :class:`~energydb.errors.AlreadyExistsError` — modify existing rows
     through scope mutators (:meth:`NodeScope.rename`, ``.update``,
     ``.delete``, ``.move_to``, ``.add``) instead, optionally batched in a
     :meth:`Client.transaction`.
   * Names must be non-empty and must not contain ``/`` (the path
     separator). Violations raise
     :class:`~energydb.errors.ValidationError` before any SQL runs and
     are also rejected by PostgreSQL ``CHECK`` constraints.
   * If any node/edge in the tree carries non-empty inline
     ``TimeSeries.df`` data, the call raises
     :class:`~energydb.errors.ValidationError` — write data separately via
     :meth:`~energydb.Client.write` or the scope helpers (see below).

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

When you want the raw row rather than a reconstructed EDM object — any
``node_type`` / ``edge_type`` string, no class lookup — use ``get_raw()`` and
the lazy navigation helpers on a scope:

.. code-block:: python

   scope = client.get_node("my-portfolio", "Offshore-1", "T01")

   scope.get_raw()      # {uuid, node_type, name, data, parent_uuid, path} — None if uuid-addressed & missing
   scope.children()     # direct children, one level: list of {uuid, node_type, name, data, parent_uuid}
   scope.descendants()  # whole subtree below (excludes self), same dict shape
   scope.path()         # resolved path tuple

   # children / descendants accept a type= filter
   turbines = client.get_node("my-portfolio").descendants(type="WindTurbine")

``EdgeScope.get_raw()`` returns ``{uuid, edge_type, name, data,
from_node_uuid, to_node_uuid}`` and works for any ``edge_type`` string;
``EdgeScope.from_node()`` / ``.to_node()`` return the endpoint
:class:`~energydb.NodeScope`\ s.

Flat queries by type / subtree / properties (return lists of EDM objects):

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

``update`` defaults to a **shallow JSONB merge** — the passed keys are merged
over the existing ``data``. Pass ``replace_data=True`` to overwrite ``data``
wholesale instead. ``move_to`` rejects re-parenting into self or any descendant
(cycle detection). ``rename`` and ``update`` are idempotent and round-trip
safe. Every mutator also accepts ``dry_run=True``, returning a
:class:`~energydb.TreeDiff` without touching the database.

The same surface exists on edges (edge ``move_to`` takes keyword-only
``from_node=`` / ``to_node=`` endpoints):

.. code-block:: python

   e = client.get_edge(uuid=line.id)
   e.update(data={"capacity": 600})          # shallow merge; replace_data=True to overwrite
   e.move_to(from_node=bus_a_scope, to_node=bus_c_scope)
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
       retention="forever",   # optional; derived from timeseries_type when omitted
       description="Nacelle anemometer",  # optional
   )

``register_series`` returns the integer ``series_id``. Pass a metadata-only
TimeSeries directly instead of the individual fields:

.. code-block:: python

   ts = edb.TimeSeries(
       name="wind_speed", unit="m/s", data_type=edb.DataType.ACTUAL,
   )
   client.get_node(uuid=t01.id).register_series(ts)

``retention``, ``canonical_unit``, and the owner columns are immutable after
insert — ``register_series`` rejects a conflicting re-registration with
:class:`~energydb.errors.AlreadyExistsError` (enforced in Python, so the
schema stays fully Alembic-autogeneratable). Reclassifying a series means
registering a new one — this preserves ClickHouse-side data integrity. When
``retention`` is omitted it is derived from ``timeseries_type``: ``FLAT``
(actuals) → ``forever``, ``OVERLAPPING`` (forecasts) → ``medium``.

**``data_type`` and ``timeseries_type``.** ``data_type`` is a value from the
:class:`~timedatamodel.DataType` enum, passed as a case-insensitive string
(lowercased internally). The full vocabulary is ``actual``, ``observation``,
``derived``, ``calculated``, ``estimation``, ``forecast``, ``prediction``,
``scenario``, ``simulation``, ``reconstruction``, ``reference``, ``baseline``,
``benchmark``, ``ideal`` — a hierarchy (e.g. ``observation`` rolls up to
``actual``). ``timeseries_type`` is the temporal shape and must be one of the
two :class:`~timedatamodel.TimeSeriesType` values: ``FLAT`` (one value per
``valid_time``) or ``OVERLAPPING`` (versioned forecasts, many
``knowledge_time`` per ``valid_time``). ``timeseries_type`` is the one series
attribute that *can* be changed after registration.


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
     - Manifest column: ``node_uuid``, ``edge_uuid``, ``path``, or
       ``from_path`` + ``to_path`` + ``edge_type``

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
internally. ``write()`` returns a :class:`~energydb.WriteResult` — an ``int``
whose value is the ``run_id`` used for this batch (so existing code that
treats the result as a run_id keeps working), carrying ``.written`` /
``.skipped`` row counts as attributes. All writes are recorded in the
``energydb.runs`` table, keyed by a client-side UUID7-derived integer.

Optional kwargs:

- ``unit`` — declare the incoming unit; if it differs from the series'
  registered ``canonical_unit``, pint computes the scalar factor and
  rescales every value before writing
- ``knowledge_time`` — broadcast a single ``knowledge_time`` (required for
  OVERLAPPING series unless a ``knowledge_time`` column is on the DataFrame)
- ``run_id``, ``workflow_id``, ``model_name``, ``run_start_time``,
  ``run_finish_time``, ``run_params`` — provenance metadata stored in
  ``energydb.runs``
- ``skip_unchanged`` / ``unchanged_scope`` — drop rows that only duplicate
  the latest stored ``(value, annotation, changed_by)`` before the insert.
  See `Skipping unchanged writes`_ for the comparison keys. The
  ``energydb.runs`` row is upserted regardless, so an all-skipped write still
  records a run.

Skipping unchanged writes
~~~~~~~~~~~~~~~~~~~~~~~~~

Writing the same window twice with unchanged data appends rows that differ
only in ``change_time`` — physically new, but invisible to every reader.
``skip_unchanged=True`` drops them before the insert, at the cost of one
bounded read-back.

What counts as "unchanged" depends on the series: for a FLAT actual, a
re-sent value is a duplicate; for an OVERLAPPING forecast, *every*
publication is meaningful even when its values happen to repeat the previous
one. ``unchanged_scope`` selects the comparison key accordingly:

- ``"auto"`` (default) — **per series**, from each series' registered
  ``timeseries_type``: FLAT compares per ``(series_id, valid_time)``,
  OVERLAPPING per ``(series_id, valid_time, knowledge_time)``. One call
  handles a manifest that mixes both. For a FLAT-only manifest this is
  identical to ``"valid_time"``.
- ``"knowledge_time"`` — that key uniformly, for every series in the call.
- ``"valid_time"`` — that key uniformly. Raises
  :class:`~energydb.errors.UnchangedScopeError` when the manifest contains
  OVERLAPPING series, because it would silently drop their republications.

.. code-block:: python

   # Mixed manifest — actuals and forecast revisions in one call.
   result = client.write(manifest, skip_unchanged=True)
   print(result.written, result.skipped)

.. note::

   OVERLAPPING series require a ``knowledge_time`` (kwarg or column) on
   every write, so whenever the knowledge-time-scoped comparison applies
   there is a real publication time to compare against — never a per-batch
   ``now()`` stamp.

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

- ``node_uuid`` — programmatic routing by UUID. Values may be
  :class:`uuid.UUID` objects or their string form; both are accepted, and a
  mixed column is fine. ``uuid.UUID`` is what psycopg returns and what
  ``get_raw()["uuid"]`` gives you, so energydb's own output feeds straight
  back in.
- ``edge_uuid`` — programmatic routing for edge-attached series, same value
  handling as ``node_uuid``
- ``path`` — human-readable, ``Utf8`` joined with ``/``
  (e.g. ``"my-portfolio/Offshore-1/T01"``). ``/`` is reserved as the
  separator; names containing ``/`` are rejected at registration. The
  manifest must use ``Utf8`` — ``List(Utf8)`` from earlier API versions
  is rejected with an explicit migration message.
- ``from_path`` + ``to_path`` + ``edge_type`` — human-readable routing for
  edge-attached series (all three columns required together), the edge
  analogue of ``path``. Each is ``Utf8`` joined with ``/``; the edge is
  resolved server-side via its endpoint nodes' paths and type. Symmetric
  with the edge read output columns, so an edge read's frame can be fed
  back in as a manifest without resolving ``edge_uuid`` first.

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

   # By node uuid (programmatic) — UUID objects or strings, either works
   pl.DataFrame({
       "node_uuid":  [t01.id] * 24,
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

   # By edge triple (human-readable; all three columns required)
   pl.DataFrame({
       "from_path":  ["my-grid/BusA"] * 24,
       "to_path":    ["my-grid/BusB"] * 24,
       "edge_type":  ["Line"] * 24,
       "data_type":  ["actual"] * 24,
       "name":       ["power_flow"] * 24,
       "valid_time": hours,
       "value":      [200.0 + h for h in range(24)],
   })

Routing modes are mutually exclusive — passing more than one routing column
raises :class:`~energydb.errors.ManifestError`. Supplying only part of the
edge triple (e.g. ``from_path`` + ``to_path`` without ``edge_type``) raises
the same.

Series must already be registered (typically via
:meth:`~energydb.Client.register_tree`) — unresolved
``(owner, data_type, name)`` triples raise
:class:`~energydb.errors.SeriesNotFoundError` before any data reaches
ClickHouse, and the exception names *every* unresolved triple, not just the
first. Writes are always strict: there is no skip mode, because silently
dropping writes would be data loss (reads have one — see
`Reading past unregistered series`_).

Optional columns and kwargs:

- ``unit`` (column or kwarg) — incoming unit, auto-converted to each series'
  canonical unit; mutually exclusive when both forms are supplied
- ``knowledge_time`` (column or kwarg) — required for OVERLAPPING series
- ``run_id``, ``workflow_id``, ``model_name``, ``run_start_time``,
  ``run_finish_time``, ``run_params`` — provenance metadata; default
  ``run_id`` is one client-generated UUID7-derived integer per call
- ``skip_unchanged`` / ``unchanged_scope`` (kwargs) — drop rows that only
  duplicate the latest stored value before the insert; ``unchanged_scope``
  (default ``"auto"``) selects the comparison key per series. See
  `Skipping unchanged writes`_.

Returns a :class:`~energydb.WriteResult` — an ``int`` run_id carrying
``.written`` / ``.skipped`` row counts. The ``energydb.runs`` row is upserted
even when every row is skipped, so an all-skipped write still records a run.

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
- ``on_missing`` — ``"raise"`` (default) or ``"skip"``; see
  `Reading past unregistered series`_

The result columns mirror the scope read: ``path``, ``data_type``,
``name``, ``valid_time``, ``value`` for node manifests; ``from_path``,
``to_path``, ``edge_type``, ``data_type``, ``name``, ``valid_time``,
``value`` for edge manifests. ``path`` / ``from_path`` / ``to_path`` are
``Utf8`` joined with ``/``. Internal identifiers (``series_id``,
``node_uuid``, ``edge_uuid``) are never on the result.

Per-window relative reads use :meth:`Client.read_relative
<energydb.Client.read_relative>`, with the same parameters as TimeDB's
:meth:`~timedb.TimeDBClient.read_relative`.

Reading past unregistered series
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default a manifest triple that resolves to no registered series fails the
whole call with :class:`~energydb.errors.SeriesNotFoundError` — one gap in a
1,500-series manifest and you get nothing back. Pass ``on_missing="skip"``
to read everything that *does* resolve:

.. code-block:: python

   result = client.read(manifest, on_missing="skip")

   result.data      # the frame (or dict) for every series that resolved
   result.missing   # one row per unresolved triple

   # NamedTuple — unpacking works too
   data, missing = client.read(manifest, on_missing="skip")

.. important::

   ``on_missing="skip"`` **changes the return type** to
   :class:`~energydb.ReadResult`, a ``NamedTuple(data, missing)``. The
   default ``"raise"`` returns ``data`` bare, exactly as before.

- ``data`` is precisely what the same call returns without ``on_missing``,
  honouring ``output=`` and ``backend=`` — including the empty shapes when
  nothing resolved (an empty frame, or ``{}`` for ``by_path``).
- ``missing`` carries the unique unresolved triples: the manifest's routing
  column(s) plus ``data_type`` / ``name``, ``Utf8`` throughout (uuids
  stringified). It is zero-row with the correct schema when everything
  resolved, and follows ``backend=`` like ``data`` does.

Either way, the raise path reports **all** unresolved triples at once —
:attr:`SeriesNotFoundError.missing <energydb.errors.SeriesNotFoundError>`
carries the full list structurally, so discovering *N* gaps takes one call
rather than *N*.

``on_missing`` covers exactly one condition: "this triple has no registered
series" (including a ``path`` that matches no node). Structural problems with
the manifest — a missing or ambiguous routing column, a non-``Utf8`` ``path``,
null routing values — raise :class:`~energydb.errors.ManifestError` under both
settings. Those are caller bugs, not catalog gaps.

Output modes
~~~~~~~~~~~~

Both :meth:`Client.read <energydb.Client.read>` and the scope reads accept
an ``output=`` kwarg that controls return shape:

- ``output="frame"`` (default) — one DataFrame with the identity columns
  broadcast on every row. Good for ETL and ad-hoc analysis where you want
  to ``group_by(path)`` or filter further downstream.
- ``output="by_path"`` — a ``dict`` keyed by a
  :class:`~energydb.SeriesKey` ``(path, data_type, name)`` for node reads,
  or an :class:`~energydb.EdgeSeriesKey`
  ``(from_path, to_path, edge_type, data_type, name)`` for edge reads. Both
  are ``NamedTuple``\ s, so keys support positional *and* attribute access
  (``key.path``, ``key.name``, …). Each value is one DataFrame per series,
  carrying only the data columns (``valid_time``, ``value``, plus opt-in
  ``knowledge_time`` / ``change_time``), sorted by ``valid_time`` ascending.

Use ``by_path`` when downstream code naturally operates per-series — model
training, plotting, per-asset writes back. Series that resolve but have
no rows in ClickHouse still appear as keys with an empty sub-frame, so
callers can index by key without ``KeyError``. :func:`~energydb.find`
filters the result dict by partial key match (e.g. ``find(result,
name="power")``).

.. code-block:: python

   by_series = client.read(manifest, output="by_path")
   for key, sub in by_series.items():
       train_one_model(key.path, sub)   # key is a SeriesKey NamedTuple

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


Concurrent reads
----------------

When the ClickHouse meta-engine table is provisioned (see `Schema
Management`_) and expressible for the query, reads run the PostgreSQL series
resolve **in parallel** with the ClickHouse value read — ClickHouse
self-resolves ``series_id`` through the ``PostgreSQL()`` engine table over the
``series_meta`` view. The result is trimmed to the exact resolve and is
byte-identical to the sequential path. Queries that cannot express an engine
predicate (``.where()`` property filters, uuid-addressed subtrees) fall back
to sequential automatically.

Two environment variables gate this path:

- ``ENERGYDB_DISABLE_ENGINE=1`` — session kill-switch; always use the
  sequential path. Set at construction, so the engine predicate is never
  even built.
- ``ENERGYDB_ENGINE_STRICT=1`` — raise on an engine-read failure instead of
  degrading to sequential (useful in tests). Without it, the first failure
  degrades the session and later reads go sequential until
  :meth:`~energydb.Client.setup_ch_meta_engine` is called again.

Logging on degrade
~~~~~~~~~~~~~~~~~~

The first engine-read failure in a process logs once, and the level tells
you which kind of problem it is:

- **The engine table is not provisioned** → one ``info`` line, no traceback.
  For a deployment that never calls
  :meth:`~energydb.Client.setup_ch_meta_engine`, this is a steady
  configuration state, not an incident: reads work, they just take the
  sequential path.
- **Anything else** (network, auth, schema drift, ClickHouse→PostgreSQL
  connectivity) → a ``warning`` with the full traceback, naming the engine
  table, ``ENERGYDB_SCHEMA``, and the fix.

Either way the session latches the degrade, so later reads skip the engine
without re-probing and without logging again.

Provisioning credentials
~~~~~~~~~~~~~~~~~~~~~~~~

The ClickHouse engine table needs PostgreSQL credentials to reach the
``series_meta`` view. By default they are **inlined into the table's DDL**,
where anyone with ``SHOW CREATE TABLE`` rights on the ClickHouse instance can
read the password — so provisioning logs a warning saying exactly that
whenever a password is actually being inlined.

For production, put the connection in a `ClickHouse named collection
<https://clickhouse.com/docs/en/operations/named-collections>`_ and point
energydb at it:

.. code-block:: bash

   ENERGYDB_CH_PG_COLLECTION=my_pg_collection

The password then stays out of both the DDL and ``SHOW CREATE TABLE``, and
the warning goes away. One related knob:

- ``ENERGYDB_CH_PG_HOST`` (``host:port``) — overrides the DSN's host for
  **ClickHouse's** network vantage. The DSN addresses PostgreSQL from your
  application, which is not necessarily how ClickHouse reaches it (e.g.
  ``postgres:5432`` on a compose network vs ``127.0.0.1:5433`` locally).
  Database, user, and password always come from the DSN — only the network
  path differs.


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


Namespaces (multi-tenancy)
--------------------------

*New in 0.10.0.* One energydb database can serve many tenants. Every row in
``node`` / ``edge`` / ``series`` carries a ``namespace`` label, and
:meth:`~energydb.Client.namespace` returns a *view* of the client bound to one
tenant:

.. code-block:: python

   root = Client()                     # root client: sees everything
   acme = root.namespace("acme")       # view: sees and stamps only "acme"

Namespacing is **opt-in and invisible to single-tenant users**: a root client
never sets the label, rows it creates land in ``'default'``, and nothing else
about its behavior changes.

How a view works:

- It **shares the root client's connection pool and ClickHouse client** — no
  extra connections. Rebinding is cheap; call ``namespace()`` per request if
  you like.
- On checkout it binds the ``energydb.namespace`` GUC (transaction-local for
  transactional work), which a server-side column default reads back to stamp
  new rows. Write paths never mention the column.
- **Row filtering is enforced by PostgreSQL Row-Level Security when the host
  application enables it** — the library ships no policies of its own. Until
  RLS is enabled, a view stamps rows but does not hide other tenants' rows.
- Lifecycle operations (``open`` / ``create`` / ``delete`` / ``close`` /
  ``setup_ch_meta_engine``) are **root-only** and raise
  :class:`~energydb.errors.ValidationError` on a view.
- Engine-parallel reads are disabled on views (the ClickHouse meta engine
  reads PostgreSQL with credentials that bypass RLS); reads fall back to the
  sequential path.

One deployment caveat: the autocommit read path binds the GUC at *session*
level and clears it before the connection returns to the pool. Behind a
transaction-mode connection pooler (PgBouncer), session state is not
guaranteed to follow your queries — run **namespaced** deployments against a
direct PostgreSQL connection. Root clients have no such constraint; since
0.10.0 they are fully pooler-safe.


.. _sdk-error-handling:

Error Handling
--------------

Every exception energydb raises deliberately lives in
:mod:`energydb.errors` and derives from
:class:`~energydb.errors.EnergyDBError`. Each class is also re-exported from
the package root, so ``from energydb import SeriesNotFoundError`` works.

.. code-block:: text

   EnergyDBError
   ├── NotFoundError              — an addressed entity does not exist
   │   ├── NodeNotFoundError
   │   ├── EdgeNotFoundError
   │   └── SeriesNotFoundError
   ├── AlreadyExistsError         — create-only violation
   ├── ValidationError            — invalid arguments or invalid operation
   │   ├── ManifestError          — structurally invalid manifest
   │   └── UnchangedScopeError    — scope would drop OVERLAPPING revisions
   ├── ConfigurationError         — client / environment misconfiguration
   └── IncompatibleUnitError      — dimensionally incompatible unit conversion

Catch the branch you mean:

.. code-block:: python

   from energydb import (
       ManifestError,
       NodeNotFoundError,
       NotFoundError,
       SeriesNotFoundError,
   )

   try:
       df = client.read(manifest)
   except SeriesNotFoundError as e:
       # Every unresolved triple, structurally — not just the first.
       for owner, data_type, name in e.missing:
           log.warning("not registered: %s %s/%s", owner, data_type, name)
   except ManifestError:
       # The manifest itself is malformed — a bug in the producer.
       raise

   try:
       node = client.get_node("my-portfolio", "Offshore-1", "T01").get()
   except NodeNotFoundError as e:
       e.path      # "my-portfolio/Offshore-1/T01"
       e.uuid      # None — this call addressed by path

   # Or handle any absence uniformly.
   except NotFoundError:
       ...

.. note::

   **Every class above also subclasses** ``ValueError``. Broad
   ``except ValueError`` handlers therefore catch all of them, which keeps
   generic error-handling code working — but the typed classes are what you
   want in new code, because message text is not a contract and the
   structured fields are.

Structured fields
~~~~~~~~~~~~~~~~~

The not-found family carries the identifiers the failing call was given, so
callers (API servers especially) can react programmatically. Fields are
keyword-only and default to ``None`` when the raise site does not know them;
``str(e)`` is always the human-readable message.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Exception
     - Fields
   * - :class:`~energydb.errors.NodeNotFoundError`
     - ``path``, ``uuid``
   * - :class:`~energydb.errors.EdgeNotFoundError`
     - ``uuid``, ``from_path``, ``to_path``, ``edge_type``
   * - :class:`~energydb.errors.SeriesNotFoundError`
     - ``route`` (``"path"`` / ``"node_uuid"`` / ``"edge_uuid"`` / the edge
       triple), ``missing`` (every unresolved ``(owner, data_type, name)``)
   * - :class:`~energydb.errors.UnchangedScopeError`
     - ``overlapping_series_ids``

What raises what
~~~~~~~~~~~~~~~~

- :class:`~energydb.errors.NotFoundError` — an addressed node, edge, or
  series does not exist: a path or uuid that resolves to nothing, an edge
  triple with no matching edge, a manifest triple with no registered series.
- :class:`~energydb.errors.AlreadyExistsError` — create-only violations:
  a UUID in a ``register_tree`` payload that already exists in the database,
  a duplicate UUID on two elements of one tree, re-registering a series with
  different immutable attributes.
- :class:`~energydb.errors.ManifestError` — the manifest is structurally
  wrong: no routing column, more than one routing column, a partial edge
  triple, missing ``data_type`` / ``name``, a ``List(Utf8)`` ``path`` column
  (use ``Utf8`` joined with ``/``), or null routing values.
- :class:`~energydb.errors.UnchangedScopeError` —
  ``unchanged_scope="valid_time"`` on a manifest containing OVERLAPPING
  series, which would drop their republications. See
  `Skipping unchanged writes`_.
- :class:`~energydb.errors.ValidationError` — everything else the caller got
  wrong: mutually exclusive kwargs passed together, an invalid enum or choice
  value, a required field missing from an EDM payload, a naive (non-UTC)
  datetime, a ``move_to`` that would create a cycle, ``dry_run=True`` inside
  a transaction, an empty path.
- :class:`~energydb.errors.ConfigurationError` — the client cannot be
  constructed: no PostgreSQL connection configured, or a DSN that is not a
  URI.
- :class:`~energydb.IncompatibleUnitError` — a requested unit is not
  dimensionally compatible with the series' ``canonical_unit``.
- ``RuntimeError`` — time-series ``read`` / ``write`` / ``read_relative`` on
  a txn-bound scope. Not part of the taxonomy: it reports misuse of the API's
  shape rather than bad data. Call them outside the ``with``-block.

Schema misconfiguration
~~~~~~~~~~~~~~~~~~~~~~~

If the configured schema does not contain energydb's tables — the wrong
``ENERGYDB_SCHEMA``, or :meth:`~energydb.Client.create` never having run —
queries fail with psycopg's own ``UndefinedTable``. energydb attaches a note
to it naming the schema it searched, the environment variable that controls
it, and the fix, so the traceback is actionable:

.. code-block:: text

   psycopg.errors.UndefinedTable: relation "node" does not exist
   energydb: the configured schema 'analytics' (ENERGYDB_SCHEMA='analytics')
   does not contain the energydb tables. Either run 'await client.create()'
   once to provision them, or point ENERGYDB_SCHEMA at the schema that has
   them.

The exception type and message are unchanged — only a note is added — so
existing handlers that catch psycopg errors are unaffected.


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

9. **Catch typed exceptions, not message text.** ``except
   SeriesNotFoundError`` (and its ``.missing`` list) says what you mean and
   keeps saying it across releases; ``if "not registered" in str(e)`` does
   not. See `Error Handling`_.

10. **Reach for ``on_missing="skip"`` on wide reads.** A 1,500-series
    manifest should not return nothing because one series was never
    registered. Log ``result.missing`` and carry on.


.. _sdk-upgrading:

Upgrading to 0.10.0
-------------------

No migration required for an existing database: no ClickHouse engine-table
re-provisioning, and every API change is additive. Three things are worth
knowing:

- **Namespaces** (see `Namespaces (multi-tenancy)`_) are new and opt-in. A
  client that never calls :meth:`~energydb.Client.namespace` behaves exactly
  as on 0.9.0. ``create()`` on a **new** database now emits namespace-aware
  DDL (a ``namespace`` column on ``node``/``edge``/``series``, composite
  keys); an **existing** 0.9.0-shaped database keeps working unchanged — no
  root-client query references the column — but the library ships no
  migration to add namespacing to existing data; adopting multi-tenancy on an
  existing database is the host application's project.
- **energydb no longer touches the connection's** ``search_path``. Every
  relation reference is written out in the SQL instead — see `Schema
  resolution`_ above. This supersedes the 0.9.0 startup-option transport, which
  a PgBouncer-based pooler rejects (Neon's pooled endpoints fail *every*
  connection with ``unsupported startup parameter in options: search_path``).
  If you were pinned to an unpooled endpoint because of that, you no longer are.
  ``ENERGYDB_SCHEMA`` semantics are unchanged, and ``SHOW search_path`` now
  echoes whatever your server or role says — energydb stopped setting it.
- **New methods**, all additive: :meth:`~energydb.Client.namespace`,
  :meth:`~energydb.Client.list_nodes_raw` (keyset-paginated raw node listing),
  and the resolve-then-read split ``scope.resolve()`` /
  ``scope.read_from_meta()`` for authorize-before-read flows.


Upgrading to 0.9.0
------------------

No migration required: no schema change, and no ClickHouse engine-table
re-provisioning. Four things are worth knowing:

- **``unchanged_scope`` now defaults to ``"auto"``** on every write. For
  FLAT-only manifests it is identical to the previous ``"valid_time"``
  default; for manifests containing OVERLAPPING series it is the fix — those
  republications are no longer dropped. Passing ``"valid_time"`` explicitly
  with OVERLAPPING series in the manifest now raises
  :class:`~energydb.errors.UnchangedScopeError` rather than losing data.
- **``on_missing="skip"`` returns a** :class:`~energydb.ReadResult`, not a
  bare frame. Only opted-in calls see this; the default is unchanged.
- **The not-provisioned engine table logs at ``info``**, not ``warning`` with
  a traceback. Alerting that matched on that warning will stop firing for
  this cause — deliberately.
- **``SHOW search_path`` echoes ``myschema,public``** (no space), because the
  search path travels in the libpq startup packet rather than a ``SET``
  statement. *Superseded by 0.10.0*, which stops setting the search path at
  all — and which you want instead if anything between you and PostgreSQL
  pools connections.

Existing ``except ValueError`` handlers keep working: every class in
:mod:`energydb.errors` also subclasses ``ValueError``.


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

   # 2. Persist structure (create-only — raises if a UUID already exists).
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
