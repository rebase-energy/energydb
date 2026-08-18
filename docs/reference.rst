Reference
=========

energydb provides a single Python interface — the :class:`~energydb.Client`
and its async twin :class:`~energydb.AsyncClient` — with two fluent scopes
(:class:`~energydb.NodeScope`, :class:`~energydb.EdgeScope`), a structured
:class:`~energydb.TreeDiff` for preview/apply workflows, and SQLAlchemy
models that double as the schema source of truth.

Client
------

The single public entry point. Owns a psycopg connection pool against
PostgreSQL (asset hierarchy and series catalog) and an
internally-constructed :class:`timedb.TimeDBClient` against ClickHouse
(time-series values).

.. autoclass:: energydb.Client
   :members:
   :special-members: __init__
   :show-inheritance:

``Client`` is a thin blocking facade: it forwards every attribute to the
:class:`~energydb.AsyncClient` below, so the full method list is documented
there once. **Each method listed under** ``AsyncClient`` **exists on**
``Client`` **too, with an identical signature and no** ``await``:

.. code-block:: python

   client.register_tree(portfolio)          # Client   — blocks
   await aclient.register_tree(portfolio)   # AsyncClient

The same holds for the scopes and the transaction: ``client.get_node(...)``
returns a synchronous view of :class:`~energydb.NodeScope`.

.. autoclass:: energydb.AsyncClient
   :members:
   :special-members: __init__
   :show-inheritance:


Results
-------

Returned by :meth:`Client.write <energydb.AsyncClient.write>`,
:meth:`NodeScope.write <energydb.NodeScope.write>`, and
:meth:`EdgeScope.write <energydb.EdgeScope.write>`. Subclasses ``int`` (the
``run_id``) and carries ``written`` / ``skipped`` row counts.

.. autoclass:: energydb.WriteResult
   :members:
   :show-inheritance:

Returned by :meth:`Client.read <energydb.AsyncClient.read>` /
:meth:`Client.read_relative <energydb.AsyncClient.read_relative>` **only** when
``on_missing="skip"`` is passed; the default returns the data bare.

.. autoclass:: energydb.ReadResult
   :members:
   :show-inheritance:

Reads with ``output="by_path"`` return a ``dict`` keyed by one of these
``NamedTuple``\ s — node-routed reads by :class:`~energydb.SeriesKey`,
edge-routed reads by :class:`~energydb.EdgeSeriesKey`. Both support
positional *and* attribute access.

.. autoclass:: energydb.SeriesKey
   :members:
   :show-inheritance:

.. autoclass:: energydb.EdgeSeriesKey
   :members:
   :show-inheritance:

.. autofunction:: energydb.find


Fluent Scopes
-------------

``client.get_node(...)`` and ``client.get_edge(...)`` return lazy scopes.
Path / filter accumulation does not hit the database; terminal operations
(``.read()``, ``.write()``, ``.get()``, ``.children()``, ``.rename()``,
``.delete()``, ``.register_series()``, …) resolve in one indexed SQL query.

Both scopes share the time-series surface (``read``, ``write``,
``read_relative``, ``read_from_meta``, ``register_series``, ``resolve``) and
add their own structural operations, all listed below.

.. autoclass:: energydb.NodeScope
   :members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: energydb.EdgeScope
   :members:
   :inherited-members:
   :show-inheritance:


Transactions
------------

``client.transaction()`` returns a :class:`Transaction` context manager
that batches structure mutations into one atomic commit. Time-series
``read`` / ``write`` / ``read_relative`` on a txn-bound scope raise
``RuntimeError`` — they do not participate in the PG transaction.

.. autoclass:: energydb.Transaction
   :members:
   :show-inheritance:


Diff Types
----------

Returned by ``client.register_tree(..., dry_run=True)`` so callers can
preview structural changes before applying them.

.. autoclass:: energydb.TreeDiff
   :members:
   :show-inheritance:

.. autoclass:: energydb.NodeChange
   :members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: energydb.EdgeChange
   :members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: energydb.NodeSnapshot
   :members:
   :show-inheritance:

.. autoclass:: energydb.EdgeSnapshot
   :members:
   :show-inheritance:


Exceptions
----------

Every exception energydb raises deliberately derives from
:class:`~energydb.errors.EnergyDBError`. Every *raisable* subclass of it
*also* derives from ``ValueError``, so broad ``except ValueError`` handlers
keep catching them (the ``EnergyDBError`` base itself is never raised
directly and does not subclass ``ValueError``). The not-found family
carries structured identifier fields so callers can react programmatically
instead of matching message text. All names are re-exported from the package
root. See :ref:`the SDK error-handling guide <sdk-error-handling>` for usage.

.. automodule:: energydb.errors

.. autoexception:: energydb.errors.EnergyDBError
   :members:
   :show-inheritance:

.. autoexception:: energydb.errors.NotFoundError
   :members:
   :show-inheritance:

.. autoexception:: energydb.errors.NodeNotFoundError
   :members:
   :show-inheritance:

.. autoexception:: energydb.errors.EdgeNotFoundError
   :members:
   :show-inheritance:

.. autoexception:: energydb.errors.SeriesNotFoundError
   :members:
   :show-inheritance:

.. autoexception:: energydb.errors.AlreadyExistsError
   :members:
   :show-inheritance:

.. autoexception:: energydb.errors.ValidationError
   :members:
   :show-inheritance:

.. autoexception:: energydb.errors.ManifestError
   :members:
   :show-inheritance:

.. autoexception:: energydb.errors.UnchangedScopeError
   :members:
   :show-inheritance:

.. autoexception:: energydb.errors.ConfigurationError
   :members:
   :show-inheritance:

.. autoexception:: energydb.IncompatibleUnitError
   :members:
   :show-inheritance:


Time-Series Declarations
------------------------

:class:`~timedatamodel.TimeSeries` lives in ``timedatamodel`` and is
re-exported from ``energydb`` for convenience:

.. code-block:: python

   from energydb import DataType, TimeSeries, TimeSeriesType

A metadata-only ``TimeSeries`` (constructed with ``df=None``) declares a
series's identity (``name``, ``unit``, ``data_type``) and its temporal
shape (``timeseries_type``: ``FLAT`` or ``OVERLAPPING``). Attach such
declarations to any ``Element`` via the ``timeseries=[...]`` constructor
kwarg; ``register_tree`` persists them alongside the structure.


Data Model Re-Exports
---------------------

For convenience, ``energydb`` re-exports the public
`EnergyDataModel <https://github.com/rebase-energy/energydatamodel>`_ and
`TimeDataModel <https://github.com/rebase-energy/timedatamodel>`_ API, so a
portfolio can be declared without a second import. These classes are
documented in their own projects; the names available as ``edb.*`` are:

**Structure and base types**
   ``Element``, ``Node``, ``Edge``, ``Reference``, ``Asset``, ``NodeAsset``,
   ``GridNode``, ``Sensor``, ``Collection``

**Collections and portfolios**
   ``Portfolio``, ``Site``, ``MultiSite``, ``Region``, ``EnergyCommunity``,
   ``VirtualPowerPlant``

**Geographic and market areas**
   ``Area``, ``BiddingZone``, ``ControlArea``, ``Country``,
   ``SynchronousArea``, ``WeatherCell``

**Asset submodules**
   ``edb.wind``, ``edb.solar``, ``edb.battery``, ``edb.hydro``,
   ``edb.heatpump``, ``edb.building``, ``edb.grid``, ``edb.weather`` — each
   holding the concrete asset classes for that domain (e.g.
   ``edb.wind.WindTurbine``, ``edb.grid.Line``)

**Time-series declarations**
   ``TimeSeries``, ``DataType``, ``DataShape``, ``Frequency``,
   ``TimeSeriesType``

**Metric helpers**
   ``Kind``, ``Quantity``, ``Scope``, ``build_metric``, and the prebuilt
   metrics ``cross_border_flow``, ``electricity_demand``,
   ``electricity_demand_area``, ``electricity_supply``,
   ``electricity_supply_area``, ``gas_demand``, ``gas_supply``,
   ``grid_frequency``, ``heating_demand``, ``spot_price``, ``temperature``


Schema (SQLAlchemy Models)
--------------------------

All tables live in the schema named by ``ENERGYDB_SCHEMA``, defaulting to
``public``. The SQLAlchemy models are the single source of truth — no raw SQL
files. Platform code imports ``energydb.models.Base`` for Alembic migrations.
Series immutability (``retention``, ``canonical_unit``, owner columns) is
enforced in Python by ``register_series`` rather than by a DB trigger, so the
schema is fully Alembic-autogeneratable.

.. automodule:: energydb.models
   :members: Node, Edge, Series, Run
   :show-inheritance:

The ``energydb.series`` table is polymorphic: each row is owned by exactly
one of ``node_uuid`` / ``edge_uuid`` (DB ``CHECK`` enforces). The
``series_id`` primary key stays ``BIGINT`` — it's the timedb-internal
handle. Identity for nodes and edges is a ``UUID`` primary key, matching
the in-memory ``Element.id``.
