Reference
=========

energydb provides a single Python interface — the :class:`~energydb.Client`
— with two fluent scopes (:class:`~energydb.NodeScope`,
:class:`~energydb.EdgeScope`), a structured :class:`~energydb.TreeDiff` for
preview/apply workflows, and SQLAlchemy models that double as the schema
source of truth.

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


Fluent Scopes
-------------

``client.get_node(...)`` and ``client.get_edge(...)`` return lazy scopes.
Path / filter accumulation does not hit the database; terminal operations
(``.read()``, ``.write()``, ``.get()``, ``.children()``, ``.rename()``,
``.delete()``, ``.register_series()``, …) resolve in one indexed CTE.

.. autoclass:: energydb.NodeScope
   :members:
   :show-inheritance:

.. autoclass:: energydb.EdgeScope
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
   :show-inheritance:

.. autoclass:: energydb.EdgeChange
   :members:
   :show-inheritance:

.. autoclass:: energydb.NodeSnapshot
   :members:
   :show-inheritance:

.. autoclass:: energydb.EdgeSnapshot
   :members:
   :show-inheritance:


Exceptions
----------

.. autoexception:: energydb.IncompatibleUnitError
   :members:


Time-Series Descriptors
-----------------------

:class:`~timedatamodel.TimeSeriesDescriptor` lives in ``timedatamodel`` and
is re-exported from ``energydb`` for convenience:

.. code-block:: python

   from energydb import DataType, TimeSeriesDescriptor, TimeSeriesType

The descriptor declares a series's identity (``name``, ``unit``,
``data_type``) and its bitemporal shape (``timeseries_type``: ``FLAT`` or
``OVERLAPPING``). Attach descriptors to any ``Element`` via the
``timeseries=[...]`` constructor kwarg; ``register_tree`` persists them
alongside the structure.


Schema (SQLAlchemy Models)
--------------------------

All tables live in the ``energydb`` PostgreSQL schema. The SQLAlchemy models
are the single source of truth — no raw SQL files. Platform code imports
``energydb.models.Base`` for Alembic migrations.

.. automodule:: energydb.models
   :members: Node, Edge, Series, Run
   :show-inheritance:

The ``energydb.series`` table is polymorphic: each row is owned by exactly
one of ``node_uuid`` / ``edge_uuid`` (DB ``CHECK`` enforces). The
``series_id`` primary key stays ``BIGINT`` — it's the timedb-internal
handle. Identity for nodes and edges is a ``UUID`` primary key, matching
the in-memory ``Element.id``.
