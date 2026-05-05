API reference
=============

Client
------

The single public entry point. Owns a psycopg connection pool against
PostgreSQL (assets and series metadata) and an internally-constructed
:class:`timedb.TimeDBClient` against ClickHouse (time-series values).

.. autoclass:: energydb.Client
   :members:
   :show-inheritance:

Fluent scopes
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

Diff types
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

Time-series descriptors
-----------------------

``TimeSeriesDescriptor`` lives in ``timedatamodel`` and is re-exported
from ``energydb`` for convenience:

.. code-block:: python

   from energydb import TimeSeriesDescriptor, DataType, TimeSeriesType

Schema (SQLAlchemy models)
--------------------------

Platform code imports ``energydb.models.Base`` for Alembic. The tables
are ``Node``, ``Edge``, ``Series``, and ``Run`` — all in the ``energydb``
PostgreSQL schema. SQLAlchemy models are the single source of truth; no
raw SQL files.

.. automodule:: energydb.models
   :members: Node, Edge, Series, Run
   :show-inheritance:
