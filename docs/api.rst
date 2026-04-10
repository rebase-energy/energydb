API reference
=============

Public entry points
-------------------

.. autoclass:: energydb.EnergyDataClient
   :members:
   :show-inheritance:

.. autoclass:: energydb.NodeScope
   :members:
   :show-inheritance:

.. autoclass:: energydb.EdgeScope
   :members:
   :show-inheritance:

Data models
-----------

``TimeSeriesDescriptor`` lives in ``timedatamodel`` and is imported directly from there:

.. code-block:: python

   from timedatamodel import TimeSeriesDescriptor, DataType, TimeSeriesType

Schema (SQLAlchemy models)
--------------------------

Platform code imports ``energydb.models.Base`` for Alembic. The tables are
``Node``, ``NodeSeries``, ``Edge``, and ``EdgeSeries``.

.. automodule:: energydb.models
   :members: Node, NodeSeries, Edge, EdgeSeries
   :show-inheritance:
