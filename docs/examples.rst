Examples
========

energydb ships a Jupyter notebook demonstrating the core workflow: declare a
portfolio, persist its structure, write data through targeted and bulk paths,
read across the full hierarchy, round-trip the tree with renames / property
edits / deletes, and link nodes with edges.

Before running the examples locally, ensure you have:

1. **Databases**: PostgreSQL (asset hierarchy + series catalog) and
   ClickHouse (time-series values). See :doc:`installation` for setup
   instructions.

2. **Environment Variables**: Set your database connection strings:

   .. code-block:: bash

      # Bash/Zsh
      export TIMEDB_PG_DSN='postgresql://user:password@host:port/database'
      export TIMEDB_CH_URL='http://default:devpassword@localhost:8123/default'

   .. code-block:: fish

      # Fish
      set -x TIMEDB_PG_DSN postgresql://user:password@host:port/database
      set -x TIMEDB_CH_URL http://default:devpassword@localhost:8123/default

3. **Jupyter**: Install Jupyter to run the notebooks interactively:

   .. code-block:: bash

      pip install jupyter


Available Notebooks
-------------------

.. toctree::
   :hidden:

   notebooks/quickstart

- :doc:`Quickstart <notebooks/quickstart>`


Notebook Descriptions
---------------------

**Quickstart**: Walks through the full energydb workflow — schema setup
with ``Client.create()``, declaring a Portfolio of wind turbines / a PV
system / a battery, persisting structure with ``register_tree``, writing
data through ``NodeScope.write``, reading across the hierarchy with
subtree / type-filter / single-asset scopes, reconstructing the tree as
an EnergyDataModel object, surgical edits via scope mutators (``rename``,
``update``, ``delete``, ``add``) and batched atomically with
``Client.transaction()``, manifest-based bulk reads in both ``frame`` and
``by_path`` output modes, edges with their own series, and cleanup. The
notebook at ``examples/quickstart.ipynb`` is the source of truth and is
auto-copied into ``docs/notebooks/`` at build time.

The notebook is also runnable in Google Colab — the first cell installs
PostgreSQL and ClickHouse inside the Colab VM via ``examples/colab_setup.py``
so you can try energydb without a local database.
