Migrations
==================

Migrations are created to update the structure of the network of nodes.
Each migration is represented by a single instance of `MigrationNode`.

.. automodule:: dist_zero.migration.migration_node
   :members:

Each `MigrationNode` will move through a series of `State` s in order to complete its behavior.

.. automodule:: dist_zero.migration.state.state
   :members:

.. automodule::  dist_zero.migration.state.state_01_starting_new_nodes
   :members:

.. automodule:: dist_zero.migration.state.state_02_starting_node_migrators
   :members:

.. automodule:: dist_zero.migration.state.state_03_starting_new_flow
   :members:

.. automodule:: dist_zero.migration.state.state_04_syncing_new_nodes
   :members:

.. automodule:: dist_zero.migration.state.state_05_preparing_switching
   :members:

.. automodule:: dist_zero.migration.state.state_06_switching
   :members:

.. automodule:: dist_zero.migration.state.state_07_terminating_migrators
   :members:

.. automodule:: dist_zero.migration.state.state_08_finished
   :members:



Migrators
----------------------------


.. automodule:: dist_zero.migration.migrator
   :members:

.. automodule:: dist_zero.migration.source_migrator
   :members:

.. automodule:: dist_zero.migration.sink_migrator
   :members:

.. automodule:: dist_zero.migration.insertion_migrator
   :members:

.. automodule:: dist_zero.migration.removal_migrator
   :members:
