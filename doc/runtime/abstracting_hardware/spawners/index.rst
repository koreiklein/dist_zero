Modes and Spawners Generally
================================

.. automodule:: dist_zero.spawners
   :members:

For virtual and cloud modes, the `dist_zero.machine_init` module should be the main entrypoint to add a new
host to the system.


dist_zero.machine_init
-----------------------

.. automodule:: dist_zero.machine_init
   :members:


Spawner Interface
------------------

DistZero is designed so that the three modes share as much code as possible.
The differences between the three modes are encapsulated in the below `Spawner` sub-classes.
Each `Spawner` subclass defines its unique way to spawn new machines.  The surrounding code,
however, should only interact with the public methods of the abstract `Spawner` base class.

.. automodule:: dist_zero.spawners.spawner
   :members:

Spawner Implementations
============================
.. toctree::
   :maxdepth: 2
   :caption: Available Spawner Subclasses

   simulator
   virtual
   cloud

