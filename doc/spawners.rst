Spawners and Machine Controllers
================================

.. automodule:: dist_zero.spawners
   :members:

For virtual and cloud modes, the `dist_zero.machine_init` module should be the main entrypoint to add a new
host to the system.

.. automodule:: dist_zero.machine_init
   :members:

DistZero is designed so that the three modes share as much code as possible.
The differences between the three modes are encapsulated in the below `Spawner` sub-classes.
Each `Spawner` subclass defines its unique way to spawn new machines.  The overall distributed
system, however, should only interact with the commond methods of the abstract `Spawner` base class.

.. automodule:: dist_zero.spawners.spawner
   :members:

.. toctree::
   :maxdepth: 2
   :caption: Available Spawner Subclasses

   simulator
   virtual
   cloud

