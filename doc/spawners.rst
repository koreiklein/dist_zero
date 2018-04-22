Runners and Machine Controllers
================================

There are 3 modes that determine how `MachineController` instances are created and how they operate.

For virtual and cloud modes, the dist_zero.machine_init module should be the main entrypoint to add a new
host to the system.

.. automodule:: dist_zero.machine_init
   :members:



.. toctree::
   :maxdepth: 2
   :caption: Contents:

   simulator
   virtual
   cloud

