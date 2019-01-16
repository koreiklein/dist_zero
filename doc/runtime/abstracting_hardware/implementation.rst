Implementations of the Distributed Hardware Interface
======================================================

`Node` instances interact with the underlying hardware via the `MachineController` interface.
The `NodeManager` provides methods that implement the `MachineController`, along with additional
methods (not accessible to `Node` instances) that allow the surrounding code to indicate to
the `NodeManager` the passage of time and the arrival of messages.

In simulated mode, the `SimulatedSpawner` class runs every `NodeManager` instance in the system.

In virtual (resp. cloud) mode, the unique call to `MachineRunner.runloop` on each container (resp. cloud instance)
runs a single `NodeManager`.

Node Manager
--------------

.. autoclass:: dist_zero.machine.NodeManager
   :members:


Machine Runner
----------------

.. automodule:: dist_zero.machine_runner
   :members:
