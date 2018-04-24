Machine
====================================

`Node` instances interact with the underlying hardware via the `MachineController` interface.
That interface has a unique implementation as a `NodeManager` that provides additional
methods, hidden from `Node` intances, that allow various types of runners to indicate to the `NodeManager`
the passage of time and the arrival of messages.

In simulated mode, the `SimulatedSpawner` class runs every `NodeManager` instance in the system.
In virtual and cloud modes, the unique `MachineRunner` runloop on each container or cloud instance
runs a single `NodeManager`.

.. automodule:: dist_zero.machine
   :members:

