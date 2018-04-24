Machine
====================================

`Node` instances interact with the underlying hardware via the `MachineController` interface.
It has a unique implementation as a `NodeManager` that provides additional
methods (inivible to `Node` intances) that allow various types of runners to pass time and events into
through.  In particular, the `SimulatedSpawner` class runs `NodeManager` instances in simulated mode,
and the unique `MachineRunner` runloop on each container or cloud instance runs a unique `NodeManager`
in virtual and cloud modes.

.. automodule:: dist_zero.machine
   :members:

