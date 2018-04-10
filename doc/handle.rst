.. _handle:

Handle
===========================

A "handle" is a `json` serializable object that is used to represent either a `Node`, or a `MachineController`.

A handle for a `MachineController` will have 'id' and 'type' keys.
A handle for a `Node` running on a `MachineController` will have an 'id' identifying itself
and a 'controller_id' identifying the `MachineController` on which it is running.
