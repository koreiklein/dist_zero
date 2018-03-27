.. _handle:

handle
===========================

Handles are `json` objects that are used to represent various things.  They can be
used to represent remove objects from machines on which those objects do not
necessarily reside.

A handle for a `MachineController` will have 'id' and 'type' keys.
A handle for a Node running on a machine will have an 'id' identifying itself
and a 'controller_id' identifying the controller on which it is running.
