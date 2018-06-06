.. _handle:

Handle
===========================

A "handle" is a `json` serializable object that is used to represent either a `Node`, or a `MachineController`.

A handle for a `MachineController` will have 'id' and 'type' keys.

A handle for a `Node` identifies one receiver `Node` to one sender `Node`.  Typically, the receiver `Node` will
hold on to the handle for a period of time, and will use it to send messages to the receiver.  A `Node` can create
a new handle from the sender node's id by calling `Node.new_handle`.

If one sender `Node` wishes to transfer one of its handles to another sender `Node`, it should call `Node.transfer_handle` on
the existing handle to obtain a new handle appropriate for use by the new sender.
