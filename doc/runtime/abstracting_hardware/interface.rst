Interface to Distributed Hardware
==================================

The Node Interface
---------------------

Nodes are the fundamental unit of computation in dist_zero.
They function much like processes do in traditional computing.
Each `MachineController` instance manages a machine, and a set of `Node` instances
that run on that machine.

.. automodule:: dist_zero.node.node
   :members:

The Machine Controller Interface
---------------------------------

.. autoclass:: dist_zero.machine.MachineController
   :members:

.. _handle:

Handle
--------------

A "handle" is a `json` serializable object that is used to represent either a `Node`, or a `MachineController`.

A handle for a `MachineController` will have 'id' and 'type' keys.

A handle for a `Node` identifies one receiver `Node` to one sender `Node`.  Typically, the receiver `Node` will
hold on to the handle for a period of time, and will use it to send messages to the receiver.  A `Node` can create
a new handle from the sender node's id by calling `Node.new_handle`.

If one sender `Node` wishes to transfer one of its handles to another sender `Node`, it should call `Node.transfer_handle` on
the existing handle to obtain a new handle appropriate for use by the new sender.

.. _transport:

Transport
----------

A "transport" is a `json` serializable object that represents the information one `Node` needs in
order to communicate with another `Node`.  Each transport allows a single "sending" `Node` to send messages
to a unique "receiving" `Node`.

Transports should include all the network information about the receiving `Node`, along with any cryptographic
information used to secure the connection.

.. _unique_ids:

Unique Ids
------------

.. automodule:: dist_zero.ids
   :members:
