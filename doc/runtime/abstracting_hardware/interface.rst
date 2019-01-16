Interface to Distributed Hardware
==================================

DistZero's distributed runtime is written on top of the following abstract interface to underlying hardware.
We assume that the user of the following interface is writing a distributed system consisting of a large
number of very small units of computation, communicating with each other via message passing, each running
on a single machine.

In summary:

  - Units of computation are written as subclasses of `Node`.  These function as lightweight distributed
    processes, much like `erlang processes <http://erlang.org/doc/reference_manual/processes.html>`_.
  - Each node instance is given access to a `MachineController` instance variable.  It must use that
    `MachineController` instance to perform any operation involving communicating with other nodes,
    spawning nodes, terminating nodes, or interacting with time.

    Importantly, nodes should **NOT** use the ordinary `asyncio` or python methods for interacting with time,
    as doing so makes it very difficult to run tests. (For example, use `MachineController.sleep_ms`
    instead of `asyncio.sleep`.)  By using only `MachineController` methods to interact with time, tests of the
    distributed runtime can reliably simulate the passage of time; in less than a second,
    a test can simulate tens or hundreds of seconds of distributed system "time".
  - Nodes communicate with each other using `handles <handle>`.  The `Node` superclass contains methods for
    creating, transferring, and sending messages to a node identified by a `handle`.

The Node Interface
---------------------

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

A handle for a `Node` identifies one receiver `Node` to one sender `Node`.
Here is the typical lifecycle of a handle:

  - The receiver `Node` will generate a handle (see `Node.new_handle`) and transmit it to a sender `Node`.
  - The sender `Node` will receive the handle, and store it somewhere for later use.
  - Over time, the sender will use `Node.send` (or possibly `MachineController.send`) to send messages to the receiver.
  - In the event that a sender `Node` wishes to allow a third-party node to send to the receiver,
    it should call `Node.transfer_handle` on the existing handle to obtain a new handle appropriate for use by
    the third party.

A handle for a `MachineController` will have 'id' and 'type' keys.

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
