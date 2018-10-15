Nodes
==================


The Node abstraction
---------------------

Nodes are the fundamental unit of computation in dist_zero.
They function much like processes do in traditional computing.
Each `MachineController` instance manages a machine, and a set of `Node` instances
that run on that machine.

.. automodule:: dist_zero.node.node
   :members:

Classes for managing the network of nodes that perform computations
-----------------------------------------------------------------------

.. automodule:: dist_zero.node.computation
   :members:

Classes for resending and acknowledging messages
--------------------------------------------------

The linker/exporter/importer classes are designed to cover all the logic regarding message acknowledgement and
retransmission.  Nodes can ensure messages are retransmitted and acknowledged appropriately by receiving all
their messages with importers, sending all their messages with exporters, and updating their internal sequence
number from time to time.  The linker connects importers with exporters, and will ensure that acknowledgements
are sent on any importer for any message M once all the messages exporter because of M have been acknowledged.

.. automodule:: dist_zero.linker
   :members:

.. automodule:: dist_zero.importer
   :members:

.. automodule:: dist_zero.exporter
   :members:

IO trees
-----------------

User input and output are modeled by trees of nodes, each of which is in instance of `DataNode`.
The nodes with height >= 0 are for managing child nodes, and the nodes of height -1 do the "actual work",
manage no kids, and are referred to as "leaf nodes".
Each leaf node represents a particular input device or output device.
The leaf nodes are organized into a tree of `DataNode` instances, culminating in a root
`DataNode` .

These trees are designed to grow and shrink as devices enter and leave the network.
As devices enter, two operations will increase the size of the tree:

- New children are spawned by nodes that do not have extra capacity for leaves in their subtree.
- The root node will insert a proxy between itself and its children, increasing the tree's total height.

These two rules will function according to constraints that prevent any one node in the tree from being
overly burdened.  Node limits (see limits in `std_system_config`) prevent nodes from having too many kids,
senders or receivers.

Any mechanism for adding leaf nodes is expected to only ever add them to data nodes
of height 0 that have excess capacity.  The two scaling rules above ensure that such an data node should always
exist.

Two operations will decrease the size of the tree:

- Parent data nodes will monitor their children to check whether any two children both have very few kids.
  In the event that two children have very few kids for long enough, those two children nodes will be merged.
- If the root node has a single kid for long enough, then that kid effectively functions as a proxy for the root,
  managing the kids that the root could be managing instead.  In this case, the root node will merge with the kid,
  removing the kid and taking over its kids for itself.

These rules ensure that no io tree will be overly provisioned for very long after the number of leaf nodes has
dropped.

.. automodule:: dist_zero.node.io.data
   :members:

.. automodule:: dist_zero.node.io.leaf
   :members:

Node classes for performing computations
--------------------------------------------

.. automodule:: dist_zero.node.sum
   :members:
