.. _dataset:

Datasets
==========

Fundamental to the DistZero runtime is the notion of a distributed dataset.  Generally, a distributed
set represents a list of some type of data, indexed by some key, and sharded across a potentially
very large number of `Node` instances.  Within the context of a single distributed system,
a distributed dataset can be used to represent a wide variety of things.
Here are some example lists each of which would be represented by a single distributed dataset:

  - The list of all user profiles corresponding to the unique users of the system.
  - The list of all web browser tabs currently connected to a system.  A single user may have more than one
    browser tab open at a time.
  - The list of all login attempts ever made by a user of the system.
  - In a university's course registration system: the list of all courses ever offered.
  - In a university's course registration system: the list of all courses offered in the fall, 2015.
  - In a university's course registration system: the list of all units in any course offered in the fall, 2015.

Internally, DistZero represents each distributed dataset with a **singly rooted tree** of `DataNode` instances.
The nodes with height >= 0 are "manager" nodes, and the nodes of height -1 are "leaf" nodes.
As suggested by the names, manager nodes manage other nodes,
whereas leaf nodes do the actual work associated with the dataset.
The nodes managed by a manager node of height ``h`` will have height ``h-1`` and
thus may be either leaf nodes or other manager nodes. They are referred to as the
"kids" or "children" of the manager node.

These trees are designed to grow and shrink as leaf nodes enter and leave the network.
As nodes enter, two operations will increase the size of the tree:

- New children are spawned by manager nodes that have capacity to add kids.
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
  removing it and "adopting" its kids.

These rules ensure that no io tree will be overly provisioned for very long after the number of leaf nodes has
dropped.

.. automodule:: dist_zero.node.io.data
   :members:

.. automodule:: dist_zero.node.io.leaf
   :members:

.. automodule:: dist_zero.node.io.adopter
   :members:

