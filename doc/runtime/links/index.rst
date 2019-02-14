.. _link:

Links
========

Fundamental to the DistZero runtime is the notion of a link between two :doc:`distributed datasets </doc/runtime/datasets/index>`.
Links are effectively used to route messages from the leaf nodes of one "source" dataset to the appropriate leaf nodes
of another "target" dataset.

For example, if the source dataset consists of all the active web browser tabs in a distributed system, and the target
dataset consists of all the unique user profiles, then it would make sense to have a link between them that would
route login attempts from browser tabs to user profiles based on a ``username`` parameter provided by the user.

Just as with :doc:`datasets </doc/runtime/datasets/index>`, each link is represented internally as a **singly rooted tree** of `LinkNode`
instances.  Link nodes of height > 0 are "manager" nodes, and link nodes of height 0 are
"leaf" nodes.

We imagine that each link is forwarding messages from left to right.  Each individual link node has some
set of `link <LinkNode>` or `data <DataNode>` nodes to its immediate "left"
and some set of `link <LinkNode>` or `data <DataNode>` nodes to its immediate "right". The link node's sole responsibility
is to ensure that every message sent from the nodes to its left (or their descendants) is delivered to the appropriate
node(s) to its right (or their descendants).  Leaf link nodes meet this responsibility by forwarding sent messages,
and manager link nodes meet this repsonsibility
by maintained a network of "children" (a.k.a. "kid") nodes that collectively forwards messages from the children
of the nodes to its left to the children of the nodes to its right.

The logic that manager link nodes use to maintain their networks of kids is encapsulated in several `Connector`
classes.

.. toctree::
   :maxdepth: 2

   link_nodes
   transactions
   connecting


