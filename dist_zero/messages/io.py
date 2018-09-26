'''
Messages to be received by input and output nodes.
'''

from dist_zero import errors


def input_action(number):
  '''
  A simple input action that generates a number

  :param int number: A number
  '''
  return {'type': 'input_action', 'number': number}


def output_action(number):
  '''
  A simple output action that generates a number

  :param int number: A number
  '''
  return {'type': 'output_action', 'number': number}


def set_adjacent(node):
  '''
  Inform a node of its adjacent node.
  This will activate the node and it will start sending all its messages
  to the new adjacent node.

  :param node: The handle of the new node
  :type node: :ref:`handle`
  '''
  return {'type': 'set_adjacent', 'node': node}


def internal_node_config(node_id, parent, variant, height, adjacent=None, adoptees=None, initial_state=None):
  '''
  A node config for creating an internal node to manage a new list of io nodes.

  :param str node_id: The id of the new node.
  :param parent: If this node is the root, then `None`.  Otherwise, the :ref:`handle` of its parent `Node`.
  :type parent: :ref:`handle` or `None`
  :param str variant: 'input' or 'output'
  :param int height: The height of the node in the tree.  See `InternalNode`
  :param adjacent: The :ref:`handle` adjacent node to either receiver from or forward to or `None`
  :type adjacent: :ref:`handle` or `None`
  :param adoptees: The list of `Node` instances that this node should adopt as its kids upon initialization,
    or `None` if the node should not initially adopt any kids.
  :type adoptees: list[:ref:`handle`] or `None`
  :param object initial_state: The initial state to use for new nodes.
  '''
  if parent is None and height == 0:
    raise errors.InternalError("internal_node_config for root nodes must have nonzero height.")

  return {
      'type': 'InternalNode',
      'id': node_id,
      'parent': parent,
      'variant': variant,
      'height': height,
      'adjacent': adjacent,
      'adoptees': [] if adoptees is None else adoptees,
      'initial_state': initial_state
  }


def merge_with(node):
  '''
  Indicates to the receiver that it should merge with one of its sibling nodes.

  :param node: The :ref:`handle` of the sibling node to merge with.
  :type node: :ref:`handle`
  '''
  return {'type': 'merge_with', 'node': node}


def adjacent_has_split(new_node, stolen_io_kid_ids):
  '''
  Sent by a newly spawned `InternalNode` that was spawned as part of an `InternalNode` split operation.
  This message will be recived by the node adjacent to the spawning node, and indicates that the newly
  spawned node is running, but needs to have the receiver spawn an adjacent for it.

  :param new_node: A :ref:`handle` for the sender.
  :type new_node: :ref:`handle`
  :param list[str] stolen_io_kid_ids: A list of ids of the io nodes that have been stolen from the recipient
    and should now belong to the sender.
  '''
  return {'type': 'adjacent_has_split', 'new_node': new_node, 'stolen_io_kid_ids': stolen_io_kid_ids}


def set_input(input_node):
  '''
  Configure the input node for a calculating node at the edge.

  :param input_node: The :ref:`handle` of the node to use as input.
  :type input_node: :ref:`handle`
  '''
  return {'type': 'set_input', 'input_node': input_node}


def set_output(output_node):
  '''
  This message informs a computation node of the data node it should output to.

  :param output_node: The :ref:`handle` of the node to use as output.
  :type output_node: :ref:`handle`
  '''
  return {'type': 'set_output', 'output_node': output_node}


def leaf_config(node_id, name, parent, variant, initial_state, recorded_user_json=None):
  '''
  A node config for a new leaf node.

  :param str node_id: The id to use for the new leaf node.
  :param str name: The name to use for the new node.
  :param parent: The handle of the parent node.
  :type parent: :ref:`handle`
  :param str variant: 'input' or 'output'
  :param object initial_state: A json serializeable starting state for this node.
  :param json recorded_user_json: json for a recorded user instance to initialize on the new node.
  '''
  return {
      'type': 'LeafNode',
      'id': node_id,
      'name': name,
      'parent': parent,
      'variant': variant,
      'initial_state': initial_state,
      'recorded_user_json': recorded_user_json,
  }


def added_sibling_kid(kid, height, variant):
  '''
  Indicates to a sibling node in a computation that a pending kid has successfully been added to the network,
  and is now ready to receive messages.

  :param kid: The :ref:`handle` of the node that was just added.
  :type kid: :ref:`handle`
  :param int height: The height of the parent.
  :param str variant: 'input' or 'output'
  '''
  return {'type': 'added_sibling_kid', 'kid': kid, 'variant': variant, 'height': height}


def hello_parent(kid):
  '''
  Sent by a newly spawned kid node to its parent to indicate that it is now live.

  :param kid: The :ref:`handle` of the newly added kid.
  :type kid: :ref:`handle`
  '''
  return {'type': 'hello_parent', 'kid': kid}


def goodbye_parent():
  '''
  Sent by a `LeafNode` to inform its parent `InternalNode` that it has left the system.
  '''
  return {'type': 'goodbye_parent'}


def kid_summary(size, n_kids):
  '''
  Periodically sent by `InternalNode` kids to their parents to give generally summary information
  that the parent needs to know about that kid.

  :param int size: An estimate of the number of `LeafNode` instances descended from the sender.
    It need not be perfectly accurate, but should be fairly close, especially if new descendents haven't been
    added in a while.
  :param n_kids: The number of immediate kids of the sender.
  '''
  return {'type': 'kid_summary', 'size': size, 'n_kids': n_kids}


def bumped_height(proxy, kid_ids, variant):
  '''
  Sent by an `InternalNode` to its adjacent node to inform it that the internal node has bumped its height
  and now has a single child as its proxy.

  :param str variant: 'input' or 'output' according to the variant of the adjacent `InternalNode`.
  :param list[str] kid_ids: The ids of the `InternalNode`'s kids which are being adopted by the proxy node.
  :param proxy: The :ref:`handle` of the new proxy node.
  :type proxy: :ref:`handle`
  '''
  return {'type': 'bumped_height', 'proxy': proxy, 'variant': variant, 'kid_ids': kid_ids}
