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


def internal_node_config(node_id,
                         parent,
                         variant,
                         height,
                         adjacent=None,
                         spawner_adjacent=None,
                         adoptees=None,
                         initial_state=None):
  '''
  A node config for creating an internal node to manage a new list of io nodes.

  :param str node_id: The id of the new node.
  :param parent: If this node is the root, then `None`.  Otherwise, the :ref:`handle` of its parent `Node`.
  :type parent: :ref:`handle` or `None`
  :param str variant: 'input' or 'output'
  :param int height: The height of the node in the tree.  See `InternalNode`
  :param adjacent: The :ref:`handle` adjacent node to either receiver from or forward to or `None`
  :type adjacent: :ref:`handle` or `None`
  :param spawner_adjacent: The node adjacent to the node that spawned self.  When provided, adjacent should be None,
    and the spawner_adjacent node will be responsible for setting up an adjacent node for self.
  :type spawner_adjacent: `None` or :ref:`handle`
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
      'spawner_adjacent': spawner_adjacent,
      'adoptees': [] if adoptees is None else adoptees,
      'initial_state': initial_state
  }


def adopt(new_parent):
  '''
  Sent by newly spawner `InternalNode` instances to each of the kids they are meant to steal,
  letting them know to change parents.

  :param new_parent: The :ref:`handle` of the new parent node.
  :type new_parent: :ref:`handle`
  '''
  return {'type': 'adopt', 'new_parent': new_parent}


def adopted_by(new_parent_id):
  '''
  Nodes that have switched parents will send this message to the old parent informing them that
  they are no longer that parent's child.

  :param str new_parent_id: The id of the parent the child has just switched to.
  '''
  return {'type': 'adopted_by', 'new_parent_id': new_parent_id}


def adopted():
  '''
  Nodes that have switched parents will send this message to the new parent informing them that
  they have switched.
  '''
  return {'type': 'adopted'}


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


def added_leaf(kid):
  '''
  Indicates that a pending LeafNode has successfull been added to the network, and is
  now ready to receive messages.

  :param kid: The :ref:`handle` of the leaf node that was just added.
  :type kid: :ref:`handle`
  '''
  return {'type': 'added_leaf', 'kid': kid}


def added_adjacent_leaf(kid, variant):
  '''
  Indicates to an adjacent edge node in a computation that a pending LeafNode has successfull been added to the network, and is
  now ready to receive messages.

  :param kid: The :ref:`handle` of the leaf node that was just added.
  :type kid: :ref:`handle`

  :param str variant: 'input' or 'output'
  '''
  return {'type': 'added_adjacent_leaf', 'kid': kid, 'variant': variant}


def hello_parent(kid):
  '''
  Sent by a newly spawned kid node to its parent to indicate that it is now live.

  :param kid: The :ref:`handle` of the newly added kid.
  :type kid: :ref:`handle`
  '''
  return {'type': 'hello_parent', 'kid': kid}


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
