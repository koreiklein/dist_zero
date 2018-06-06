'''
Messages to be received by input and output nodes.
'''


def set_adjacent(node):
  '''
  Inform a node of its adjacent node.
  This will activate the node and it will start sending all its messages
  to the new adjacent node.

  :param node: The handle of the new node
  :type node: :ref:`handle`
  '''
  return {'type': 'set_adjacent', 'node': node}


def internal_node_config(node_id, variant, adjacent, initial_state=None):
  '''
  A node config for creating an internal node to manage a new list of io nodes.

  :param str node_id: The id of the new node.
  :param str variant: 'input' or 'output'
  :param adjacent: The :ref:`handle` adjacent node to either receiver from or forward to.
  :type adjacent: :ref:`handle`
  :param object initial_state: The initial state to use for new nodes.
  '''
  return {
      'type': 'InternalNode',
      'id': node_id,
      'variant': variant,
      'adjacent': adjacent,
      'initial_state': initial_state
  }


def set_input(input_node):
  '''
  Configure the input node for a calculating node at the edge.

  :param input_node: The :ref:`handle` of the node to use as input.
  :type input_node: :ref:`handle`
  '''
  return {'type': 'set_input', 'input_node': input_node}


def set_output(output_node):
  '''
  Configure the output node for a calculating node at the edge.

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
