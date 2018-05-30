'''
Messages to be received by input and output nodes.
'''


def set_adjacent(node, transport):
  '''
  Inform a node of its adjacent node.
  This will activate the node and it will start sending all its messages
  to the new adjacent node.

  :param node: The handle of the new node
  :type node: :ref:`handle`
  :param transport: Transport data for communicating with node.
  :type transport: :ref:`transport`
  '''
  return {'type': 'set_adjacent', 'node': node, 'transport': transport}


def internal_node_config(node_id, variant, initial_state=None):
  '''
  A node config for creating an internal node to manage a new list of io nodes.

  :param str node_id: The id of the new node.
  :param str variant: 'input' or 'output'
  :param object initial_state: The initial state to use for new nodes.
  '''
  return {'type': 'InternalNode', 'id': node_id, 'variant': variant, 'initial_state': initial_state}


def leaf_config(node_id, name, parent, parent_transport, variant, initial_state, recorded_user_json=None):
  '''
  A node config for a new leaf node.

  :param str node_id: The id to use for the new leaf node.
  :param str name: The name to use for the new node.
  :param parent: The handle of the parent node.
  :type parent: :ref:`handle`
  :param object parent_transport: A transport for talking to the parent.
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
      'parent_transport': parent_transport,
      'initial_state': initial_state,
      'recorded_user_json': recorded_user_json,
  }


def added_leaf(kid, transport):
  '''
  Indicates that a pending LeafNode has successfull been added to the network, and is
  now ready to receive messages.

  :param kid: The :ref:`handle` of the leaf node that was just added.
  :type kid: :ref:`handle`

  :param object transport: A transport that the recipient can use to send messages to the new leaf.
  '''
  return {'type': 'added_leaf', 'kid': kid, 'transport': transport}


def added_adjacent_leaf(kid, variant, transport):
  '''
  Indicates to an adjacent edge node in a computation that a pending LeafNode has successfull been added to the network, and is
  now ready to receive messages.

  :param kid: The :ref:`handle` of the leaf node that was just added.
  :type kid: :ref:`handle`

  :param str variant: 'input' or 'output'

  :param object transport: A transport that the recipient can use to send messages to the new leaf.
  '''
  return {'type': 'added_adjacent_leaf', 'kid': kid, 'variant': variant, 'transport': transport}
