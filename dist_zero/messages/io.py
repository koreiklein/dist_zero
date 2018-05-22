'''
Messages to be received by input and output nodes.
'''


def input_node_config(node_id):
  '''
  A node config for creating a node to manage a new list of inputs.

  :param str node_id: The id of the new node.
  '''
  return {'type': 'InputNode', 'id': node_id}


def output_node_config(node_id, initial_state):
  '''
  A node config for creating a node to manage a new list of outputs.

  :param str node_id: The id of the new node.
  :param object initial_state: The initial state to use for new nodes.
  '''
  return {'type': 'OutputNode', 'id': node_id, 'initial_state': initial_state}


def input_leaf_config(node_id, name, parent, parent_transport, recorded_user_json=None):
  '''
  Add a new leaf node to an InputNode list.

  :param str name: The name to use for the new node.
  :param parent: The handle of the parent node.
  :type parent: :ref:`handle`
  :param object parent_transport: A transport for talking to the parent.
  :param json recorded_user_json: json for a recorded user instance to initialize on the new node.
  '''
  return {
      'type': 'InputLeafNode',
      'id': node_id,
      'name': name,
      'parent': parent,
      'parent_transport': parent_transport,
      'recorded_user_json': recorded_user_json,
  }


def output_leaf_config(node_id, name, initial_state, parent, parent_transport):
  '''
  Add a new leaf node to an OutputNode list.

  :param str node_id: The id of the new node.

  :param str name: The name to use for the new node.

  :param object initial_state: A json serializeable starting state for this node.

  :param parent: The handle of the parent node.
  :type parent: :ref:`handle`

  :param object parent_transport: A transport for talking to the parent.
  '''
  return {
      'type': 'OutputLeafNode',
      'id': node_id,
      'name': name,
      'initial_state': initial_state,
      'parent': parent,
      'parent_transport': parent_transport,
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


def added_input_leaf(kid, transport):
  '''
  Indicates that a pending input LeafNode has successfull been added to the network, and is
  now ready to receive messages.

  :param kid: The :ref:`handle` of the leaf node that was just added.
  :type kid: :ref:`handle`

  :param object transport: A transport that the recipient can use to send messages to the new leaf.
  '''
  return {'type': 'added_input_leaf', 'kid': kid, 'transport': transport}


def added_output_leaf(kid, transport):
  '''
  Indicates that a pending output LeafNode has successfull been added to the network, and is
  now ready to receive messages.

  :param kid: The :ref:`handle` of the leaf node that was just added.
  :type kid: :ref:`handle`

  :param object transport: A transport that the recipient can use to send messages to the new leaf.
  '''
  return {'type': 'added_output_leaf', 'kid': kid, 'transport': transport}
