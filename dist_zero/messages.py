'''
Functions to build standard messages.
'''

ENCODING = 'utf-8'
'''
The encoding to use for messages.
This should be a string understood by the python internals that operate on encodings.
'''


def ip_transport(host):
  return {'type': 'ip_transport', 'host': host}


# Handles
def machine_controller_handle(machine_id):
  return {'type': 'MachineController', 'id': machine_id}


# Machine configs


def machine_config(machine_controller_id, machine_name):
  '''
  A machine config

  :param str machine_controller_id: The unique id for the new machine.
  :param str machine_name: A human readable name for the new machine.
  '''
  return {'type': 'machine_config', 'machine_name': machine_name, 'id': machine_controller_id}


# Node configs


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


def sum_node_started(transport):
  '''
  For when a new sum node informs its parent that it has just started, and will now be sending messages.
  '''
  return {'type': 'sum_node_started', 'transport': transport}


def middle_node_is_live():
  '''
  Indicates that a middle node is now fully synced up.
  '''
  return {'type': 'middle_node_is_live'}


def middle_node_is_duplicated():
  '''
  Indicates that a middle node is now receiving from all the proper senders.
  '''
  return {'type': 'middle_node_is_duplicated'}


def start_duplicating(receiver, transport):
  '''
  This message is sent to inform a node that it should duplicate its sends to a new receiver.

  After receipt of this message, the node will send messages to the usual receivers just as before, but also
  send duplicated messages to the newly added receiver.

  :param receiver: The :ref:`handle` of the node to send the duplicates to.
  :type receiver: :ref:`handle`

  :param transport: A :ref:`transport` for talking to receiver.
  :type transport: :ref:`transport`
  '''
  return {'type': 'start_duplicating', 'receiver': receiver, 'transport': transport}


def finish_duplicating():
  '''
  This message is sent to a node that is duplicating its sends to inform it that it no longer need send messages
  to the old receivers.
  '''
  return {'type': 'finish_duplicating'}


def set_sum_total(total):
  '''
  For sum nodes that are the middle nodes in a migration and are currently migrating but not synced up,
  the message informs them of their total.

  :param int total: The total to start with.
  '''
  return {'type': 'set_sum_total', 'total': total}


def sum_node_config(node_id, senders, receivers, parent=None, parent_transport=None):
  '''
  A node config for creating a node to accept increments from a set of senders, sum them
  together and pass all increments to every receiver.

  :param str node_id: The id of the new node.
  :param list senders: A list of :ref:`handle` for sending nodes.
  :param list receivers: A list of :ref:`handle` for receiving nodes.
  :param parent: The :ref:`handle` of the parent `SumNode` of this node.
  :type parent: :ref:`handle`
  :param parent_transport: A :ref:`transport` for talking to this node's parent.
  :type parent_transport: :ref:`transport`
  '''
  return {
      'type': 'SumNode',
      'id': node_id,
      'senders': senders,
      'receivers': receivers,
      'parent': parent,
      'parent_transport': parent_transport
  }


def input_leaf_config(node_id, name, parent, parent_transport, receivers, recorded_user_json=None):
  '''
  Add a new leaf node to an InputNode list.

  :param str name: The name to use for the new node.
  :param parent: The handle of the parent node.
  :type parent: :ref:`handle`
  :param object parent_transport: A transport for talking to the parent.
  :param list receivers: The list of handles of receiver nodes the new node should send to.
  :param json recorded_user_json: json for a recorded user instance to initialize on the new node.
  '''
  return {
      'type': 'InputLeafNode',
      'id': node_id,
      'name': name,
      'parent': parent,
      'parent_transport': parent_transport,
      'receivers': receivers,
      'recorded_user_json': recorded_user_json,
  }


def output_leaf_config(node_id, name, initial_state, parent, parent_transport, senders):
  '''
  Add a new leaf node to an OutputNode list.

  :param str node_id: The id of the new node.

  :param str name: The name to use for the new node.

  :param object initial_state: A json serializeable starting state for this node.

  :param parent: The handle of the parent node.
  :type parent: :ref:`handle`

  :param object parent_transport: A transport for talking to the parent.

  :param list senders: The list of handles of senders nodes the new node should receive from.
  :type senders: A list of :ref:`handle`
  '''
  return {
      'type': 'OutputLeafNode',
      'id': node_id,
      'name': name,
      'initial_state': initial_state,
      'parent': parent,
      'parent_transport': parent_transport,
      'senders': senders,
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


def add_link(node, direction, transport):
  '''
  Inform a node that it is now linked to a new node.

  :param node: The handle of the new node
  :type node: :ref:`handle`
  :param str direction: 'sender' or 'receiver' depending respectively on whether the newly added node will
    send or receive from the node getting this message.
  :param transport: Transport data for communicating with node.
  :type transport: :ref:`transport`
  '''
  return {'type': 'add_link', 'node': node, 'direction': direction, 'transport': transport}


def added_link(transport):
  '''
  Inform a node that it has been successfully linked, and give a transport back to the sender.

  :param transport: Transport data for communicating with node.
  :type transport: :ref:`transport`
  '''
  return {'type': 'added_link', 'transport': transport}


# Actions


def increment(amount):
  '''
  :param int amount: An integer amount by which to increment.
  '''
  return {'type': 'increment', 'amount': amount}


def start_sending_to(new_receiver, transport):
  '''
  A message informing a node to start sending messages to a new receiver.

  :param new_receiver: The receiver to start sending to.
  :type new_receiver: A node :ref:`handle`
  :param transport: A transport allowing to send to new_receiver.
  :type transport: :ref:`transport`
  '''
  return {'type': 'start_sending_to', 'node': new_receiver, 'transport': transport}


def start_receiving_from(new_sender, transport):
  '''
  A message informing a node to start receiving messages from a new sender.

  :param new_sender: The sender to start receiving from
  :type new_sender: A node :ref:`handle`
  :param transport: A transport allowing to send to new_sender.
  :type transport: :ref:`transport`
  '''
  return {'type': 'start_receiving_from', 'node': new_sender, 'transport': transport}


def machine_start_node(node_config):
  '''
  A message to a machine indicating that it should start a new node based on a config.

  :param node_config: A node config for a new node.
  :type node_config: :ref:`message`

  :param str node_id: The unique id to use for this node.
  '''
  return {'type': 'machine_start_node', 'node_config': node_config}


def machine_deliver_to_node(node, message, sending_node):
  '''
  A message to a machine telling it to deliver an embedded message to a node.

  :param node: A node to send to.
  :type node: :ref:`handle`
  :param message: The message to deliver
  :type message: :ref:`message`
  :param sending_node: The node that was sending, or `None` if the message was not sent by a node.
  :type sending_node: :ref:`handle`
  '''
  return {'type': 'machine_deliver_to_node', 'message': message, 'node': node, 'sending_node': sending_node}


# API messages
def api_new_transport(sender, receiver):
  '''
  Get and return a transport that can be used to send from sender to receiver.
  :param sender: The :ref:`handle` of a sending node.
  :type sender: :ref:`handle`
  :param receiver: The :ref:`handle` of a sending node.
  :type receiver: :ref:`handle`
  '''
  return {'type': 'api_new_transport', 'sender': sender, 'receiver': receiver}


def api_get_output_state(node):
  '''
  Get and return the current output state for an output node.
  :param node: The :ref:`handle` of an output leaf node.
  :type node: :ref:`handle`
  :return: The current output state of that node.
  :rtype: object
  '''
  return {'type': 'api_get_output_state', 'node': node}


def api_create_kid_config(internal_node, new_node_name, machine_controller_handle):
  '''
  Create a node_config for a new kid node of an internal io node.

  :param internal_node: The :ref:`handle` of the parent internalnode.
  :type internal_node: :ref:`handle`
  :param str new_node_name: The name to use for the new node.
  :param machine_controller_handle: The :ref:`handle` of the machine on which the new node will run.
  :type machine_controller_handle: :ref:`handle`

  :return: A node_config for creating the new kid node.
  :rtype: :ref:`message`
  '''
  return {
      'type': 'api_create_kid_config',
      'internal_node_id': internal_node['id'],
      'new_node_name': new_node_name,
      'machine_controller_handle': machine_controller_handle,
  }
