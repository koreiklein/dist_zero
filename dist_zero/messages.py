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


def finished_duplicating():
  '''
  This message is sent to inform a migrator that an input has stopped sending messages as it was before
  the migration started, and is now only sending messages the new way.
  '''
  return {'type': 'finished_duplicating'}


def set_sum_total(total):
  '''
  For sum nodes that are the middle nodes in a migration and are currently migrating but not synced up,
  the message informs them of their total.

  :param int total: The total to start with.
  '''
  return {'type': 'set_sum_total', 'total': total}


def sum_node_config(node_id,
                    senders,
                    receivers,
                    sender_transports,
                    receiver_transports,
                    output_node=None,
                    output_transport=None,
                    input_node=None,
                    input_transport=None,
                    pending_sender_ids=None,
                    parent=None,
                    parent_transport=None):
  '''
  A node config for creating a node to accept increments from a set of senders, sum them
  together and pass all increments to every receiver.

  :param str node_id: The id of the new node.

  :param list senders: A list of :ref:`handle` for sending nodes.
  :param list receivers: A list of :ref:`handle` for receiving nodes.

  :param list sender_transports: A list of :ref:`transport` of the nodes sending increments
  :param list receiver_transports: A list of :ref:`transport` of the nodes to receive increments

  :param list pending_sender_ids: In the event that this node is starting via a migration, this is the list of
    senders that must be registered as sending duplicates in order for this node to decide that it is fully duplicated.

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
      'sender_transports': sender_transports,
      'receiver_transports': receiver_transports,
      'output_node': output_node,
      'input_node': input_node,
      'output_transport': output_transport,
      'input_transport': input_transport,
      'pending_sender_ids': pending_sender_ids if pending_sender_ids is not None else [],
      'parent': parent,
      'parent_transport': parent_transport
  }


def template_sum_node_config(senders=None, receivers=None):
  return sum_node_config(
      node_id=None,
      senders=senders if senders is not None else [],
      # The input leaf will add the input and input_transport parameters
      sender_transports=[],
      receivers=receivers if receivers is not None else [],
      receiver_transports=[],
      parent=None,
      parent_transport=None)


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


def added_link(node, direction, transport):
  '''
  Inform a node that it is now linked to a new node.

  :param node: The handle of the new node
  :type node: :ref:`handle`
  :param str direction: 'sender' or 'receiver' depending respectively on whether the newly added node will
    send to or receive from the node getting this message.
  :param transport: Transport data for communicating with node.
  :type transport: :ref:`transport`
  '''
  return {'type': 'added_link', 'node': node, 'direction': direction, 'transport': transport}


# Actions


def increment(amount):
  '''
  :param int amount: An integer amount by which to increment.
  '''
  return {'type': 'increment', 'amount': amount}


def set_input(input_node, transport):
  '''
  Configure the input node for a calculating node at the edge.

  :param input_node: The :ref:`handle` of the node to use as input.
  :type input_node: :ref:`handle`

  :param transport: A :ref:`transport` for talking to input_node
  :type transport: :ref:`transport`
  '''
  return {'type': 'set_input', 'input_node': input_node, 'transport': transport}


def activate_input(receiver, transport):
  '''
  Activates an input node when its edge node has been set.

  :param receiver: The :ref:`handle` of the node to be the receiver.
  :type receiver: :ref:`handle`

  :param transport: A :ref:`transport` for talking to receiver
  :type transport: :ref:`transport`
  '''
  return {'type': 'activate_input', 'receiver': receiver, 'transport': transport}


def set_output(output_node, transport):
  '''
  Configure the output node for a calculating node at the edge.

  :param output_node: The :ref:`handle` of the node to use as output.
  :type output_node: :ref:`handle`

  :param transport: A :ref:`transport` for talking to output_node
  :type transport: :ref:`transport`
  '''
  return {'type': 'set_output', 'output_node': output_node, 'transport': transport}


def activate_output(sender, transport):
  '''
  Activates an output node when its edge node has been set.

  :param sender: The :ref:`handle` of the node to be the sender.
  :type sender: :ref:`handle`

  :param transport: A :ref:`transport` for talking to sender
  :type transport: :ref:`transport`
  '''
  return {'type': 'activate_output', 'sender': sender, 'transport': transport}


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
