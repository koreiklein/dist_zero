'''
Messages relating to machines
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
