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


def machine_deliver_to_node(node_id, message, sending_node_id):
  '''
  A message to a machine telling it to deliver an embedded message to a node.

  :param str node_id: The id of a node to send to.
  :param str sending_node_id: The id of the node that sent the message.
  :param message: The message to deliver
  :type message: :ref:`message`
  '''
  return {'type': 'machine_deliver_to_node', 'message': message, 'node_id': node_id, 'sending_node_id': sending_node_id}


# API messages
def api_new_handle(local_node_id, new_node_id):
  '''
  Get a new handle that can be used to send messages to a local node.  The handle will be used
  by a node that has not yet been spawned.

  :param str local_node_id: The id of an exsiting `Node` on this machine.
  :param str new_node_id: The id of a `Node` that has not yet been spawned.
  '''
  return {'type': 'api_new_handle', 'local_node_id': local_node_id, 'new_node_id': new_node_id}


def api_get_output_state(node_id):
  '''
  Get and return the current output state for an output node.
  :param str node: The id of an output leaf node.
  :return: The current output state of that node.
  :rtype: object
  '''
  return {'type': 'api_get_output_state', 'node_id': node_id}


def api_create_kid_config(internal_node_id, new_node_name, machine_controller_handle):
  '''
  Create a node_config for a new kid node of an internal io node.

  :param internal_node_id: The id of the parent `InternalNode`.
  :param str new_node_name: The name to use for the new node.
  :param machine_controller_handle: The :ref:`handle` of the machine on which the new node will run.
  :type machine_controller_handle: :ref:`handle`

  :return: A node_config for creating the new kid node.
  :rtype: :ref:`message`
  '''
  return {
      'type': 'api_create_kid_config',
      'internal_node_id': internal_node_id,
      'new_node_name': new_node_name,
      'machine_controller_handle': machine_controller_handle,
  }
