'''
Messages relating to machines
'''


def ip_transport(host):
  return {'type': 'ip_transport', 'host': host}


# Machine configs


def std_system_config():
  '''
  Miscellaneous configuration for the overall system.
  '''
  return {
      # When an `InternalNode` has this many kids, it will trigger a split.
      'INTERNAL_NODE_KIDS_LIMIT': 200,

      # Every time this many milliseconds pass on an internal node, it should send a kid_summary message
      # to its parent.
      'KID_SUMMARY_INTERVAL': 200,

      # When all the kids of an internal node have less than this much capacity,
      # it should spawn a new kid
      'TOTAL_KID_CAPACITY_TRIGGER': 5,

      # If a sum node has more than this many senders/receivers, it will trigger a
      # "sum node split migration" to create a middle layer of senders/receivers.
      'SUM_NODE_SENDER_LIMIT': 15,
      'SUM_NODE_RECEIVER_LIMIT': 15,

      # A sum node split will create this many new nodes
      'SUM_NODE_SPLIT_N_NEW_NODES': 2,

      # If a sum node has fewer than SUM_NODE_RECEIVER_LOWER_LIMIT receivers and SUM_NODE_SENDER_LOWER_LIMIT senders
      # for more than SUM_NODE_TOO_FEW_RECEIVERS_TIME_MS milliseconds,
      # it will trigger a migration to excise the sum node.
      'SUM_NODE_RECEIVER_LOWER_LIMIT': 3,
      'SUM_NODE_SENDER_LOWER_LIMIT': 3,
      'SUM_NODE_TOO_FEW_RECEIVERS_TIME_MS': 3 * 1000,
  }


def std_simulated_network_errors_config():
  '''
  Configuration for simulating network errors.

  This configuration produces no simulated errors at all.

  ```
  {
    'outgoing': {
      'drop': { 'rate': 0.0, 'regexp': '.*' },
      'reorder': { 'rate': 0.0, 'regexp': '.*' },
      'duplicate': { 'rate': 0.0, 'regexp': '.*' },
    },
    'incomming': {
      'drop': { 'rate': 0.0, 'regexp': '.*' },
      'reorder': { 'rate': 0.0, 'regexp': '.*' },
      'duplicate': { 'rate': 0.0, 'regexp': '.*' },
    },
  }
  ```
  '''
  return {
      direction: {error_type: {
          'rate': 0.0,
          'regexp': '.*'
      }
                  for error_type in ['drop', 'reorder', 'duplicate']}
      for direction in ['incomming', 'outgoing']
  }


def machine_config(
    machine_controller_id,
    machine_name,
    system_id,
    mode,
    network_errors_config=None,
    system_config=None,
    random_seed=None,
):
  '''
  A machine configuration.

  :param str machine_controller_id: The unique id for the new machine.
  :param str machine_name: A human readable name for the new machine.
  :param str system_id: The unique id for the overall system.
  :param str mode: A mode (from `dist_zero.spawners`) (simulated, virtual, or cloud)

  :param dict network_errors_config: Configuration for simulating network errors.
    See `std_simulated_network_errors_config` for an example.
  :param dict system_config: Other miscellaneous configuration for the overall system.
    See `std_system_config` for an example.
  :param str random_seed: An optional seed to use for all this machine's randomness.  If it is not provided,
    the machine will get a random seed from the underlying operating system.
  '''
  return {
      'type': 'machine_config',
      'machine_name': machine_name,
      'id': machine_controller_id,
      'mode': mode,
      'system_id': system_id,
      'network_errors_config': network_errors_config or std_simulated_network_errors_config(),
      'system_config': system_config or std_system_config(),
      'random_seed': random_seed,
  }


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
def api_node_message(node_id, message):
  '''
  Generic api message for passing an inner message to a node on this machine.

  :param str node_id: The id of a `Node` instance running on self.
  :param message: An inner message for the handle_api_message method of the node identified by ``node_id``
  :type message: :ref:`message`
  '''
  return {'type': 'api_node_message', 'node_id': node_id, 'message': message}


def new_handle(new_node_id):
  '''
  Get a new handle that can be used to send messages to a local node.  The handle will be used
  by a node that has not yet been spawned.

  :param str new_node_id: The id of a `Node` that has not yet been spawned.
  '''
  return {'type': 'new_handle', 'new_node_id': new_node_id}


def get_output_state():
  '''
  Get and return the current output state for an output node.
  :return: The current output state of that node.
  :rtype: object
  '''
  return {'type': 'get_output_state'}


def get_stats():
  '''
  Get and return the stats for a `Node` in the network.
  :return: The current stats of that node.
  :rtype: dict
  '''
  return {'type': 'get_stats'}


def get_kids():
  '''API message to a node to get its dictionary of kids.'''
  return {'type': 'get_kids'}


def get_senders():
  '''API message to a node to get its dictionary of senders.'''
  return {'type': 'get_senders'}


def get_receivers():
  '''API message to a node to get its dictionary of receivers.'''
  return {'type': 'get_receivers'}


def get_capacity():
  '''API message to a node to get its dictionary of capacity data.'''
  return {'type': 'get_capacity'}


def get_adjacent_handle():
  '''API message to a node to get the handle of its adjacent (for internal and leaf nodes).'''
  return {'type': 'get_adjacent_handle'}


def create_kid_config(new_node_name, machine_id):
  '''
  Create a node_config for a new kid node of an internal io node.

  :param str new_node_name: The name to use for the new node.
  :param str machine_id: The id of the machine on which the new node will run.

  :return: A node_config for creating the new kid node.
  :rtype: :ref:`message`
  '''
  return {
      'type': 'create_kid_config',
      'new_node_name': new_node_name,
      'machine_id': machine_id,
  }


def kill_node():
  '''API message to a node telling it to die.'''
  return {'type': 'kill_node'}


def spawn_new_senders():
  '''
  Indicates to a sum node that it should spawn new senders.
  '''
  return {'type': 'spawn_new_senders'}
