'''
Messages relating to machines
'''


def server_address(domain, ip, port):
  '''
  A way to connect to a new web server.
  '''
  return {'domain': domain, 'ip': ip, 'port': port}


def ip_transport(host):
  return {'type': 'ip_transport', 'host': host}


# Machine configs


def std_system_config():
  '''
  Miscellaneous configuration for the overall system.

  **DATA_NODE_KIDS_LIMIT**

  When an `DataNode` has this many kids, it will trigger a split.

  **KID_SUMMARY_INTERVAL**

  Every time this many milliseconds pass on a data node, it should send a kid_summary message
  to its parent.

  **TOTAL_KID_CAPACITY_TRIGGER**

  When all the kids of a data node have less than this much capacity,
  it should spawn a new kid

  **SUM_NODE_SENDER_LIMIT, SUM_NODE_RECEIVER_LIMIT**

  If a sum node has more than this many senders/receivers, it will trigger a
  transaction that should create a middle layer of senders/receivers.

  **SUM_NODE_SPLIT_N_NEW_NODES**

  A sum node split will create this many new nodes

  **SUM_NODE_RECEIVER_LOWER_LIMIT, SUM_NODE_SENDER_LOWER_LIMIT, SUM_NODE_TOO_FEW_RECEIVERS_TIME_MS**

  If a sum node has fewer than ``SUM_NODE_RECEIVER_LOWER_LIMIT`` receivers and ``SUM_NODE_SENDER_LOWER_LIMIT`` senders
  for more than ``SUM_NODE_TOO_FEW_RECEIVERS_TIME_MS`` milliseconds,
  it will trigger a transaction to excise the sum node.
  '''
  return {
      # When an `DataNode` has this many kids, it will trigger a split.
      'DATA_NODE_KIDS_LIMIT': 200,

      # Limits on the number of senders and receivers to a `LinkNode`
      'LINK_NODE_MAX_SENDERS': 200,
      'LINK_NODE_MAX_RECEIVERS': 200,

      # Every time this many milliseconds pass on a data node, it should send a kid_summary message
      # to its parent.
      'KID_SUMMARY_INTERVAL': 200,

      # When all the kids of a data node have less than this much capacity,
      # it should spawn a new kid
      'TOTAL_KID_CAPACITY_TRIGGER': 5,

      # If a sum node has more than this many senders/receivers, it will trigger a
      # transaction to create a middle layer of senders/receivers.
      'SUM_NODE_SENDER_LIMIT': 15,
      'SUM_NODE_RECEIVER_LIMIT': 15,

      # A sum node split will create this many new nodes
      'SUM_NODE_SPLIT_N_NEW_NODES': 2,

      # If a sum node has fewer than SUM_NODE_RECEIVER_LOWER_LIMIT receivers and SUM_NODE_SENDER_LOWER_LIMIT senders
      # for more than SUM_NODE_TOO_FEW_RECEIVERS_TIME_MS milliseconds,
      # it will trigger a transaction to excise the sum node.
      'SUM_NODE_RECEIVER_LOWER_LIMIT': 3,
      'SUM_NODE_SENDER_LOWER_LIMIT': 3,
      'SUM_NODE_TOO_FEW_RECEIVERS_TIME_MS': 3 * 1000,
  }


def std_simulated_network_errors_config():
  '''
  Configuration for simulating network errors.

  This configuration produces no simulated errors at all.

  .. code-block:: python

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
    spawner=None,
    ip_address=None,
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

  :param object spawner: The serialized `Spawner` instance to use on the new machine.
  :param str ip_address: If provided, the public ip address of the running machine.
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
      'spawner': None,
      'ip_address': ip_address,
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


def link_datasets(link_config):
  '''
  Instructs a `ProgramNode` to create a new link.
  :param link_config: A config from `link_config`
  '''
  return {'type': 'link_datasets', 'link_config': link_config}


def spy(spy_key):
  '''API message to retrieve the value of a spied expression on a leaf.'''
  return {'type': 'spy', 'spy_key': spy_key}


def get_datasets():
  '''
  An API request to a `ProgramNode` to get the dictionary mapping each dataset id to its handle.
  '''
  return {'type': 'get_datasets'}


def get_links():
  '''
  An API request to a `ProgramNode` to get the dictionary mapping each link id to its handle.
  '''
  return {'type': 'get_links'}


def get_spy_roots():
  '''
  An API request to a `ProgramNode` to get the dictionary mapping each spy_key to the handle of the root `DataNode`
  of the dataset responsible for that spy key.
  '''
  return {'type': 'get_spy_roots'}


def new_handle(new_node_id):
  '''
  Get a new handle that can be used to send messages to a local node.  The handle will be used
  by a node that has not yet been spawned.

  :param str new_node_id: The id of a `Node` that has not yet been spawned.
  '''
  return {'type': 'new_handle', 'new_node_id': new_node_id}


def get_stats():
  '''
  Get and return the stats for a `Node` in the network.
  :return: The current stats of that node.
  :rtype: dict
  '''
  return {'type': 'get_stats'}


def get_data_link(link_key, key_type):
  '''
  API message to a data node to get the handle of the node subscribed to a particular ``link_key``

  :param str link_key: The link key
  :param str key_type: 'input' or 'output'
  '''
  return {'type': 'get_data_link', 'link_key': link_key, 'key_type': key_type}


def get_kids():
  '''API message to a node to get its dictionary of kids.'''
  return {'type': 'get_kids'}


def get_leftmost_kids():
  '''API message to a link node to get its dictionary of leftmost kids.'''
  return {'type': 'get_leftmost_kids'}


def get_interval():
  '''API message to a node to get its pair of left,right endpoints of its interval.'''
  return {'type': 'get_interval'}


def get_senders():
  '''API message to a node to get its dictionary of senders.'''
  return {'type': 'get_senders'}


def get_receivers():
  '''API message to a node to get its dictionary of receivers.'''
  return {'type': 'get_receivers'}


def get_capacity():
  '''API message to a node to get its dictionary of capacity data.'''
  return {'type': 'get_capacity'}


def create_kid_config(new_node_name, machine_id):
  '''
  Create a node_config for a new kid node of a data node.

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
