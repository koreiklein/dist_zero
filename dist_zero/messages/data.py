'''
Messages to be received by input and output nodes.
'''


def route_dns(domain_name):
  '''
  Indicates to a root data node that it should set up DNS to resolve domain_name
  (possibly via some load balancers) to a server being run by a height 1
  `DataNode` with capacity to add new kids.
  '''
  return {'type': 'route_dns', 'domain_name': domain_name}


def routing_start(domain_name):
  '''
  Indicates to a child node in an input tree that it should begin routing from
  DNS and load balancers.
  '''
  return {'type': 'routing_start', 'domain_name': domain_name}


def routing_started(server_address):
  '''
  Inform a parent that routing has started.

  :param server_address: A `server_address` object that can be used to
    send messages to the endpoint this child has just started.
  '''
  return {'type': 'routing_started', 'server_address': server_address}


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


# FIXME(KK): Use actual program configs, containing reactive graphs.
def demo_dataset_program_config(input_link_keys, output_link_keys):
  return {
      'type': 'demo_dataset_program_config',
      'input_link_keys': input_link_keys,
      'output_link_keys': output_link_keys
  }


def data_node_config(node_id, parent, height, dataset_program_config, recorded_user_json=None):
  '''
  A node config for creating a data node to manage a new list of data nodes.

  :param str node_id: The id of the new node.
  :param parent: If this node is the root, then `None`.  Otherwise, the :ref:`handle` of its parent `Node`.
  :type parent: :ref:`handle` or `None`
  :param int height: The height of the node in the tree.  See `DataNode`
  :param object dataset_program_config: Configuration information for what kind of leaf nodes to run.
  :param object initial_state: The initial state to use for new nodes.
  :param json recorded_user_json: json for a recorded user instance to initialize on the new node.
  '''
  return {
      'type': 'DataNode',
      'id': node_id,
      'parent': parent,
      'height': height,
      'dataset_program_config': dataset_program_config,
      'recorded_user_json': recorded_user_json,
  }


def collect_leaf_config():
  return {'type': 'collect_leaf_config', 'interval_type': 'interval'}


def hello_parent(kid, kid_summary=None, interval=None):
  '''
  Sent by a newly spawned kid node to its parent to indicate that it is now live.

  :param kid: The :ref:`handle` of the newly added kid.
  :type kid: :ref:`handle`
  :param tuple interval: A pair of the start and end point of this kid if provided.
  '''
  return {'type': 'hello_parent', 'kid': kid, 'kid_summary': kid_summary, 'interval': interval}


def set_leaf_key(key):
  '''
  Sent by an `AddLeafParent` node to the leaf it is adding to inform its new leaf
  what key it has been assigned.
  '''
  return {'type': 'set_leaf_key', 'key': key}


def absorb_these_kids(kid_ids, left_endpoint):
  '''
  Indicates to an Absorber which kid ids it should wait for before it is finished.

  :param list[str] kid_ids: The ids of the kids that the `Absorber` must adopt before its role in the transaction is finished.
  :param left_endpoint: The leftmost endpoint of the interval the absorbed kids cover.  
  :type left_endpoint: float or `intervals.Min`
  '''
  return {'type': 'absorb_these_kids', 'kid_ids': kid_ids, 'left_endpoint': left_endpoint}


def finished_absorbing(summary, new_interval):
  '''
  Indicates to the parent of an `Absorber` node that the absorber has finished absorbing everything it needs to absorb.

  :param object summary: The summary of the state of the `Absorber` node.
  :param tuple new_interval: The new interval the absorber is now responsible for
  '''
  return {'type': 'finished_absorbing', 'summary': summary, 'new_interval': new_interval}


def goodbye_parent():
  '''
  Sent by a leaf node to inform its parent `DataNode` that it has left the system.
  '''
  return {'type': 'goodbye_parent'}


def finished_splitting(summary):
  '''
  Sent by a `SplitNode` role when it finishes splitting.
  '''
  return {'type': 'finished_splitting', 'summary': summary}


def kid_summary(size, n_kids, availability, messages_per_second, height):
  '''
  Periodically sent by `DataNode` kids to their parents to give generally summary information
  that the parent needs to know about that kid.

  :param int size: An estimate of the number of leaves descended from the sender.
    It need not be perfectly accurate, but should be fairly close, especially if new descendents haven't been
    added in a while.
  :param n_kids: The number of immediate kids of the sender.
  :param float height: The estimated message rate in hertz.  It counts the rate of delivery of messages
    for this node and *all* its descendants combined.
  :param int height: The height of the sender.
  :param int availability: The availability to add new senders to a collect network.
  '''
  return {
      'type': 'kid_summary',
      'size': size,
      'n_kids': n_kids,
      'height': height,
      'availability': availability,
      'messages_per_second': messages_per_second
  }


def bumped_height(proxy, kid_ids):
  '''
  Sent by an `DataNode` to its adjacent node to inform it that the data node has bumped its height
  and now has a single child as its proxy.

  :param list[str] kid_ids: The ids of the `DataNode`'s kids which are being adopted by the proxy node.
  :param proxy: The :ref:`handle` of the new proxy node.
  :type proxy: :ref:`handle`
  '''
  return {'type': 'bumped_height', 'proxy': proxy, 'kid_ids': kid_ids}
