'''
Messages to be received by input and output nodes.
'''

from dist_zero import errors


def route_dns(domain_name):
  '''
  Indicates to a root io node that it should set up DNS to resolve domain_name
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


def set_adjacent(node):
  '''
  Inform a node of its adjacent node.
  This will activate the node and it will start sending all its messages
  to the new adjacent node.

  :param node: The handle of the new node
  :type node: :ref:`handle`
  '''
  return {'type': 'set_adjacent', 'node': node}


def adopter_node_config(adoptees, data_node_config):
  '''
  An `AdopterNode` that will wait until some nodes child nodes have been adopted, and 
  '''
  return {
      'type': 'AdopterNode',
      'id': data_node_config['id'],
      'adoptees': adoptees,
      'data_node_config': data_node_config
  }


def data_node_config(node_id, parent, variant, height, leaf_config, recorded_user_json=None):
  '''
  A node config for creating a data node to manage a new list of io nodes.

  :param str node_id: The id of the new node.
  :param parent: If this node is the root, then `None`.  Otherwise, the :ref:`handle` of its parent `Node`.
  :type parent: :ref:`handle` or `None`
  :param str variant: 'input' or 'output'
  :param int height: The height of the node in the tree.  See `DataNode`
  :param object leaf_config: Configuration information for what kind of leaf nodes to run.
  :param object initial_state: The initial state to use for new nodes.
  :param json recorded_user_json: json for a recorded user instance to initialize on the new node.
  '''
  return {
      'type': 'DataNode',
      'id': node_id,
      'parent': parent,
      'variant': variant,
      'height': height,
      'leaf_config': leaf_config,
      'recorded_user_json': recorded_user_json,
  }


def merge_with(node):
  '''
  Indicates to the receiver that it should merge with one of its sibling nodes.

  :param node: The :ref:`handle` of the sibling node to merge with.
  :type node: :ref:`handle`
  '''
  return {'type': 'merge_with', 'node': node}


def sum_leaf_config(initial_state):
  return {'type': 'sum_leaf_config', 'initial_state': initial_state}


def collect_leaf_config():
  return {'type': 'collect_leaf_config'}


def hello_parent(kid):
  '''
  Sent by a newly spawned kid node to its parent to indicate that it is now live.

  :param kid: The :ref:`handle` of the newly added kid.
  :type kid: :ref:`handle`
  '''
  return {'type': 'hello_parent', 'kid': kid}


def absorb_these_kids(kid_ids):
  '''
  Indicates to an Absorber which kid ids it should wait for before it is finished.

  :param list[str] kid_ids: The ids of the kids that the `Absorber` must adopt before its role in the transaction is finished.
  '''
  return {'type': 'absorb_these_kids', 'kid_ids': kid_ids}


def finished_absorbing():
  '''
  Indicates to the parent of an `Absorber` node that the absorber has finished absorbing everything it needs to absorb.
  '''
  return {'type': 'finished_absorbing'}


def goodbye_parent():
  '''
  Sent by a leaf node to inform its parent `DataNode` that it has left the system.
  '''
  return {'type': 'goodbye_parent'}


def kid_summary(size, n_kids, availability):
  '''
  Periodically sent by `DataNode` kids to their parents to give generally summary information
  that the parent needs to know about that kid.

  :param int size: An estimate of the number of leaves descended from the sender.
    It need not be perfectly accurate, but should be fairly close, especially if new descendents haven't been
    added in a while.
  :param n_kids: The number of immediate kids of the sender.
  :param int availability: The availability to add new senders to a collect network.
  '''
  return {'type': 'kid_summary', 'size': size, 'n_kids': n_kids, 'availability': availability}


def bumped_height(proxy, kid_ids, variant):
  '''
  Sent by an `DataNode` to its adjacent node to inform it that the data node has bumped its height
  and now has a single child as its proxy.

  :param str variant: 'input' or 'output' according to the variant of the adjacent `DataNode`.
  :param list[str] kid_ids: The ids of the `DataNode`'s kids which are being adopted by the proxy node.
  :param proxy: The :ref:`handle` of the new proxy node.
  :type proxy: :ref:`handle`
  '''
  return {'type': 'bumped_height', 'proxy': proxy, 'variant': variant, 'kid_ids': kid_ids}
