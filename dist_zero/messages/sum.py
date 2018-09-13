'''
Messages to be received by sum nodes
'''


def increment(amount):
  '''
  :param int amount: An integer amount by which to increment.
  '''
  return {'type': 'increment', 'amount': amount}


def added_sender(node):
  '''
  Sent by a node to indicate that it will now send to the recipient.

  :param node: The :ref:`handle` of the sender.
  :type node: :ref:`handle`
  '''
  return {'type': 'added_sender', 'node': node}


def added_receiver(node):
  '''
  Sent by a node to indicate that it will now receive from the recipient.

  :param node: The :ref:`handle` of the receiver.
  :type node: :ref:`handle`
  '''
  return {'type': 'added_receiver', 'node': node}


def sum_node_started(sum_node_handle):
  '''
  For when a new sum node informs its parent that it has just started, and will now be sending messages.

  :param sum_node_handle: The :ref:`handle` of the newly started node.
  :type sum_node_handle: :ref:`handle`
  '''
  return {'type': 'sum_node_started', 'sum_node_handle': sum_node_handle}


def sum_node_config(
    node_id,
    senders,
    receivers,
    configure_right_parent_ids,
    parent,
    left_is_data,
    right_is_data,
    migrator=None,
):
  '''
  A node config for creating a node to accept increments from a set of senders, sum them
  together and pass all increments to every receiver.

  :param str node_id: The id of the new node.
  :param bool left_is_data: True iff the node just to the left is a data node.
  :param bool right_is_data: True iff the node just to the right is a data node.

  :param list senders: A list of :ref:`handle` for sending nodes.
  :param list receivers: A list of :ref:`handle` for receiving nodes.

  :param parent: A :ref:`handle` for the parent `ComputationNode`

  :param migrator: The migrator config for the new node if it is being started as part of a migration.

  '''
  return {
      'type': 'SumNode',
      'id': node_id,
      'configure_right_parent_ids': configure_right_parent_ids,
      'senders': senders,
      'receivers': receivers,
      'parent': parent,
      'left_is_data': left_is_data,
      'right_is_data': right_is_data,
      'migrator': migrator,
  }
