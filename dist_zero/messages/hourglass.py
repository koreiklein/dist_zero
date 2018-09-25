'''
Messages relevant to performing an hourglass operation (mid_node_id, senders, receivers), in which
a complete bipartite graph of connections between senders and receivers is replaced with
an hourglass network in which ever sender has one connection to mid_node_id and mid_node_id
has one connection to each receiver.
'''


def mid_node_up(node):
  '''Indicates that a new mid node is now up and running.'''
  return {'type': 'mid_node_up', 'node': node}


def mid_node_ready(node_id):
  '''Indicates that a new mid node is now receiving from all its senders.'''
  return {'type': 'mid_node_ready', 'node_id': node_id}


def start_hourglass(receiver_ids, mid_node):
  '''
  Let a sender node of an hourglass operation know to start switching from sending to receivers to sending to the
  mid node.

  :param list[str] receiver_ids: The receivers to which this sender should no longer send.
  :param mid_node: The :ref:`handle` of the mid node of the hourglass operation.
  :type mid_node: :ref:`handle`
  '''
  return {'type': 'start_hourglass', 'receiver_ids': receiver_ids, 'mid_node': mid_node}


def hourglass_swap(mid_node_id, sequence_number):
  '''
  Indicates to a receiver node in an hourglass operation that a sender has swapped to using the mid node.

  :param str mid_node_id: The id of the mid node of the hourglass operation.
  :param int sequence_number: The first sequence number after those still destined for this receiver from this sender.
  '''
  return {
      'type': 'hourglass_swap',
      'mid_node_id': mid_node_id,
      'sequence_number': sequence_number,
  }


def hourglass_receive_from_mid_node(mid_node, n_hourglass_senders):
  '''
  Indicates to a receiver node in a hourglass operation that it should send a left_config to the mid_node
  and start receiving from it.

  :param mid_node: A :ref:`handle` for the middle node.
  :type mid_node: :ref:`handle`

  :param int n_hourglass_senders: The number of senders that should swap away from the receiver before it can switch to
    the mid node.

  '''
  return {
      'type': 'hourglass_receive_from_mid_node',
      'mid_node': mid_node,
      'n_hourglass_senders': n_hourglass_senders,
  }
