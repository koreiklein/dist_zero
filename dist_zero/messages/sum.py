'''
Messages to be received by sum nodes
'''


def increment(amount):
  '''
  :param int amount: An integer amount by which to increment.
  '''
  return {'type': 'increment', 'amount': amount}


def sum_node_started(sum_node_handle):
  '''
  For when a new sum node informs its parent that it has just started, and will now be sending messages.

  :param sum_node_handle: The :ref:`handle` of the newly started node.
  :type sum_node_handle: :ref:`handle`
  '''
  return {'type': 'sum_node_started', 'sum_node_handle': sum_node_handle}


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
                    spawning_migration=None,
                    spawning_migration_transport=None):
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

  :param spawning_migration: The :ref:`handle` of the migration spawning this `SumNode` if it has one.
  :type spawning_migration: :ref:`handle` or None.
  :param spawning_migration_transport: A :ref:`transport` for talking to this node's spawning migration.
  :type spawning_migration_transport: :ref:`transport` or None
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
      'spawning_migration': spawning_migration,
      'spawning_migration_transport': spawning_migration_transport
  }
