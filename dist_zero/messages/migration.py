'''
Messages to be received by migration nodes.
'''


def connect_node(node, direction):
  '''
  Inform a node that it is now linked to a new node either
  as a sender or a receiver.

  :param node: The handle of the new node
  :type node: :ref:`handle`
  :param str direction: 'sender' or 'receiver' depending respectively on whether the newly added node will
    send to or receive from the node getting this message.
  '''
  return {'type': 'connect_node', 'node': node, 'direction': direction}


def middle_node_is_synced():
  '''
  Indicates that a middle node is now fully synced up.
  '''
  return {'type': 'middle_node_is_synced'}


def middle_node_is_live(input_node_id_to_first_live_sequence_number):
  '''
  Indicate to the migrator node that a middle node is now live and sending totally up to date messages.

  :param dict[str, int] input_node_id_to_first_live_sequence_number: For each input node to that middle node,
    this map assigns the first sequence number that it will send through the middle node after the swap.
  '''
  return {
      'type': 'middle_node_is_live',
      'input_node_id_to_first_live_sequence_number': input_node_id_to_first_live_sequence_number
  }


def started_duplication(node, sequence_number, message):
  '''
  Informs a middle node in a migration that an input node is now duplicating to it.

  :param node: The :ref:`handle` of the `Node` that is now duplicating messages.
  :type node: :ref:`handle`

  :param int sequence_number: The sequence number of the new message.
  :param message: The message for that sequence number.
  :type message: :ref:`message`
  '''
  return {'type': 'started_duplication', 'sequence_number': sequence_number, 'message': message, 'node': node}


def middle_node_is_duplicated(duplicator_id_to_first_sequence_number):
  '''
  Indicates that a middle node is now receiving from all the proper senders.

  :param dict[int,int] duplicator_id_to_first_sequence_number: For the id of each `Node` that is
    duplicating to the middle node, this map gives the first sequence number the middle node received
    from that duplicator.
  '''
  return {
      'type': 'middle_node_is_duplicated',
      'duplicator_id_to_first_sequence_number': duplicator_id_to_first_sequence_number
  }


def start_duplicating(old_receiver_id, receiver):
  '''
  This message is sent to inform a node that it should duplicate its sends to a new receiver.

  After receipt of this message, the node will send messages to the usual receivers just as before, but also
  send duplicated messages to the newly added receiver.

  :param str old_receiver_id: The id of the node whose messages should be duplicated.

  :param receiver: The :ref:`handle` of the node to send the duplicates to.
  :type receiver: :ref:`handle`

  '''
  return {'type': 'start_duplicating', 'old_receiver_id': old_receiver_id, 'receiver': receiver}


def swap_to_duplicate(node_id):
  '''
  Indicates to an input `Node` that it should send new messages only on
  the newly duplicated exporter, and not to the `Node` identified by ``node_id``.

  :param str node_id: The id of the `Node` to which the receiver of this message should stop sending.
  '''
  return {'type': 'swap_to_duplicate', 'node_id': node_id}


def swapped_to_duplicate(node_id, first_live_sequence_number):
  '''
  Indicates to a middle `Node` in a migration that an input node has now swapped to using it instead of
  the previous receiver.

  :param int node_id: The id of the input node that has swapped to using the middle node.
  :param int first_live_sequence_number: The first sequence number that will be sent to the middle node after
    the swap.
  '''
  return {'type': 'swapped_to_duplicate', 'node_id': node_id, 'first_live_sequence_number': first_live_sequence_number}


def finish_duplicating(receiver_id):
  '''
  This message is sent to a node that is duplicating its sends to inform it that it no longer need send messages
  to the old receivers.

  :param str receiver: The id of the receiver `Node` which sent the original `start_duplicating` message to begin duplication.
  '''
  return {'type': 'finish_duplicating', 'receiver_id': receiver_id}


def finished_duplicating():
  '''
  This message is sent to inform a migrator that an input has stopped sending messages as it was before
  the migration started, and is now only sending messages the new way.
  '''
  return {'type': 'finished_duplicating'}
