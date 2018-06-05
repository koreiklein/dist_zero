'''
Messages to be received by migration nodes.
'''


def connect_internal(node, direction):
  '''
  Inform a node internal to a computation that it is now linked to a new node either
  as a sender or a receiver.

  :param node: The handle of the new node
  :type node: :ref:`handle`
  :param str direction: 'sender' or 'receiver' depending respectively on whether the newly added node will
    send to or receive from the node getting this message.
  '''
  return {'type': 'connect_internal', 'node': node, 'direction': direction}


def middle_node_is_live():
  '''
  Indicates that a middle node is now fully synced up.
  '''
  return {'type': 'middle_node_is_live'}


def middle_node_is_duplicated():
  '''
  Indicates that a middle node is now receiving from all the proper senders.
  '''
  return {'type': 'middle_node_is_duplicated'}


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
