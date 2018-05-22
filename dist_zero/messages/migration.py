'''
Messages to be received by migration nodes.
'''


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


def start_duplicating(receiver, transport):
  '''
  This message is sent to inform a node that it should duplicate its sends to a new receiver.

  After receipt of this message, the node will send messages to the usual receivers just as before, but also
  send duplicated messages to the newly added receiver.

  :param receiver: The :ref:`handle` of the node to send the duplicates to.
  :type receiver: :ref:`handle`

  :param transport: A :ref:`transport` for talking to receiver.
  :type transport: :ref:`transport`
  '''
  return {'type': 'start_duplicating', 'receiver': receiver, 'transport': transport}


def finish_duplicating():
  '''
  This message is sent to a node that is duplicating its sends to inform it that it no longer need send messages
  to the old receivers.
  '''
  return {'type': 'finish_duplicating'}


def finished_duplicating():
  '''
  This message is sent to inform a migrator that an input has stopped sending messages as it was before
  the migration started, and is now only sending messages the new way.
  '''
  return {'type': 'finished_duplicating'}
