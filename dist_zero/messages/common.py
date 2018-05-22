'''
Common code for all messages modules.
Certain common messages go here as well.
'''

ENCODING = 'utf-8'
'''
The encoding to use for messages.
This should be a string understood by the python internals that operate on encodings.
'''


def added_link(node, direction, transport):
  '''
  Inform a node that it is now linked to a new node.

  :param node: The handle of the new node
  :type node: :ref:`handle`
  :param str direction: 'sender' or 'receiver' depending respectively on whether the newly added node will
    send to or receive from the node getting this message.
  :param transport: Transport data for communicating with node.
  :type transport: :ref:`transport`
  '''
  return {'type': 'added_link', 'node': node, 'direction': direction, 'transport': transport}
