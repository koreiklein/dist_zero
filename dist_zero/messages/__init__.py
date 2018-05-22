'''
Functions to build standard messages.
'''
from .common import ENCODING

from . import common, migration, sum, machine, io

# Actions


def activate_input(receiver, transport):
  '''
  Activates an input node when its edge node has been set.

  :param receiver: The :ref:`handle` of the node to be the receiver.
  :type receiver: :ref:`handle`

  :param transport: A :ref:`transport` for talking to receiver
  :type transport: :ref:`transport`
  '''
  return {'type': 'activate_input', 'receiver': receiver, 'transport': transport}


def activate_output(sender, transport):
  '''
  Activates an output node when its edge node has been set.

  :param sender: The :ref:`handle` of the node to be the sender.
  :type sender: :ref:`handle`

  :param transport: A :ref:`transport` for talking to sender
  :type transport: :ref:`transport`
  '''
  return {'type': 'activate_output', 'sender': sender, 'transport': transport}
