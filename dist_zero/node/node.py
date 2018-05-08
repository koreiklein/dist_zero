import dist_zero.logging

from dist_zero import messages


class Node(object):
  '''Abstract base class for nodes'''

  def __init__(self, logger):
    self.logger = dist_zero.logging.LoggerAdapter(logger, extra={'cur_node_id': self.id})

  def send(self, receiver, message):
    self._controller.send(receiver, message, self.handle())

  def set_transport(self, other, transport=None):
    '''
    Set the transport to use when this node sends messages to node.

    :param node: The :ref:`handle` of a node.
    :type node: :ref:`handle`
    '''
    self._controller.set_transport(self.handle(), other, transport)

  def convert_transport_for(self, sender, receiver):
    '''
    Given a transport that self can use to link to kid,
    create a new transport that the other node can use to link to kid.

    :param sender: The :ref:`handle` of the node that will be sending.
    :type sender: :ref:`handle`

    :param receiver: The :ref:`handle` of the node that will be receiving.
    :type receiver: :ref:`handle`

    :return: A transport that other can use to talk to kid.
    :rtype: :ref:`transport`
    '''
    return self._controller.convert_transport_for(current_sender=self.handle(), new_sender=sender, receiver=receiver)

  def new_transport_for(self, node_id):
    '''
    Create a transport that a node can use to link to self.

    :param str node_id: The id of some node.

    :return: A transport that node can use to link to self.
    '''
    return self._controller.new_transport_for(self.id, node_id)

  def initialize(self):
    '''Called exactly once, when a node starts to run.'''
    pass

  def handle(self):
    '''
    This node's handle.

    :return: A :ref:`handle` for the current node.
    :rtype: :ref:`handle`
    '''
    raise RuntimeError('Abstract Superclass')

  def elapse(self, ms):
    '''
    Elapse ms of time on this node.

    :param int ms: A number of milliseconds.
    '''
    raise RuntimeError('Abstract Superclass')

  def receive(self, message, sender):
    '''
    Receive a message from some sender.

    :param message: A :ref:`message` from one of the senders to this node.
    :type message: :ref:`message`

    :param sender: The :ref:`handle` of the node that sent the message.
    :type sender: :ref:`handle`
    '''
    raise RuntimeError('Abstract Superclass')
