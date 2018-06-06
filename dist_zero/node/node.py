import dist_zero.logging

from dist_zero import messages


class Node(object):
  '''Abstract base class for nodes'''

  def __init__(self, logger):
    self.logger = dist_zero.logging.LoggerAdapter(logger, extra={'cur_node_id': self.id})

  def send(self, receiver, message):
    self._controller.send(node_handle=receiver, message=message, sending_node_id=self.id)

  def new_handle(self, for_node_id):
    '''
    Create a new handle for sending to self.

    :param str for_node_id: The id of the node that will be sending via the new handle.
    :return: A :ref:`handle` that the ``for_node`` can use to send messages to self.
    :rtype: :ref:`handle`
    '''
    return self._handle(transport=self._controller.new_transport(node=self, for_node_id=for_node_id))

  def transfer_handle(self, handle, for_node_id):
    '''
    Given a handle for use by self, create a new handle for use by another node.

    :param handle: A :ref:`handle` that self can use to send to another node.
    :type handle: :ref:`handle`
    :param str for_node_id: The id of some other node.

    :return: A :ref:`handle` that the ``for_node`` will be able to use to send to the node referenced by handle.
    :rtype: :ref:`handle`
    '''
    return {
        'type': handle['type'],
        'id': handle['id'],
        'controller_id': handle['controller_id'],
        'transport': self._controller.transfer_transport(transport=handle['transport'], for_node_id=for_node_id),
    }

  def _handle(self, transport):
    return {
        'type': self.__class__.__name__,
        'id': self.id,
        'controller_id': self._controller.id,
        'transport': transport,
    }

  def initialize(self):
    '''Called exactly once, when a node starts to run.'''
    pass

  def elapse(self, ms):
    '''
    Elapse ms of time on this node.

    :param int ms: A number of milliseconds.
    '''
    raise RuntimeError('Abstract Superclass')

  def receive(self, message, sender_id):
    '''
    Receive a message from some sender.

    :param message: A :ref:`message` from one of the senders to this node.
    :type message: :ref:`message`

    :param str sender_id: The id of the node that sent the message.
    '''
    raise RuntimeError('Abstract Superclass')
