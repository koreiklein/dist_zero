import dist_zero.logging

from dist_zero import messages


class Node(object):
  '''Abstract base class for nodes'''

  def __init__(self, logger):
    self.logger = dist_zero.logging.LoggerAdapter(logger, extra={'cur_node_id': self.id})

  def send(self, receiver, message):
    self._controller.send(node_handle=receiver, message=message, sending_node_id=self.id)

  def convert_handle_for_existing_node(self, existing_handle, new_node_handle):
    result = dict(existing_handle)
    return result

  def convert_handle_for_new_node(self, handle, new_node_id):
    '''
    :param handle: A :ref:`handle` that self can use to talk to another node.
    :type handle: :ref:`handle`
    :param str new_node_id: The id of node that is not yet spawned.

    :return: A :ref:`handle` that the newly spawned node will be able to use to talk to the node referenced by handle.
    :rtype: :ref:`handle`
    '''
    result = dict(handle)
    return result

  def fresh_handle(self, other_node_id):
    '''
    Given the id of a node that has *not yet been spawned*, generate a handle
    to include in that node's config so that once it starts, it can connect to self.

    This method should be used by parent spawning nodes to generate proper configs for their kids.

    :param str other_node_id: The id of a node that has not yet been spawned.
    '''
    return self._handle(transport=self._controller.fresh_transport_for(local_node=self, new_node_id=other_node_id))

  def connect_handle(self, other_node):
    '''
    Given a handle for sending from self to other_node,
    produce a companion handle for sending from other_node to self.

    :param other_node: The :ref:`handle` of another node.
    :type other_node: :ref:`handle`
    '''
    return self._handle(transport=self._controller.new_transport_for(self, other_node))

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
