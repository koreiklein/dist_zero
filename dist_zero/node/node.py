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

  def convert_transport_for(self, other, transport):
    '''
    Given a transport that self can use to link to kid,
    create a new transport that the other node can use to link to kid.

    :param other: The :ref:`handle` of a node.
    :type other: :ref:`handle`

    :param object transport: A transport that self can use to talk to kid.

    :return: A transport that other can use to talk to kid.
    :rtype: object
    '''
    # NOTE(KK): At the moment (4/8/2018), there is nothing specific to the sending node in a transport,
    # so it is correct safe not to modify the transport when converting.
    return transport

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
