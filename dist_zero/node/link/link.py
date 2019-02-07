from ..node import Node


class LinkNode(Node):
  def __init__(self, node_id, height, left_is_data, right_is_data, leaf_config, controller):
    self.id = node_id
    self._height = height
    self._left_is_data = left_is_data
    self._right_is_data = right_is_data
    self._leaf_config = leaf_config
    self._controller = controller

    self._kids = {}

  def elapse(self, ms):
    pass

  def receive(self, message, sender_id):
    #if message['type'] == 'sequence_message':
    #else:
    raise errors.InternalError("Unrecognized message type {}".format(message['type']))

  def deliver(self, message, sequence_number, sender_id):
    '''
    Abstract method for delivering new messages to this node.
    '''
    # FIXME(KK): Perhaps we should remove deliver entierly?
    raise RuntimeError('Not Implemented')
