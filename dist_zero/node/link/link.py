import logging

from dist_zero import errors

from ..node import Node
from . import link_leaf

logger = logging.getLogger(__name__)


class LinkNode(Node):
  def __init__(self, node_id, height, left_is_data, right_is_data, leaf_config, controller):
    self.id = node_id
    self._height = height
    self._left_is_data = left_is_data
    self._right_is_data = right_is_data
    self._leaf_config = leaf_config
    self._controller = controller

    self._kids = {}

    # FIXME(KK): This is all specific to summing.  Please remove it once leaves implement general reactive graphs.
    self._current_state = 0
    '''For when sum link nodes (of height 0) track their internal state'''

    super(LinkNode, self).__init__(logger)

  @staticmethod
  def from_config(node_config, controller):
    return LinkNode(
        node_id=node_config['id'],
        height=node_config['height'],
        left_is_data=node_config['left_is_data'],
        right_is_data=node_config['right_is_data'],
        leaf_config=node_config['leaf_config'],
        controller=controller)

  def elapse(self, ms):
    pass

  # FIXME(KK): Much of the below was copied from an old LinkNode implementation and is specific to link networks
  # that always sum their inputs.  Consider removing/rewriting much of it

  def handle_api_message(self, message):
    # FIXME(KK): This logic is from the old LinkNode code and is horribly broken.  Fix it!
    if message['type'] == 'get_kids':
      return {key: value for key, value in self.kids.items() if value is not None}
    elif message['type'] == 'get_senders':
      return {importer.sender_id: importer.sender for importer in self._importers.values()}
    elif message['type'] == 'get_receivers':
      return self._receivers if self._receivers is not None else {}
    else:
      return super(LinkNode, self).handle_api_message(message)

  def stats(self):
    return {
        'height': self._height,
        'n_retransmissions': self.linker.n_retransmissions,
        'n_reorders': self.linker.n_reorders,
        'n_duplicates': self.linker.n_duplicates,
        'sent_messages': self.linker.least_unused_sequence_number,
        'acknowledged_messages': self.linker.least_unacknowledged_sequence_number(),
    }

  def receive(self, message, sender_id):
    #if message['type'] == 'sequence_message':
    #else:
    super(LinkNode, self).receive(message=message, sender_id=sender_id)

  def deliver(self, message, sequence_number, sender_id):
    self._deltas.add_message(sender_id=sender_id, sequence_number=sequence_number, message=message)

  def start_leaf(self):
    if self._height == 0:
      self._leaf = link_leaf.from_config(leaf_config=self._leaf_config, node=self)
      self._controller.periodically(LinkNode.SEND_INTERVAL_MS,
                                    lambda: self._maybe_send_forward_messages(LinkNode.SEND_INTERVAL_MS))

  def _maybe_send_forward_messages(self, ms):
    '''Called periodically to give leaf nodes an opportunity to send their messages.'''
    if not self.deltas_only and \
        self._deltas.has_data():

      self.send_forward_messages()

    self.linker.elapse(ms)

  def send_forward_messages(self, before=None):
    delta_messages = self._deltas.pop_deltas(before=before)

    if not delta_messages:
      return self.least_unused_sequence_number
    else:
      self._current_state = self._leaf.process_increment(self._current_state, delta_messages)
      return self.least_unused_sequence_number
