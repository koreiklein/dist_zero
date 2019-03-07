import logging

from dist_zero import messages, errors
from dist_zero.node.node import Node

logger = logging.getLogger(__name__)


class ProgramNode(Node):
  def __init__(self, node_id, controller):
    self.id = node_id

    self._spy_key_to_ds_id = None # Should be populated by the transaction that starts this `ProgramNode`

    # self._datasets and self._links should both be populated by the transaction that starts this `ProgramNode`
    self._datasets = None # Map from dataset root `DataNode` id to its handle
    self._links = None # Map from link root `LinkNode` id to its handle

    self._controller = controller

    super(ProgramNode, self).__init__(logger)

  @staticmethod
  def from_config(node_config, controller):
    return ProgramNode(node_id=node_config['id'], controller=controller)

  def elapse(self, ms):
    pass

  def deliver(self, message, sequence_number, sender_id):
    pass

  def receive(self, message, sender_id):
    super(ProgramNode, self).receive(message=message, sender_id=sender_id)

  def handle_api_message(self, message):
    if message['type'] == 'get_spy_roots':
      return {key: self._datasets[ds_id] for key, ds_id in self._spy_key_to_ds_id.items()}
    else:
      return super(ProgramNode, self).handle_api_message(message)
