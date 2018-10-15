import logging

from dist_zero import messages
from dist_zero.node.node import Node

from .data import DataNode

logger = logging.getLogger(__name__)


class AdopterNode(Node):
  def __init__(self, node_id, parent, adoptees, data_node_config, controller):
    self.id = node_id
    self._parent = parent
    self._controller = controller
    self._data_node_config = data_node_config

    self._pending_adoptees = {adoptee['id']: False for adoptee in adoptees}
    self._kids = {adoptee['id']: adoptee for adoptee in adoptees}

    super(AdopterNode, self).__init__(logger)

  @staticmethod
  def from_config(config, controller):
    return AdopterNode(
        adoptees=config['adoptees'],
        node_id=config['data_node_config']['id'],
        parent=config['data_node_config']['parent'],
        data_node_config=config['data_node_config'],
        controller=controller)

  def initialize(self):
    # Must adopt any kids that were added before initialization.
    for kid in self._kids.values():
      self.send(kid, messages.migration.adopt(self.new_handle(kid['id'])))

  def _added_kid(self, kid):
    kid_id = kid['id']
    self._kids[kid_id] = kid
    self._pending_adoptees[kid_id] = True
    if all(self._pending_adoptees.values()):
      new_node = DataNode.from_config(self._data_node_config, self._controller)
      new_node.set_initial_kids(self._kids)
      self._controller.change_node(self.id, new_node)
      new_node.initialize()

  def receive(self, message, sender_id):
    if message['type'] == 'hello_parent':
      self._added_kid(message['kid'])
    else:
      super(AdopterNode, self).receive(message=message, sender_id=sender_id)
