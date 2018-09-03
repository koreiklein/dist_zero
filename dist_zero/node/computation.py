import logging

from dist_zero import messages, ids, errors

from .node import Node

logger = logging.getLogger(__name__)


class ComputationNode(Node):
  def __init__(self, node_id, parent, height, migrator_config, controller):
    self.id = node_id
    self._controller = controller

    self.parent = parent
    self.height = height
    self.kids = {}
    self._kid_n_senders = {}
    self._kid_n_receivers = {}

    self._exporters = {}

    self._senders = {sender['id']: sender for sender in migrator_config['senders']}

    self._initial_migrator_config = migrator_config
    self._initial_migrator = None # It will be initialized later.

    super(ComputationNode, self).__init__(logger)

    for sender_id in self._senders.keys():
      self._deltas.add_sender(sender_id)

  def is_data(self):
    return False

  def set_graph(self, graph):
    self._graph = graph

  def checkpoint(self, before=None):
    pass

  def activate_swap(self, migration_id, new_receiver_ids, kids, use_output=False, use_input=False):
    self.kids.update(kids)

  def initialize(self):
    self.logger.info(
        'Starting internal computation node {computation_node_id}', extra={
            'computation_node_id': self.id,
        })
    if self._initial_migrator_config:
      self._initial_migrator = self.attach_migrator(self._initial_migrator_config)

    self.linker.initialize()

  @staticmethod
  def from_config(node_config, controller):
    return ComputationNode(
        node_id=node_config['id'],
        parent=node_config['parent'],
        height=node_config['height'],
        migrator_config=node_config['migrator'],
        controller=controller)

  def elapse(self, ms):
    pass

  def deliver(self, message, sequence_number, sender_id):
    pass

  def send_forward_messages(self, before=None):
    return 1 + self._linker.advance_sequence_number()

  def export_to_node(self, receiver):
    if receiver['id'] in self._exporters:
      raise errors.InternalError("Already exporting to this node.", extra={'existing_node_id': receiver['id']})
    self._exporters[receiver['id']] = self.linker.new_exporter(receiver=receiver)

  def _pick_new_receiver_for_leaf(self):
    possible = list(self.kids.values())
    if len(possible) == 0:
      raise errors.InternalError("ComputationNode did not have a child to which to connect its new adjacent.")

    leaf_ids = [node for node in self._graph.nodes() if node.startswith('LeafNode')]
    leaf_adjacents = set(adjacent for leaf_id in leaf_ids for adjacent in self._graph.node_receivers(leaf_id))

    non_adjacent_kids = list(
        sorted(
            (receiver for receiver in possible if receiver['id'] not in leaf_adjacents),
            key=lambda receiver: len(self._graph.node_senders(receiver['id'])),
        ))

    if not non_adjacent_kids:
      raise errors.InternalError("There was no appropriate child for which to add the leaf adjacent node.")

    return self._controller.random.choice(non_adjacent_kids)

  def _pick_new_sender_for_leaf(self):
    possible = list(self.kids.values())
    if len(possible) == 0:
      raise errors.InternalError("ComputationNode did not have a child to which to connect its new adjacent.")

    leaf_ids = [node for node in self._graph.nodes() if node.startswith('LeafNode')]
    leaf_adjacents = set(adjacent for leaf_id in leaf_ids for adjacent in self._graph.node_senders(leaf_id))

    non_adjacent_kids = list(
        sorted(
            (sender for sender in possible if sender['id'] not in leaf_adjacents),
            key=lambda sender: len(self._graph.node_receivers(sender['id'])),
        ))

    if not non_adjacent_kids:
      raise errors.InternalError("There was no appropriate child for which to add the leaf adjacent node.")

    return self._controller.random.choice(non_adjacent_kids)

  def receive(self, message, sender_id):
    if message['type'] == 'added_adjacent_leaf':
      new_leaf = message['kid']
      if message['variant'] == 'input':
        receiver = self._pick_new_receiver_for_leaf()
        node_id = ids.new_id('SumNode_input_adjacent')
        self._graph.add_node(node_id)
        self._graph.add_node(new_leaf['id'])
        self._graph.add_edge(node_id, receiver['id'])
        self._graph.add_edge(new_leaf['id'], node_id)
        self._controller.spawn_node(
            messages.sum.sum_node_config(
                node_id=node_id,
                senders=[],
                parent=self.new_handle(node_id),
                receivers=[self.transfer_handle(receiver, node_id)],
                input_node=self.transfer_handle(handle=new_leaf, for_node_id=node_id),
            ))
      elif message['variant'] == 'output':
        sender = self._pick_new_sender_for_leaf()
        node_id = ids.new_id('SumNode_output_adjacent')
        self._graph.add_node(node_id)
        self._graph.add_node(new_leaf['id'])
        self._graph.add_edge(sender['id'], node_id)
        self._graph.add_edge(node_id, new_leaf['id'])
        self._controller.spawn_node(
            messages.sum.sum_node_config(
                node_id=node_id,
                parent=self.new_handle(node_id),
                senders=[self.transfer_handle(sender, node_id)],
                receivers=[],
                output_node=self.transfer_handle(handle=new_leaf, for_node_id=node_id),
            ))
      else:
        raise errors.InternalError("Unrecognized variant {}".format(message['variant']))
    else:
      super(ComputationNode, self).receive(message=message, sender_id=sender_id)

  def stats(self):
    return {
        'height': self.height,
        'n_retransmissions': self.linker.n_retransmissions,
        'n_reorders': self.linker.n_reorders,
        'n_duplicates': self.linker.n_duplicates,
        'sent_messages': self.linker.least_unused_sequence_number,
        'acknowledged_messages': self.linker.least_unacknowledged_sequence_number(),
    }

  def handle_api_message(self, message):
    if message['type'] == 'get_kids':
      return self.kids
    elif message['type'] == 'get_senders':
      return self._senders
    elif message['type'] == 'get_receivers':
      return {sender_id: exporter.receiver for sender_id, exporter in self._exporters.items()}
    else:
      return super(ComputationNode, self).handle_api_message(message)
