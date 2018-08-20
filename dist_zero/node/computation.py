import logging

from dist_zero import messages, ids

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

  def checkpoint(self, before=None):
    pass

  def activate_swap(self, migration_id, new_receiver_ids, kids):
    self.kids.update(kids)

    for receiver_id in new_receiver_ids:
      if receiver_id not in self._exporters:
        import ipdb
        ipdb.set_trace()
      receiver = self._exporters[receiver_id].receiver
      self.send(receiver, messages.migration.swapped_to_duplicate(migration_id, first_live_sequence_number=0))

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

  def receive(self, message, sender_id):
    if message['type'] == 'added_adjacent_leaf':
      if message['variant'] == 'input':
        possible_receivers = list(self.kids.values())
        if len(possible_receivers) == 0:
          raise errors.InternalError("ComputationNode did not have a child to which to connect its new adjacent.")
        # FIXME(KK): Instead of the below, always pick the 'best' child (in some reasonable sense).
        receiver = possible_receivers[0]
        node_id = ids.new_id('SumNode_input_adjacent')
        self._controller.spawn_node(
            messages.sum.sum_node_config(
                node_id=node_id,
                senders=[],
                parent=self.new_handle(node_id),
                receivers=[self.transfer_handle(receiver, node_id)],
                input_node=self.transfer_handle(handle=message['kid'], for_node_id=node_id),
            ))
      elif message['variant'] == 'output':
        possible_senders = list(self.kids.values())
        if len(possible_senders) == 0:
          raise errors.InternalError("ComputationNode did not have a child to which to connect its new adjacent.")
        # FIXME(KK): Instead of the below, always pick the 'best' child (in some reasonable sense).
        sender = possible_senders[0]

        node_id = ids.new_id('SumNode_output_adjacent')
        self._controller.spawn_node(
            messages.sum.sum_node_config(
                node_id=node_id,
                parent=self.new_handle(node_id),
                senders=[self.transfer_handle(sender, node_id)],
                receivers=[],
                output_node=self.transfer_handle(handle=message['kid'], for_node_id=node_id),
            ))
      else:
        raise errors.InternalError("Unrecognized variant {}".format(message['variant']))
    else:
      super(ComputationNode, self).receive(message=message, sender_id=sender_id)

  def handle_api_message(self, message):
    if message['type'] == 'get_kids':
      return self.kids
    elif message['type'] == 'get_senders':
      return self._senders
    else:
      return super(ComputationNode, self).handle_api_message(message)
