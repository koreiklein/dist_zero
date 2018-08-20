import logging

from dist_zero import messages, errors, ids, deltas, settings, misc
from .node import Node

logger = logging.getLogger(__name__)


class SumNode(Node):
  '''
  An internal node for summing all increments from its senders and forwarding the total to its receivers.

  Each `SumNode` is of one of three types

  * input `SumNode`, which receive as the adjacent node to an `LeafNode` or `InternalNode` and send to ``receivers``.
    These nodes have an ``input_node`` but no ``output_node`` and some nonempty list of ``receivers``.
  * output `SumNode`, which send as the adjacent node to an `LeafNode` or `InternalNode` and receive from ``senders``.
    These nodes have an ``output_node`` but no ``input_node`` and some nonempty list of ``senders``.
  * internal `SumNode`, which receive from senders and send to receivers.  These nodes have ``input_node is None``
    and ``output_node is None``

  Note that input/output `SumNode` could be for either `LeafNode` or `InternalNode`.  A `SumNode` adjacent to an 
  `InternalNode` is primarily responsible for helping to spin up new leaves, whereas a `SumNode` adjacent to a
  `LeafNode` will actually receive input messages from (or send output messages to) its adjacent leaf.
  '''

  SEND_INTERVAL_MS = 30
  '''The number of ms between sends to receivers.'''

  def __init__(self, node_id, senders, receivers, input_node, output_node, parent, controller, migrator_config=None):
    '''
    :param str node_id: The node id for this node.
    :param list senders: A list of :ref:`handle` of the nodes sending increments
    :param list receivers: A list of :ref:`handle` of the nodes to receive increments

    :param input_node: The :ref:`handle` of the input node to this node if it has one.
    :type input_node: :ref:`handle` or None

    :param output_node: The :ref:`handle` of the output node to this node if it has one.
    :type output_node: :ref:`handle` or None

    :param parent: The :ref:`handle` of the parent node that spawned this node.
    :type parent: :ref:`handle`

    :param object migrator_config: Configuration for an initializing migrator, or None if the node
      is not being initialized as part of a migration.

    :param `MachineController` controller: the controller for this node's machine.
    '''
    self._controller = controller

    self.id = node_id
    self.parent = parent

    self.height = 0

    if settings.IS_TESTING_ENV:
      self._TESTING_total_before_first_swap = 0
      self._TESTING_swapped_once = False
      self._TESTING_total_after_first_swap = 0

    # Invariants:
    #   At certain points in time, a increment message is sent to every receiver.
    #   self._unsent_time_ms is the number of elapsed milliseconds since the last such point in time
    #   self._current_state is the total amount of increment sent to receivers as of that point in time
    #     (note: the amonut is always identical for every receiver)
    #   self._deltas is the complete set of updates received since that point in time.  None of the deltas
    #     have been added to self._current_state or sent to receivers.
    self._current_state = 0
    # Map from sender_id to a list of pairs (remote_sequence_number, message)
    self._unsent_time_ms = 0
    self._now_ms = 0

    self._time_since_had_enough_receivers_ms = 0

    super(SumNode, self).__init__(logger)

    if input_node is None:
      self._input_importer = None
    else:
      self._input_importer = self.linker.new_importer(input_node)
      self._deltas.add_sender(self._input_importer.sender_id)

    self._initial_migrator_config = migrator_config
    self._initial_migrator = None # It will be initialized later.

    self._output_exporter = None if output_node is None else self.linker.new_exporter(output_node)
    self._exporters = {receiver['id']: self.linker.new_exporter(receiver) for receiver in receivers}
    self._importers = {}

    for sender in senders:
      self.import_from_node(sender)

  def is_data(self):
    return False

  def initialize(self):
    self.logger.info(
        'Starting sum node {sum_node_id}. input={input_node_id}, output={output_node_id}',
        extra={
            'sum_node_id': self.id,
            'input_node_id': self._input_importer.sender_id if self._input_importer is not None else None,
            'output_node_id': self._output_exporter.receiver_id if self._output_exporter is not None else None,
        })
    if self._initial_migrator_config:
      self._initial_migrator = self.attach_migrator(self._initial_migrator_config)

    self.linker.initialize()

  def send_forward_messages(self, before=None):
    '''
    Generate a new sequence number, combine deltas into an update message, and send it on all exporters.

    :param dict[str, int] before: An optional dictionary mapping sender ids to sequence_numbers.
      If provided, process only up to the provided sequence number for each sender id.
    :return: the next unused sequence number
    :rtype: int
    '''
    new_state, increment, updated = self._deltas.pop_deltas(state=self._current_state, before=before)
    if not updated:
      return self.least_unused_sequence_number
    else:
      self.logger.debug("Sending new increment of {increment}.", extra={'increment': increment})
      self._current_state = new_state
      sequence_number = self.linker.advance_sequence_number()
      self._send_increment(increment=increment, sequence_number=sequence_number)
      return sequence_number + 1

  @staticmethod
  def from_config(node_config, controller):
    return SumNode(
        node_id=node_config['id'],
        senders=node_config['senders'],
        receivers=node_config['receivers'],
        output_node=node_config['output_node'],
        input_node=node_config['input_node'],
        parent=node_config['parent'],
        migrator_config=node_config['migrator'],
        controller=controller)

  def deliver(self, message, sequence_number, sender_id):
    '''
    Called by `Importer` instances in self._importers to deliver messages to self.
    Also called for an edge sum node adjacent to an input_node when the input node triggers incrementing the sum.
    '''
    # Don't update any internal state just yet, but wait until the next sequence number is generated.
    self._deltas.add_message(sender_id=sender_id, sequence_number=sequence_number, message=message)

  def receive(self, sender_id, message):
    if message['type'] == 'set_input':
      if self._input_importer is not None:
        raise errors.InternalError("Can not set a new input node when one already exists.")
      self._input_importer = self.linker.new_importer(message['input_node'])
      self._deltas.add_sender(self._input_importer.sender_id)
    elif message['type'] == 'set_output':
      if self._output_exporter is not None:
        raise errors.InternalError("Can not set a new output node when one already exists.")
      self._output_exporter = self.linker.new_exporter(message['output_node'])
    elif message['type'] == 'connect_node':
      node = message['node']
      direction = message['direction']

      if direction == 'sender':
        if node['id'] not in self._importers:
          self.import_from_node(node)
      elif direction == 'receiver':
        if node['id'] not in self._exporters:
          self.export_to_node(node)
      else:
        raise errors.InternalError("Unrecognized direction parameter '{}'".format(direction))
    elif message['type'] == 'adjacent_has_split':
      # Spawn a new adjacent for the newly spawned io node and remove any kids stolen from self.
      node_id = ids.new_id('SumNode_adjacent_for_split')
      new_node = message['new_node']
      self._controller.spawn_node(
          messages.sum.sum_node_config(
              node_id=node_id,
              senders=[new_node],
              receivers=[self.transfer_handle(exporter.receiver, node_id) for exporter in self._exporters.values()],
          ))
    else:
      super(SumNode, self).receive(message=message, sender_id=sender_id)

  def import_from_node(self, node, first_sequence_number=0):
    '''
    Start importing from node.

    :param node: The :ref:`handle` of a `Node` that should now be sending to self.
    :type node: :ref:`handle`
    '''
    if node['id'] in self._importers:
      raise errors.InternalError("Already importing from this node.", extra={'existing_node_id': node['id']})
    self._importers[node['id']] = self.linker.new_importer(sender=node, first_sequence_number=first_sequence_number)
    self._deltas.add_sender(node['id'])

  def export_to_node(self, receiver):
    if receiver['id'] not in self._exporters:
      self._exporters[receiver['id']] = self.linker.new_exporter(receiver=receiver)

  def elapse(self, ms):
    self._unsent_time_ms += ms
    self._time_since_had_enough_receivers_ms += ms

    if not self.deltas_only and \
        self._deltas.has_data() and \
        self._unsent_time_ms > SumNode.SEND_INTERVAL_MS:

      self._check_limits()
      self.send_forward_messages()

    self._now_ms += ms

    for migrator in self.migrators.values():
      migrator.elapse(ms)

    self.linker.elapse(ms)

  def _check_limits(self):
    '''Test for various kinds of load problems and take appropriate actions to remedy them.'''
    SENDER_LIMIT = self.system_config['SUM_NODE_SENDER_LIMIT']
    TOO_FEW_RECEIVERS_TIME_MS = self.system_config['SUM_NODE_TOO_FEW_RECEIVERS_TIME_MS']
    SUM_NODE_RECEIVER_LOWER_LIMIT = self.system_config['SUM_NODE_RECEIVER_LOWER_LIMIT']
    SUM_NODE_SENDER_LOWER_LIMIT = self.system_config['SUM_NODE_SENDER_LOWER_LIMIT']

    if len(self._exporters) >= SUM_NODE_RECEIVER_LOWER_LIMIT or len(self._importers) >= SUM_NODE_SENDER_LOWER_LIMIT:
      self._time_since_had_enough_receivers_ms = 0

    elif self._time_since_had_enough_receivers_ms > TOO_FEW_RECEIVERS_TIME_MS and \
        self._input_importer is None \
        and self._output_exporter is None:
      self._time_since_had_enough_receivers_ms = 0
    self.logger.info("current n_senders = {n_senders}", extra={'n_senders': len(self._importers)})

    if len(self._importers) >= SENDER_LIMIT:
      if not self.migrators:
        self.logger.info("Hitting sender limit of {sender_limit} senders", extra={'sender_limit': SENDER_LIMIT})
        self._spawn_new_senders_migration()

  def _spawn_new_senders_migration(self):
    '''
    Trigger a migration to spawn new sender nodes between self and its current senders.
    '''
    partition = {
        ids.new_id('SumNode_middle_for_migration'): importers
        for importers in misc.partition(
            items=list(self._importers.values()), n_buckets=self.system_config['SUM_NODE_SPLIT_N_NEW_NODES'])
    }
    reverse_partition = {
        importer.sender_id: middle_node_id
        for middle_node_id, importers in partition.items() for importer in importers
    }

    insertion_node_configs = [
        messages.sum.sum_node_config(
            node_id=nid,
            senders=[],
            receivers=[],
            migrator=messages.migration.insertion_migrator_config(
                senders=[self.transfer_handle(importer.sender, for_node_id=nid) for importer in importers],
                receivers=[self_handle],
            ),
        ) for nid, importers in partition.items() for self_handle in [self.new_handle(nid)]
    ]

    node_id = ids.new_id('MigrationNode_add_layer_before_sum_node')

    return self._controller.spawn_node(
        node_config=messages.migration.migration_node_config(
            node_id=node_id,
            source_nodes=[(self.transfer_handle(importer.sender, node_id),
                           messages.migration.source_migrator_config(
                               will_sync=False,
                               exporter_swaps=[(self.id, [reverse_partition[importer.sender_id]])],
                           )) for importer in self._importers.values()],
            sink_nodes=[(self.new_handle(node_id),
                         messages.migration.sink_migrator_config(
                             will_sync=True,
                             new_flow_sender_ids=[
                                 insertion_node_config['id'] for insertion_node_config in insertion_node_configs
                             ],
                             old_flow_sender_ids=list(self._importers.keys()),
                         ))],
            removal_nodes=[],
            insertion_node_configs=insertion_node_configs,
            sync_pairs=[
                # A single pair, indicating that self should sync to the insertion nodes.
                (self.new_handle(node_id),
                 [insertion_node_config['id'] for insertion_node_config in insertion_node_configs])
            ],
        ))

  def _send_increment(self, increment, sequence_number):
    if settings.IS_TESTING_ENV:
      if self._TESTING_swapped_once:
        self._TESTING_total_after_first_swap += increment
      else:
        self._TESTING_total_before_first_swap += increment

    for exporter in self._exporters.values():
      exporter.export_message(
          message=messages.sum.increment(amount=increment),
          sequence_number=sequence_number,
      )

    if self._output_exporter and self._output_exporter.receiver_id.startswith('LeafNode'):
      message = messages.io.output_action(increment)
      self._output_exporter.export_message(message=message, sequence_number=sequence_number)

    self._unsent_time_ms = 0

  def switch_flows(self, migration_id, old_exporters, new_exporters, new_receivers):
    for receiver_id in old_exporters:
      exporter = self._exporters.pop(receiver_id)
      self.send(exporter.receiver,
                messages.migration.swapped_from_duplicate(
                    migration_id, first_live_sequence_number=exporter.internal_sequence_number))

    for exporter in new_exporters.values():
      self.send(exporter.receiver,
                messages.migration.swapped_to_duplicate(
                    migration_id, first_live_sequence_number=exporter.internal_sequence_number))

    self._exporters.update(new_exporters)

  def activate_swap(self, migration_id, new_receiver_ids, kids):
    if len(kids) != 0:
      raise errors.InternalError("Sum nodes should never be passed kids by a migrator.")

    for receiver_id in new_receiver_ids:
      exporter = self._exporters[receiver_id]
      self.send(exporter.receiver,
                messages.migration.swapped_to_duplicate(
                    migration_id, first_live_sequence_number=exporter._internal_sequence_number))

  def checkpoint(self, before=None):
    self.send_forward_messages(before=before)

  def remove_migrator(self, migration_id):
    for nid in self.migrators[migration_id]._receivers:
      self._exporters[nid]._migration_id = None

    super(SumNode, self).remove_migrator(migration_id)

  def sink_swap(self, deltas, old_sender_ids, new_senders, new_importers, linker):
    self._deltas = deltas
    self.linker.remove_importers(old_sender_ids)
    self.linker.absorb_linker(linker)
    self._importers = new_importers

  def handle_api_message(self, message):
    if message['type'] == 'spawn_new_senders':
      return self._spawn_new_senders_migration()
    elif message['type'] == 'get_senders':
      return {sender_id: importer.sender for sender_id, importer in self._importers.items()}
    elif message['type'] == 'get_receivers':
      return {receiver_id: exporter.receiver for receiver_id, exporter in self._exporters.items()}
    else:
      return super(SumNode, self).handle_api_message(message)
