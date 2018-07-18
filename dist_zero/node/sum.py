import logging

from dist_zero import messages, errors, ids, migration, deltas, settings
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

  def __init__(self, node_id, senders, receivers, input_node, output_node, controller, migrator_config=None):
    '''
    :param str node_id: The node id for this node.
    :param list senders: A list of :ref:`handle` of the nodes sending increments
    :param list receivers: A list of :ref:`handle` of the nodes to receive increments

    :param input_node: The :ref:`handle` of the input node to this node if it has one.
    :type input_node: :ref:`handle` or None

    :param output_node: The :ref:`handle` of the output node to this node if it has one.
    :type output_node: :ref:`handle` or None

    :param object migrator_config: Configuration for an initializing migrator, or None if the node
      is not being initialized as part of a migration.

    :param `MachineController` controller: the controller for this node's machine.
    '''
    self._controller = controller

    self.id = node_id

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
    self._deltas = deltas.Deltas()
    self._unsent_time_ms = 0
    self._now_ms = 0

    self.deltas_only = False
    '''
    When true, this node should never apply deltas to its current state.  It should collect them in the deltas
    map instead.
    '''

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

  def initialize(self):
    self.logger.info(
        'Starting sum node {sum_node_id}. input={input_node_id}, output={output_node_id}',
        extra={
            'sum_node_id': self.id,
            'input_node_id': self._input_importer.sender_id if self._input_importer is not None else None,
            'output_node_id': self._output_exporter.receiver_id if self._output_exporter is not None else None,
        })
    if self._initial_migrator_config:
      self._initial_migrator = self._attach_migrator(self._initial_migrator_config)

    self.linker.initialize()

  @staticmethod
  def from_config(node_config, controller):
    return SumNode(
        node_id=node_config['id'],
        senders=node_config['senders'],
        receivers=node_config['receivers'],
        output_node=node_config['output_node'],
        input_node=node_config['input_node'],
        migrator_config=node_config['migrator'],
        controller=controller)

  def deliver(self, message, sequence_number, sender_id):
    '''
    Called by `Importer` instances in self._importers to deliver messages to self.
    Also called for an edge sum node adjacent to an input_node when the input node triggers incrementing the sum.
    '''
    # Don't update any internal state just yet, but wait until the next sequence number is generated.
    self._deltas.add_message(sender_id=sender_id, sequence_number=sequence_number, message=message)

  def _attach_migrator(self, migrator_config):
    migration_id = migrator_config['migration']['id']
    if migration_id in self.migrators:
      self.logger.error(
          "There is already a migration running on {cur_node_id} for migration {migration_id}",
          extra={'migration_id': migration_id})
    else:
      migrator = migration.migrator_from_config(migrator_config=migrator_config, node=self)
      self.migrators[migration_id] = migrator
      migrator.initialize()

  def remove_migrator(self, migration_id):
    '''Remove a migrator for self.migrators.'''
    self.migrators.pop(migration_id)

  def receive(self, sender_id, message):
    if message['type'] == 'sequence_message':
      self.linker.receive_sequence_message(message['value'], sender_id)
    elif message['type'] == 'attach_migrator':
      self._attach_migrator(message['migrator_config'])
    elif message['type'] == 'migration':
      migration_id, migration_message = message['migration_id'], message['message']
      if migration_id not in self.migrators:
        # Possible, when a migration was removed at about the same time as some of the last few
        # acknowledgement or retransmission messages came through.
        self.logger.warning(
            "Got a migration message for a migration which is not running on this node.",
            extra={
                'migration_id': migration_id,
                'migration_message_type': migration_message['type']
            })
      else:
        self.migrators[migration_id].receive(sender_id=sender_id, message=migration_message)
    elif message['type'] == 'set_input':
      if self._input_importer is not None:
        raise errors.InternalError("Can not set a new input node when one already exists.")
      self._input_importer = self.linker.new_importer(message['input_node'])
      self._deltas.add_sender(self._input_importer.sender_id)
    elif message['type'] == 'set_output':
      if self._output_exporter is not None:
        raise errors.InternalError("Can not set a new output node when one already exists.")
      self._output_exporter = self.linker.new_exporter(message['output_node'])
    elif message['type'] == 'added_adjacent_leaf':
      if message['variant'] == 'input':
        if self._input_importer is not None:
          node_id = ids.new_id('SumNode_input_kid')
          self_handle = self.new_handle(node_id)
          self._controller.spawn_node(
              messages.sum.sum_node_config(
                  node_id=node_id,
                  senders=[],
                  receivers=[self_handle],
                  input_node=self.transfer_handle(handle=message['kid'], for_node_id=node_id),
              ))
      elif message['variant'] == 'output':
        if self._output_exporter is not None:
          node_id = ids.new_id('SumNode_output_kid')
          self_handle = self.new_handle(node_id)
          self._controller.spawn_node(
              messages.sum.sum_node_config(
                  node_id=node_id,
                  senders=[self_handle],
                  receivers=[],
                  output_node=self.transfer_handle(handle=message['kid'], for_node_id=node_id),
              ))
      else:
        raise errors.InternalError("Unrecognized variant {}".format(message['variant']))
    elif message['type'] == 'connect_node':
      node = message['node']
      direction = message['direction']

      if direction == 'sender':
        self.import_from_node(node)
      elif direction == 'receiver':
        if node['id'] in self._exporters:
          raise errors.InternalError("Received connect_node for an exporter that had already been added.")
        self._exporters[node['id']] = self.linker.new_exporter(receiver=node)
      else:
        raise errors.InternalError("Unrecognized direction parameter '{}'".format(direction))
    else:
      self.logger.error("Unrecognized message {bad_msg}", extra={'bad_msg': message})

  def import_from_node(self, node, first_sequence_number=0):
    '''
    Start importing from node.

    :param node: The :ref:`handle` of a `Node` that should now be sending to self.
    :type node: :ref:`handle`
    '''
    if node['id'] in self._importers:
      raise errors.InternalError("Received connect_node for an importer that had already been added.")
    self._importers[node['id']] = self.linker.new_importer(sender=node, first_sequence_number=first_sequence_number)
    self._deltas.add_sender(node['id'])

  def elapse(self, ms):
    self._unsent_time_ms += ms

    if not self.deltas_only and \
        self._deltas.has_data() and \
        self._unsent_time_ms > SumNode.SEND_INTERVAL_MS:

      self.logger.info("current n_senders = {n_senders}", extra={'n_senders': len(self._importers)})

      SENDER_LIMIT = self.system_config['SUM_NODE_SENDER_LIMIT']
      if len(self._importers) >= SENDER_LIMIT:
        if not self.migrators:
          self.logger.info("Hitting sender limit of {sender_limit} senders", extra={'sender_limit': SENDER_LIMIT})
          self._spawn_new_senders_migration()

      self.send_forward_messages()

    self._now_ms += ms

    for migrator in self.migrators.values():
      migrator.elapse(ms)

    self.linker.elapse(ms)

  def _spawn_new_senders_migration(self):
    '''
    Trigger a migration to spawn new sender nodes between self and its current senders.
    '''
    node_id = ids.new_id('MigrationNode_add_layer_before_sum_node')
    insertion_node_configs, partition = self._new_sender_configs()
    reverse_partition = {
        importer.sender_id: middle_node_id
        for middle_node_id, importers in partition.items() for importer in importers
    }
    return self._controller.spawn_node(
        node_config=messages.migration.migration_node_config(
            node_id=node_id,
            source_nodes=[(self.transfer_handle(importer.sender, node_id),
                           messages.migration.source_migrator_config(
                               exporter_swaps=[(self.id, [reverse_partition[importer.sender_id]])], ))
                          for importer in self._importers.values()],
            sink_nodes=[(self.new_handle(node_id),
                         messages.migration.sink_migrator_config(
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

  def _new_sender_configs(self):
    '''
    Calculate which middle nodes to spawn in order to protect the current node against a high load from the leftmost
    sender nodes, and return a pair
      (the middle nodes' configs, the partition assigning to each new id the list of importers it will use)

    Side effect: update self._partition to map each middle node id to the leftmost nodes that will send to it
      once we reach the terminal state.
    '''
    n_new_nodes = self.system_config['SUM_NODE_SPLIT_N_NEW_NODES']
    n_senders_per_node = len(self._importers) // n_new_nodes

    importers = list(self._importers.values())

    partition = {}

    for i in range(n_new_nodes):
      new_id = ids.new_id('SumNode_middle_for_migration')

      partition[new_id] = importers[i * n_senders_per_node:(i + 1) * n_senders_per_node]

    new_node_ids = list(partition.keys())

    # allocate the remaining senders as evenly as possible
    for j, extra_importer in enumerate(importers[n_new_nodes * n_senders_per_node:]):
      partition[new_node_ids[j]].append(extra_importer)

    configs = [
        messages.sum.sum_node_config(
            node_id=node_id,
            senders=[],
            receivers=[],
            migrator=messages.migration.insertion_migrator_config(
                senders=[self.transfer_handle(importer.sender, for_node_id=node_id) for importer in importers],
                receivers=[self_handle],
            ),
        ) for node_id, importers in partition.items() for self_handle in [self.new_handle(node_id)]
    ]
    return configs, partition

  def send_forward_messages(self, before=None):
    '''
    Generate a new sequence number, combine deltas into an update message, and send it on all exporters.
    :param dict[str, int] before: An optional dictionary mapping sender ids to sequence_numbers.
      If provided, process only up to the provided sequence number for each sender id.
    :return: the next unused sequence number
    :rtype: int
    '''
    unsent_total = self._deltas.pop_deltas(before=before)

    self.logger.debug(
        "Sending new increment of {unsent_total} to all {n_receivers} receivers",
        extra={
            'unsent_total': unsent_total,
            'n_receivers': len(self._exporters)
        })

    sequence_number = self.linker.advance_sequence_number()
    self._send_increment(increment=unsent_total, sequence_number=sequence_number)
    self._current_state += unsent_total
    return sequence_number + 1

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

  def handle_api_message(self, message):
    if message['type'] == 'spawn_new_senders':
      return self._spawn_new_senders_migration()
    else:
      return super(SumNode, self).handle_api_message(message)
