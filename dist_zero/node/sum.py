import logging

from dist_zero import messages, errors, ids
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

  def __init__(self, node_id, senders, receivers, spawning_migration, input_node, output_node, pending_sender_ids,
               controller):
    '''
    :param str node_id: The node id for this node.
    :param list senders: A list of :ref:`handle` of the nodes sending increments
    :param list receivers: A list of :ref:`handle` of the nodes to receive increments

    :param list pending_sender_ids: In the event that this node is starting via a migration, this is the list of
      senders that must be registered as sending duplicates in order for this node to decide that it is fully duplicated.

    :param input_node: The :ref:`handle` of the input node to this node if it has one.
    :type input_node: :ref:`handle` or None

    :param output_node: The :ref:`handle` of the output node to this node if it has one.
    :type output_node: :ref:`handle` or None

    :param spawning_migration: The :ref:`handle` of the migration spawing this `SumNode` if it has one.
    :type spawning_migration: :ref:`handle` or None

    '''
    self._controller = controller

    self.id = node_id

    self._spawning_migration = spawning_migration

    self.migrator = None

    # For middle nodes in a migration, the set of input nodes that still need to start duplicating to self.
    self._pending_sender_ids = set(pending_sender_ids)
    # For middle nodes in a migration, the set of input nodes that still need to swap to self.
    self._pending_swapped_sender_ids = set(pending_sender_ids)
    # For middle nodes in a migration, each input node that duplicates to self is mapped to the first sequence
    # number that node will send live to self after the swap is finished.
    self._input_node_id_to_first_live_sequence_number = {}

    # Invariants:
    #   At certain points in time, a increment message is sent to every receiver.
    #   self._unsent_time_ms is the number of elapsed milliseconds since the last such point in time
    #   self._current_state is the total amount of increment sent to receivers as of that point in time
    #     (note: the amonut is always identical for every receiver)
    #   self._deltas is the complete set of updates received since that point in time.  None of the deltas
    #     have been added to self._current_state or sent to receivers.
    self._current_state = 0
    # Map from sender_id to a list of pairs (remote_sequence_number, message)
    self._deltas = {}
    self._unsent_time_ms = 0
    self._now_ms = 0

    self.deltas_only = False
    '''
    When true, this node should never apply deltas to its current state.  It should collect them in the deltas
    map instead.
    '''

    self.standby_mode = False
    '''
    When true, this node is participating in a migration, and has finished syncing up self._current_state,
    but is not yet responsible for sending messages to its output node.
    '''

    self._duplicator_id_to_first_sequence_number = {}
    '''
    For middle nodes in a migration as duplicators introduce themselves.
    This map tracks for each node duplicating to self, the first sequence_number that it ever sent.
    '''

    super(SumNode, self).__init__(logger)

    if input_node is None:
      self._input_importer = None
    else:
      self._input_importer = self.linker.new_importer(input_node)
      self._deltas[self._input_importer.sender_id] = []
    self._output_exporter = None if output_node is None else self.linker.new_exporter(output_node)

    self._importers = {sender['id']: self.linker.new_importer(sender) for sender in senders}
    for sender_id in self._importers.keys():
      self._deltas[sender_id] = []
    self._exporters = {receiver['id']: self.linker.new_exporter(receiver) for receiver in receivers}

  def migration_finished(self):
    '''
    Called when the current migrator has finished migrating.

    '''
    self.migrator = None

  @staticmethod
  def from_config(node_config, controller):
    return SumNode(
        node_id=node_config['id'],
        senders=node_config['senders'],
        receivers=node_config['receivers'],
        pending_sender_ids=node_config['pending_sender_ids'],
        output_node=node_config['output_node'],
        input_node=node_config['input_node'],
        spawning_migration=node_config['spawning_migration'],
        controller=controller)

  def _maybe_middle_node_goes_live(self):
    '''
    For middle nodes that are still getting swapped_to_duplicate messages.
    Check whether both
      a) all input nodes have swapped
      and 
      b) all the messages before the first_live_sequence_numbers have been received.
    if so, inform the spawning_migration that self is live.
    '''
    if self._input_node_id_to_first_live_sequence_number and not self._pending_swapped_sender_ids:
      if all(self._importers[node_id].least_undelivered_remote_sequence_number >= flsn
             for node_id, flsn in self._input_node_id_to_first_live_sequence_number.items()):
        self.logger.info("Middle node from a migration is now going live")

        # Receive all messages before the first live sequence numbers now, without sending them to
        # any downstream receiver nodes.
        for node_id, flsn in self._input_node_id_to_first_live_sequence_number.items():
          new_pairs = []
          for rsn, delta_message in self._deltas[node_id]:
            if rsn < flsn:
              self._current_state += delta_message['amount']
            else:
              new_pairs.append((rsn, delta_message))
          self._deltas[node_id] = new_pairs

        # Go into the state for ordinary sum node behavior.
        self.deltas_only = False
        self.standby_mode = False
        self.send(self._spawning_migration,
                  messages.migration.middle_node_is_live(self._input_node_id_to_first_live_sequence_number))
        self._input_node_id_to_first_live_sequence_number = None

  def _maybe_finish_middle_node_duplication(self):
    '''
    For middle nodes that are still getting duplication messages.
    Check whether all the appropriate inputs are successfully duplicating their messages, and if so,
    inform the spawning_migration that the duplication is complete.
    '''
    if not self._pending_sender_ids:
      self.send(self._spawning_migration,
                messages.migration.middle_node_is_duplicated(self._duplicator_id_to_first_sequence_number))

      # Now that the duplications are done, this map should never be used again.  null it out.
      self._duplicator_id_to_first_sequence_number = None

      # Enter standby mode, and collect deltas.
      # PERF(KK): For cases where standby mode could take a long time, it would make some sense
      # for the nodes to apply some deltas while in standby mode to avoid collecting too many.
      # The problem with doing that, is that until the middle node has received a swapped_to_duplicate message
      # from an input node, it doesn't know which of that input node's messages are before or after that sequence number,
      # and should not accidentally apply a later message as it must be saved and sent on the middle node's exporter
      # as an increment.
      self.standby_mode = True
      self.deltas_only = True

  def deliver(self, message, sequence_number, sender_id):
    '''
    Called by `Importer` instances in self._importers to deliver messages to self.
    Also called for an edge sum node adjacent to an input_node when the input node triggers incrementing the sum.
    '''
    # Don't update any internal state just yet, but wait until the next sequence number is generated.
    self._deltas[sender_id].append((sequence_number, message))

    if self.migrator:
      self.migrator.check_trimming_trigger()

    self._maybe_middle_node_goes_live()

  def receive(self, sender_id, message):
    if message['type'] == 'sequence_message':
      self.linker.receive_sequence_message(message['value'], sender_id)
    elif message['type'] == 'set_input':
      if self._input_importer is not None:
        raise errors.InternalError("Can not set a new input node when one already exists.")
      self._input_importer = self.linker.new_importer(message['input_node'])
      self._deltas[self._input_importer.sender_id] = []
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
        if node['id'] in self._importers:
          raise errors.InternalError("Received connect_node for an importer that had already been added.")
        self._importers[node['id']] = self.linker.new_importer(sender=node)
        self._deltas[node['id']] = []
      elif direction == 'receiver':
        if node['id'] in self._exporters:
          raise errors.InternalError("Received connect_node for an exporter that had already been added.")
        self._exporters[node['id']] = self.linker.new_exporter(receiver=node)
      else:
        raise errors.InternalError("Unrecognized direction parameter '{}'".format(direction))
    elif message['type'] == 'started_duplication':
      if self._spawning_migration is None:
        raise errors.InternalError(
            "Only middle nodes from a migration should ever receive a started_duplication message.")
      else:
        node = message['node']
        if node['id'] not in self._pending_sender_ids:
          raise errors.InternalError("Only middle nodes with an appriate pending sender id"
                                     " should receive started_duplication messages.")
        else:
          # As soon as we're getting started_duplication messages, we must ensure that all updates go
          # are stored as deltas until the set_sum_total message arrives.
          self.deltas_only = True
          self._deltas[node['id']] = []
          self._pending_sender_ids.remove(node['id'])
          new_importer = self.linker.new_importer(sender=node, first_sequence_number=message['sequence_number'])
          self._importers[node['id']] = new_importer
          self._duplicator_id_to_first_sequence_number[node['id']] = message['sequence_number']
          self._maybe_finish_middle_node_duplication()
    elif message['type'] == 'finished_duplicating':
      self.migrator.finished_duplicating(sender_id)
    elif message['type'] == 'sum_node_started':
      if self.migrator:
        self.migrator.middle_node_started(message['sum_node_handle'])
    elif message['type'] == 'set_sum_total':
      self._current_state = message['total']
      unsent_total = self._combine_and_remove_deltas()
      self._unsent_time_ms = 0
      self.standby_mode = True
      self._current_state += unsent_total
      self.send(self._spawning_migration, messages.migration.middle_node_is_synced())
    elif message['type'] == 'swapped_to_duplicate':
      node_id = message['node_id']
      if node_id not in self._pending_swapped_sender_ids:
        raise errors.InternalError("node_id of recently swapped node should be found in pending_swapped_sender_ids")
      self._pending_swapped_sender_ids.remove(node_id)
      self._input_node_id_to_first_live_sequence_number[node_id] = message['first_live_sequence_number']

      self._maybe_middle_node_goes_live()
    elif message['type'] == 'swap_to_duplicate':
      self._exporters[message['node_id']].swap_to_duplicate()
    elif message['type'] == 'middle_node_is_duplicated':
      self.migrator.middle_node_duplicated(message['duplicator_id_to_first_sequence_number'], middle_node_id=sender_id)
    elif message['type'] == 'middle_node_is_synced':
      self.migrator.middle_node_synced(sender_id)
    elif message['type'] == 'middle_node_is_live':
      self.migrator.middle_node_live(sender_id, message['input_node_id_to_first_live_sequence_number'])
    elif message['type'] == 'start_duplicating':
      old_receiver_id = message['old_receiver_id']
      new_receiver = message['receiver']
      self.logger.info(
          "Starting duplication phase for {cur_node_id} . {new_receiver_id} will now receive duplicates from {old_receiver_id}.",
          extra={
              'new_receiver_id': new_receiver['id'],
              'old_receiver_id': old_receiver_id,
          })
      exporter = self._exporters[old_receiver_id]
      first_duplicated_sequence_number = self._send_forward_messages()
      exporter.duplicate(self.linker.new_exporter(new_receiver), sequence_number=first_duplicated_sequence_number)
    elif message['type'] == 'finish_duplicating':
      receiver_id = message['receiver_id']
      exporter = self._exporters[receiver_id]
      del self._exporters[receiver_id]
      new_exporter = exporter.finish_duplicating()
      self._exporters[new_exporter.receiver_id] = new_exporter
    else:
      self.logger.error("Unrecognized message {bad_msg}", extra={'bad_msg': message})

  def elapse(self, ms):
    self._unsent_time_ms += ms

    if not self._pending_sender_ids and \
        not self.standby_mode and \
        not self.deltas_only and \
        any(self._deltas.values()) and \
        self._unsent_time_ms > SumNode.SEND_INTERVAL_MS:

      self.logger.info("current n_senders = {n_senders}", extra={'n_senders': len(self._importers)})

      SENDER_LIMIT = self.system_config['SUM_NODE_SENDER_LIMIT']
      if len(self._importers) >= SENDER_LIMIT:
        self.logger.info("Hit sender limit of {sender_limit} senders", extra={'sender_limit': SENDER_LIMIT})
        if self.migrator is None:
          self._hit_sender_limit()

      self._send_forward_messages()

    self._now_ms += ms

    self.linker.elapse(ms)

  def _hit_sender_limit(self):
    self.migrator = SumNodeSenderSplitMigrator(self)
    self.migrator.start()

  def _combine_and_remove_deltas(self):
    '''
    Sum all the amounts in the deltas, remove them, and return the result.
    '''
    result = 0
    for sender_id, pairs in list(self._deltas.items()):
      for sequence_number, message in pairs:
        if message['type'] == 'input_action':
          result += message['number']
        elif message['type'] == 'increment':
          result += message['amount']
        else:
          raise errors.InternalError('Unrecognized message type "{}"'.format(message['type']))
      self._deltas[sender_id] = []

    return result

  def _send_forward_messages(self):
    '''
    Generate a new sequence number, combine deltas into an update message, and send it on all exporters.
    :return: the next unused sequence number
    :rtype: int
    '''
    if self.standby_mode:
      raise errors.InternalError("Nodes should not be sending messages on exporters while in standby mode.")

    unsent_total = self._combine_and_remove_deltas()

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
    for exporter in self._exporters.values():
      exporter.export_message(
          message=messages.sum.increment(amount=increment),
          sequence_number=sequence_number,
      )

    if self._output_exporter and self._output_exporter.receiver_id.startswith('LeafNode'):
      message = messages.io.output_action(increment)
      self._output_exporter.export_message(message=message, sequence_number=sequence_number)

    self._unsent_time_ms = 0

  def initialize(self):
    self.logger.info(
        'Starting sum node {sum_node_id}. input={input_node_id}, output={output_node_id}',
        extra={
            'sum_node_id': self.id,
            'input_node_id': self._input_importer.sender_id if self._input_importer is not None else None,
            'output_node_id': self._output_exporter.receiver_id if self._output_exporter is not None else None,
        })
    if self._spawning_migration:
      self.send(
          self._spawning_migration,
          messages.sum.sum_node_started(sum_node_handle=self.new_handle(self._spawning_migration['id'])))

    self.linker.initialize()


class SumNodeSenderSplitMigrator(object):
  '''
  For managing the way in which a sum node migrates to having new nodes between it and its senders.

  Terminology:

  The initial state before the migration:
   (large set of leftmost sender nodes) --sending_to--> current node
  The final state after the migration:
   (large set of sender nodes) --sending_to--> (smaller set of middle nodes) --sending_to--> current node

  There are thus three sets of nodes involved, the 'leftmost' sender nodes, the 'middle' nodes and the 'current' node

  The job of this migrator is to spin up an appropriate set of middle nodes in between the leftmost nodes
  and the current node.
  '''

  # Below are the states of a migrator.
  # The migrator will move linearly from each state to the next state.
  # Each state has some trigger, the condition that must be met in order to transition.  As soon as the transition
  # is met, the migrator will move into the next state.
  # Each state has an action, something that the migrator is responsible for doing while in that state.
  # Each state also has a description, which explains the state of affairs of the overall migration while the
  # migrator is in that state.

  STATE_NEW = 'NEW'
  '''The initial state of the migrator.'''

  STATE_INITIALIZING_NEW_NODES = 'INITIALIZING_NEW_NODES'
  '''
  Trigger: none.  The migrator can enter this state immediately.
  Action: The migrator tries to spawn new nodes.
  Description: The migrator is waiting for new nodes to start running.
  '''

  STATE_DUPLICATING_INPUTS = 'DUPLICATING_INPUTS'
  '''
  Trigger: The migrator has received confirmations that each new node is now running.
  Action: The migrator tries to get input nodes to duplicate their exports.
  Description: The migrator is waiting for confirmations that the input nodes are duplicating.
  '''

  STATE_SYNCING_NEW_NODES = 'SYNCING_NEW_NODES'
  '''
  Trigger: The migrator has received confirmations that all input nodes are duplicating.
  Action: The migrator attempts to bring the new nodes up to speed so that they can be treated as live.
  Description: The migrator is waiting for the new nodes to sync up with the current node they will be replacing.
  '''

  STATE_SWAPPING_OUTPUTS = 'SWAPPING_OUTPUTS'
  '''
  Trigger: The migrator has received confirmations that the new nodes are live, and can be safely relied on
  in place of the input nodes.
  Action: The migrator converts all the output nodes to rely on the new nodes instead of the input nodes.
  Description: The migrator is waiting for confirmation that all output nodes have finished relying on the input nodes
  and are safely using the new nodes instead.
  '''

  STATE_TRIMMING_INPUTS = 'TRIMMING_INPUTS'
  '''
  Trigger: The migrator has received confirmations that no nodes currently depend on the old output of the input nodes.
  Action: The migrator attempts to trim the input nodes (i.e. to stop them from sending their original messages,
  and send only to the new nodes they were duplicating to).
  Description: The migrator is waiting to be sure that all input nodes have been trimmed.
  '''

  STATE_FINISHED = 'FINISHED'
  '''
  Trigger: The migrator has received confirmations that all the input nodes are trimmed.
  Action: None.
  Description: The migration is over.
  '''

  def __init__(self, sum_node):
    '''
    :param sum_node: The underlying sum node
    :type sum_node: `SumNode`
    '''
    self.node = sum_node
    self._state = SumNodeSenderSplitMigrator.STATE_NEW

    self.n_new_nodes = 2

    self._middle_nodes = []
    '''Handles of the middle nodes'''

    self._partition = {}
    '''A map from middle node id to a list of the importers that will be moved to it'''
    self._duplicating_input_ids = set()
    '''The set of ids of input nodes that have been told to duplicate, and have not yet confirmed that they
    are done duplicating.'''

    self._middle_node_states = {}
    '''
    A map from middle node id to its state.  One of 
      'new' -- The config was generated, but the node has not yet been heard from.
      'started' -- This node has received a first initialization message from the newly started node.
      'duplicated' -- This node is now getting duplicated messages from all the appropriate senders.
      'synced' -- This node has synced up its model and can now be considered "up to date".
      'live' -- This node has received its model and is getting up to date messages from its input nodes.
    '''

    self._input_id_to_first_sequence_number = {}
    '''
    A map from input node id to the first sequence number that it duplicated to its assigned middle node.
    This map starts off empty, but will be complete as the migrator leaves STATE_DUPLICATING_INPUTS.
    '''

    self._input_node_id_to_first_live_sequence_number = {}
    '''
    A map from input node id to the first sequence number that it will send once it has swapped to its assigned
    middle node.  This map starts off empty, but will be complete as the migrator leaves STATE_SWAPPING_OUTPUTS
    '''

  def _transition_state(self, from_state, to_state):
    '''Move from one state to another.'''
    if self._state != from_state:
      raise RuntimeError("Must be in state {} to transition".format(from_state))
    self.logger.info(
        "Migration transition {from_state} -> {to_state} for {cur_node_id}",
        extra={
            'from_state': from_state,
            'to_state': to_state,
        })
    self._state = to_state

  def start(self):
    '''Start the migration.'''
    self._transition_to_initializing_middle_nodes()

  def _transition_to_initializing_middle_nodes(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_NEW,
                           SumNodeSenderSplitMigrator.STATE_INITIALIZING_NEW_NODES)

    for node_config in self._new_sender_configs():
      self.controller.spawn_node(node_config=node_config)

  def _transition_to_duplicating(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_INITIALIZING_NEW_NODES,
                           SumNodeSenderSplitMigrator.STATE_DUPLICATING_INPUTS)

    self.node.deltas_only = True
    for middle_node in self._middle_nodes:
      for importer in self._partition[middle_node['id']]:
        self._duplicating_input_ids.add(importer.sender_id)
        importer.duplicate_paired_exporters_to(middle_node)

  def _transition_to_syncing(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_DUPLICATING_INPUTS,
                           SumNodeSenderSplitMigrator.STATE_SYNCING_NEW_NODES)

    increment = 0

    for importers in self._partition.values():
      for sender_id in (i.sender_id for i in importers):
        first_sequence_number_from_middle_node = self._input_id_to_first_sequence_number[sender_id]
        remaining_pairs = []
        for rsn, message in self.node._deltas[sender_id]:
          if rsn < first_sequence_number_from_middle_node:
            increment += message['amount']
          else:
            remaining_pairs.append((rsn, message))
        self.node._deltas[sender_id] = remaining_pairs

    sequence_number = self.node.linker.advance_sequence_number()
    self.node._send_increment(increment=increment, sequence_number=sequence_number)
    self.node._current_state += increment

    middle_node_id_to_starting_total = {}
    total = self.node._current_state
    total_quotient, total_remainder = total // self.n_new_nodes, total % self.n_new_nodes
    for i, middle_node_id in enumerate(self._partition.keys()):
      # The first total_remainder nodes start with a slightly larger total
      middle_node_id_to_starting_total[middle_node_id] = total_quotient + 1 if i < total_remainder else total_quotient

    for middle_node in self._middle_nodes:
      self.node.send(middle_node, messages.sum.set_sum_total(middle_node_id_to_starting_total[middle_node['id']]))

    self.node.deltas_only = False

  def _transition_to_swapping_outputs(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_SYNCING_NEW_NODES,
                           SumNodeSenderSplitMigrator.STATE_SWAPPING_OUTPUTS)

    # Since the current node is right here, we can swap the outputs immediately and
    # transition directly into the trimming state.
    for importers in self._partition.values():
      for i in importers:
        self.node.send(i.sender, messages.migration.swap_to_duplicate(node_id=self.node.id))

  def _transition_to_trimming(self, removed_importers):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_SWAPPING_OUTPUTS,
                           SumNodeSenderSplitMigrator.STATE_TRIMMING_INPUTS)

    for importer in removed_importers:
      self.node.send(importer.sender, messages.migration.finish_duplicating(receiver_id=self.node.id))

  def _transition_to_finished(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_TRIMMING_INPUTS, SumNodeSenderSplitMigrator.STATE_FINISHED)

    self.node.migration_finished()

  def middle_node_synced(self, middle_node_id):
    '''Called when a middle node has been confirmed to be fully synced up.'''
    self.logger.info("Marking middle node as now being live", extra={'middle_node_id': middle_node_id})
    self._middle_node_states[middle_node_id] = 'synced'

    if all(state == 'synced' for state in self._middle_node_states.values()):
      self._transition_to_swapping_outputs()

  def middle_node_live(self, middle_node_id, input_node_id_to_first_live_sequence_number):
    self._middle_node_states[middle_node_id] = 'live'
    self._input_node_id_to_first_live_sequence_number.update(input_node_id_to_first_live_sequence_number)

    self.check_trimming_trigger()

  def check_trimming_trigger(self):
    '''
    While in the STATE_SWAPPING_OUTPUTS state, this method checks whether all liveness messages have
    arrived from middle nodes and all the input importers have delivered the requisite messages.
    If so, it finished swapping outputs and enters the STATE_TRIMMING_INPUTS state.
    '''
    if self._state == SumNodeSenderSplitMigrator.STATE_SWAPPING_OUTPUTS and \
        all(state == 'live'
          for state in self._middle_node_states.values()) and \
        all(sn == self.node._importers[node_id].least_undelivered_remote_sequence_number
          for node_id, sn in self._input_node_id_to_first_live_sequence_number.items()):

      # All live messages from middle nodes, and the requisite prior messages from input nodes have arrived.
      # Finish the switch to the new behavior and enter the trimming state.
      removed_importers = self._remove_importers(set(self._partition.keys()))
      self._transition_to_trimming(removed_importers)

  def _remove_importers(self, remaining_sender_ids):
    '''
    Remove importers entirely from this node.

    :param set remaining_sender_ids: The set of senders that are still sending.  All other senders should be deactivated.
    :return: The removed `Importer` instances.
    :rtype: list[`Importer`]
    '''
    new_importers = {}
    removed_importers = []
    for sender_id, importer in self.node._importers.items():
      if sender_id in remaining_sender_ids:
        new_importers[sender_id] = importer
      else:
        removed_importers.append(importer)
        self.node.linker.remove_importer(importer)

    self.logger.info(
        "Removing {n_removed_importers} importers as a migration ends",
        extra={
            'n_removed_importers': len(removed_importers),
        })

    self.node._importers = new_importers

    return removed_importers

  def middle_node_started(self, middle_node_handle):
    '''
    Called when a middle node is confirmed to be running.

    :param middle_node_handle: The :ref:`handle` of the middle node.
    :type middle_node_handle: :ref:`handle`
    '''
    self.logger.info("Marking middle node as having started", extra={'middle_node_id': middle_node_handle['id']})
    self._middle_nodes.append(middle_node_handle)
    self._middle_node_states[middle_node_handle['id']] = 'started'
    if all(state == 'started' for state in self._middle_node_states.values()):
      self._transition_to_duplicating()

  def middle_node_duplicated(self, input_id_to_first_sequence_number, middle_node_id):
    '''
    Called when a middle node has confirmed that it is receiving all the proper duplicated inputs.

    :param dict[int, int] input_id_to_first_sequence_number: To each id of an input node assigned to the
      middle node with an id of ``middle_node_id``, this map assigns the first sequence number received by that
      middle node.

    :param str middle_node_id: The id of the middle node.
    '''
    self.logger.info("Marking middle node as now being duplicated", extra={'middle_node_id': middle_node_id})
    self._middle_node_states[middle_node_id] = 'duplicated'
    self._input_id_to_first_sequence_number.update(input_id_to_first_sequence_number)

    if all(state == 'duplicated' for state in self._middle_node_states.values()):
      self._transition_to_syncing()

  def finished_duplicating(self, sender_id):
    '''
    Called when a sender sum node is sending messages only according the structure at the end of migration, and not
    the duplicate messages from the beginning of the migration.

    :param str sender_id: The id of the sender node that has finished duplicating.
    '''
    self._duplicating_input_ids.remove(sender_id)
    if not self._duplicating_input_ids:
      self._transition_to_finished()

  def _new_sender_configs(self):
    '''
    Calculate which middle nodes to spawn in order to protect the current node against a high load from the leftmost
    sender nodes, and return the middle nodes' configs.

    Side effect: update self._partition to map each middle node id to the leftmost nodes that will send to it
      once we reach the terminal state.
    '''
    n_senders_per_node = len(self.node._importers) // self.n_new_nodes

    importers = list(self.node._importers.values())

    for i in range(self.n_new_nodes):
      new_id = ids.new_id('SumNode_middle_for_migration')

      self._partition[new_id] = importers[i * n_senders_per_node:(i + 1) * n_senders_per_node]
      self._middle_node_states[new_id] = 'new'

    new_node_ids = list(self._partition.keys())

    # allocate the remaining senders as evenly as possible
    for j, extra_importer in enumerate(importers[self.n_new_nodes * n_senders_per_node:]):
      self._partition[new_node_ids[j]].append(extra_importer)

    return [
        messages.sum.sum_node_config(
            node_id=node_id,
            pending_sender_ids=[i.sender_id for i in importers],
            senders=[],
            receivers=[self_handle],
            spawning_migration=self_handle,
        ) for node_id, importers in self._partition.items() for self_handle in [self.node.new_handle(node_id)]
    ]

  @property
  def logger(self):
    return self.node.logger

  @property
  def controller(self):
    return self.node._controller
