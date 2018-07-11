from dist_zero import errors, messages, deltas, linker, settings

from . import migrator


class SinkMigrator(migrator.Migrator):
  NEW_FLOW_ACKNOWLEDGEMENT_INTERNAL_MS = 30
  '''
  The number of milliseconds between points in time when the `SinkMigrator` acknowledges messages in the new flow.
  '''

  def __init__(self, new_flow_sender_ids, old_flow_sender_ids, migration, node):
    '''
    :param migration: The :ref:`handle` of the `MigrationNode` running the migration.
    :type migration: :ref:`handle`
    :param node: The `Node` on which this migrator runs.
    :type node: `Node`
    '''
    self._migration = migration
    self._node = node
    self._linker = linker.Linker(self._node, logger=self._node.logger, deliver=self.deliver)

    self._waiting_for_swap = False

    self._deltas = deltas.Deltas()

    self._now_ms = 0

    self._swapped = False
    '''True once this migrator has swapped to the new flow.'''

    self._new_sender_id_to_first_flow_sequence_number = {sender_id: None for sender_id in new_flow_sender_ids}
    '''
    Once the flow has started, this map will assign to each new sender_id its first sequence_number
    that reflects the new flow.
    '''
    self._old_sender_id_to_first_flow_sequence_number = {sender_id: None for sender_id in old_flow_sender_ids}
    '''
    Once the flow has started, this map will assign to each old sender_id its first sequence_number
    that reflects the new flow.
    '''

    self._new_importers = {sender_id: None for sender_id in new_flow_sender_ids}
    '''Sender id to the new importer'''

    for sender_id in new_flow_sender_ids:
      self._deltas.add_sender(sender_id)

    self._old_sender_id_to_first_swapped_sequence_number = {sender_id: None for sender_id in old_flow_sender_ids}
    '''Maps each old sender_id to the first sequence number after it has swapped to the new flow.'''
    self._new_sender_id_to_first_swapped_sequence_number = {sender_id: None for sender_id in new_flow_sender_ids}
    '''Maps each new sender_id to the first sequence number after it has swapped to the new flow.'''

    self._flow_is_started = False
    '''True iff the new flow has started'''

    self._start_syncing_message = None # A start_syncing message received by self if one exists.

    self._sync_target_to_status = None
    '''
    This will eventually be a map taking each target syncing node to either
      'pending' if the node is not yet synced
      'synced' if it is
    '''

  @staticmethod
  def from_config(migrator_config, node):
    '''
    Create and return a new `SinkMigrator` from a config.

    :param dict migrator_config: Configuration dictionary for the `Migrator` instance.
    :param node: The `Node` instance on which the new `Migrator` will run.
    :type node: `Node`

    :return: The appropriate `SinkMigrator` instance.
    :rtype: `SinkMigrator`
    '''
    return SinkMigrator(
        new_flow_sender_ids=migrator_config['new_flow_sender_ids'],
        old_flow_sender_ids=migrator_config['old_flow_sender_ids'],
        migration=migrator_config['migration'],
        node=node)

  def _maybe_complete_flow(self):
    if all(val is not None for val in self._new_sender_id_to_first_flow_sequence_number.values()) \
        and all(val is not None for val in self._old_sender_id_to_first_flow_sequence_number.values()):
      self._node.logger.info("SinkMigrator completed flow.", extra={'migration_id': self.migration_id})
      self._flow_is_started = True
      self._node.deltas_only = False
      sequence_number = self._node.send_forward_messages()
      self._node.send(self._migration, messages.migration.completed_flow(sequence_number=sequence_number))

  def new_handle(self, for_node_id):
    return self._node.new_handle(for_node_id)

  def send(self, receiver, message):
    self._node.send(receiver=receiver, message=message)

  def deliver(self, message, sequence_number, sender_id):
    self._deltas.add_message(sender_id=sender_id, sequence_number=sequence_number, message=message)

  def receive(self, sender_id, message):
    if message['type'] == 'started_flow':
      # These messages are received from the new flow.
      if self._flow_is_started:
        self._node.logger.warning("Received a started_flow message after the flow had already started.")
        return

      self._new_importers[sender_id] = self._linker.new_importer(
          sender=message['sender'], first_sequence_number=message['sequence_number'])

      self._new_sender_id_to_first_flow_sequence_number[sender_id] = message['sequence_number']
      self._maybe_complete_flow()

    elif message['type'] == 'replacing_flow':
      # These messages are received from the old flow.

      if self._flow_is_started:
        self._node.logger.warning("Ignoring a 'replacing_flow' message received after the new flow has started")
        return

      self._old_sender_id_to_first_flow_sequence_number[sender_id] = message['sequence_number']
      self._maybe_complete_flow()
    elif message['type'] == 'sequence_message':
      if self._swapped:
        if not hasattr(self, '_FIXME_total_seq_msg_after_swap'):
          self._FIXME_total_seq_msg_after_swap = 0
          self._FIXME_seq_msg_set = set()
        else:
          if message['value']['type'] == 'receive' and message['value']['sequence_number'] not in self._FIXME_seq_msg_set:
            self._FIXME_seq_msg_set.add(message['value']['sequence_number'])
            self._FIXME_total_seq_msg_after_swap += message['value']['message']['amount']
            print('sink migrator seq msg {}'.format(self._FIXME_total_seq_msg_after_swap))
        self._node.linker.receive_sequence_message(message=message['value'], sender_id=sender_id)
      else:
        self._linker.receive_sequence_message(message=message['value'], sender_id=sender_id)
    elif message['type'] == 'start_syncing':
      self._start_syncing_message = message
      self._maybe_start_syncing()
    elif message['type'] == 'sum_total_set':
      self._sync_target_to_status[sender_id] = 'synced'
      if all(status == 'synced' for status in self._sync_target_to_status.values()):
        self._node.send(self._migration, messages.migration.syncer_is_synced())
    elif message['type'] == 'prepare_for_switch':
      self._waiting_for_swap = True
      self._node.send(self._migration, messages.migration.prepared_for_switch())
    elif message['type'] == 'swapped_from_duplicate':
      self._old_sender_id_to_first_swapped_sequence_number[sender_id] = message['first_live_sequence_number']
      self._maybe_swap()
    elif message['type'] == 'swapped_to_duplicate':
      self._new_sender_id_to_first_swapped_sequence_number[sender_id] = message['first_live_sequence_number']
      self._maybe_swap()
    elif message['type'] == 'terminate_migrator':
      self._node.remove_migrator(self.migration_id)
      self._node.send(self._migration, messages.migration.migrator_terminated())
    else:
      raise errors.InternalError('Unrecognized migration message type "{}"'.format(message['type']))

  def _maybe_swap(self):
    if not self._swapped \
        and all(sn is not None for sn in self._old_sender_id_to_first_swapped_sequence_number.values()) \
        and all(sn is not None for sn in self._new_sender_id_to_first_swapped_sequence_number.values()) \
        and self._node._deltas.covers(self._old_sender_id_to_first_swapped_sequence_number) \
        and self._deltas.covers(self._new_sender_id_to_first_swapped_sequence_number):
      self._node.logger.info("SinkMigrator swapping flows.", extra={'migration_id': self.migration_id})
      # NOTE: Since the source nodes should have stopped sending along the old flow since their
      #   first_swapped_sequence_number, the ``before`` argument below shouldn't be necessary.
      #   Here, we pass it in anyways.  It shouldn't make a difference either way.
      self._node.send_forward_messages(before=self._old_sender_id_to_first_swapped_sequence_number)
      self._swapped = True
      self._waiting_for_swap = False

      # Now, update the current node to listen only to the new flow.

      # Discard deltas before _new_sender_id_to_first_swapped_sequence_number, as they were exactly
      # what was represented from the old flow as of the above call to send_forward_messages.
      self._deltas.pop_deltas(before=self._new_sender_id_to_first_swapped_sequence_number)

      FIXME_total = [
          v._FIXME_total_before_swap for k, v in self._node._controller._node_by_id.items()
          if k.startswith('SumNode_input_kid_')
      ]
      self._node._FIXME_swapped = True
      if self._node._current_state != sum(FIXME_total):
        raise RuntimeError("FIXME Remove this . totals did not match.")

      # FIXME(KK): This code is obviously too intrusive into the internal details of these objects.
      self._node._deltas = deltas.Deltas()
      for sender_id in self._new_importers:
        self._node._deltas.add_sender(sender_id)
      self._node.deltas_only = False
      self._node.linker.remove_importers(set(self._old_sender_id_to_first_flow_sequence_number.keys()))

      self._node.linker._branching.extend(self._linker._branching)
      self._node.linker._branching.sort()
      self._node._importers = {
          sender_id: self._node.linker.new_importer(
              sender=importer.sender,
              first_sequence_number=self._new_sender_id_to_first_swapped_sequence_number[sender_id],
              remote_sequence_number_to_early_message=importer._remote_sequence_number_to_early_message,
          )
          for sender_id, importer in self._new_importers.items()
      }

      print('first swapped')
      print(self._new_sender_id_to_first_swapped_sequence_number)

      self.send(self._migration, messages.migration.switched_flows())
      # self._linker should no longer be used.  Null it out.
      self._linker = None

  def _maybe_start_syncing(self):
    if self._start_syncing_message is not None and \
        self._sync_target_to_status is None and \
        self._node._deltas.covers(self._old_sender_id_to_first_flow_sequence_number):

      self._node.logger.info("SinkMigrator started syncing.", extra={'migration_id': self.migration_id})
      self._node.deltas_only = False
      receivers = self._start_syncing_message['receivers']
      self._sync_target_to_status = {receiver['id']: 'pending' for receiver in receivers}

      self._node.send_forward_messages(before=self._old_sender_id_to_first_flow_sequence_number)

      total = self._node._current_state
      total_quotient, total_remainder = total // len(receivers), total % len(receivers)

      for i, receiver in enumerate(receivers):
        # The first total_remainder receivers will get a total that is one greater than that of the other receivers.
        # This way, the totals of the receivers will add to the total of self._node
        self.send(receiver,
                  messages.migration.set_sum_total(
                      migration_id=self.migration_id,
                      from_node=self._node.new_handle(receiver['id']),
                      total=total_quotient + 1 if i < total_remainder else total_quotient))

  def elapse(self, ms):
    self._maybe_start_syncing()
    self._maybe_swap()
    self._now_ms += ms

    if not self._swapped and not self._waiting_for_swap:
      # Importantly, we must not elapse time on a linker while waiting for a swap, as it may
      # prematurely acknowledge
      self._linker.elapse(ms)
      if self._now_ms >= SinkMigrator.NEW_FLOW_ACKNOWLEDGEMENT_INTERNAL_MS:
        self._now_ms %= SinkMigrator.NEW_FLOW_ACKNOWLEDGEMENT_INTERNAL_MS
        self._linker.advance_sequence_number()

  @property
  def migration_id(self):
    return self._migration['id']

  def initialize(self):
    self._node.deltas_only = True
    self.send(self._migration, messages.migration.attached_migrator())
