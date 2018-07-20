from dist_zero import errors, deltas, messages, linker, settings

from . import migrator


class InsertionMigrator(migrator.Migrator):
  def __init__(self, migration, senders, receivers, node):
    '''
    :param migration: The :ref:`handle` of the `MigrationNode` running the migration.
    :type migration: :ref:`handle`
    :param list senders: A list of :ref:`handle` of the `Node` s that will send to self by the end of the migration.
    :param list receivers: A list of :ref:`handle` of the `Node` s that will receive from self by the end of the migration.
    :param node: The `Node` on which this migrator runs.
    :type node: `Node`
    '''
    self._migration = migration
    self._node = node

    # When true, the swap has been prepared, and the migrator should be checking whether it's time to swap.
    self._waiting_for_swap = False

    self._senders = {sender['id']: sender for sender in senders}
    self._receivers = {receiver['id']: receiver for receiver in receivers}

    self._sender_id_to_status = {sender_id: 'new' for sender_id in self._senders}
    '''
    Map each sender node id to either
      'new' at first
      'started_flow' once the node is sending flow here
      'swapped' once the node has swapped to the new flow
    '''
    self._new_sender_id_to_first_live_sequence_number = {}
    self._flow_is_started = False

  @staticmethod
  def from_config(migrator_config, node):
    '''
    Create and return a new `InsertionMigrator` from a config.

    :param dict migrator_config: Configuration dictionary for the `Migrator` instance.
    :param node: The `Node` instance on which the new `Migrator` will run.
    :type node: `Node`

    :return: The appropriate `InsertionMigrator` instance.
    :rtype: `InsertionMigrator`
    '''
    return InsertionMigrator(
        migration=migrator_config['migration'],
        senders=migrator_config['senders'],
        receivers=migrator_config['receivers'],
        node=node)

  def _maybe_send_started_flow(self):
    if all(val == 'started_flow' for val in self._sender_id_to_status.values()):
      # Stay in deltas_only mode until after receiving set_sum_total.
      # Until then, the model state has not been initialized and the deltas can *not* be applied.
      self._node.deltas_only.add(self.migration_id)
      self._flow_is_started = True

      for receiver in self._receivers.values():
        self._node.send(
            receiver,
            messages.migration.started_flow(
                migration_id=self.migration_id,
                sequence_number=0, # Insertion nodes always start at sequence number 0
                sender=self._node.new_handle(receiver['id'])))

  def receive(self, sender_id, message):
    if message['type'] == 'sequence_message':
      self._node.linker.receive_sequence_message(message['value'], sender_id=sender_id)
      self._maybe_swap()
    elif message['type'] == 'started_flow':
      self._node.import_from_node(message['sender'], first_sequence_number=message['sequence_number'])
      self._sender_id_to_status[sender_id] = 'started_flow'
      self._maybe_send_started_flow()
    elif message['type'] == 'set_sum_total':
      if not self._flow_is_started:
        raise errors.InternalError("Migrator should not receive set_sum_total before the flow has started.")
      # Set the starting state, as of the state on inputs when they sent started_flow.
      self._node._current_state = message['total']

      # Exit deltas only, add the exporters and start sending to them.
      for nid, receiver in self._receivers.items():
        self._node._exporters[nid] = self._node.linker.new_exporter(receiver, migration_id=self.migration_id)
      self._node.deltas_only.remove(self.migration_id)
      self._node.send_forward_messages()
      self._node.send(message['from_node'], messages.migration.sum_total_set(self.migration_id))
    elif message['type'] == 'prepare_for_switch':
      self._node.deltas_only.add(self.migration_id)
      self._node.send(self._migration, messages.migration.prepared_for_switch())
      self._waiting_for_swap = True
    elif message['type'] == 'swapped_to_duplicate':
      self._sender_id_to_status[sender_id] = 'swapped'
      self._new_sender_id_to_first_live_sequence_number[sender_id] = message['first_live_sequence_number']
      self._maybe_swap()
    elif message['type'] == 'terminate_migrator':
      self._node.remove_migrator(self.migration_id)
      for nid in self._receivers:
        self._node._exporters[nid]._migration_id = None
      self._node.send(self._migration, messages.migration.migrator_terminated())
    else:
      raise errors.InternalError('Unrecognized migration message type "{}"'.format(message['type']))

  def _maybe_swap(self):
    if self._waiting_for_swap and \
        all(status == 'swapped' for status in self._sender_id_to_status.values()) and \
        self._node._deltas.covers(self._new_sender_id_to_first_live_sequence_number):
      self._node.send_forward_messages(self._new_sender_id_to_first_live_sequence_number)
      self._node.deltas_only.remove(self.migration_id)
      self._waiting_for_swap = False
      for receiver_id in self._receivers.keys():
        exporter = self._node._exporters[receiver_id]
        self._node.send(exporter.receiver,
                        messages.migration.swapped_to_duplicate(
                            self.migration_id, first_live_sequence_number=exporter._internal_sequence_number))

      if settings.IS_TESTING_ENV:
        self._node._TESTING_swapped_once = True

  @property
  def migration_id(self):
    return self._migration['id']

  def elapse(self, ms):
    self._maybe_swap()

  def initialize(self):
    self._node.deltas_only.add(self.migration_id)
    self._node.send(self._migration, messages.migration.attached_migrator(self._node.new_handle(self.migration_id)))
