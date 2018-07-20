from dist_zero import errors, messages

from . import migrator


class RemovalMigrator(migrator.Migrator):
  def __init__(self, migration, node, sender_ids, receiver_ids):
    '''
    :param migration: The :ref:`handle` of the `MigrationNode` running the migration.
    :type migration: :ref:`handle`
    :param node: The `Node` on which this migrator runs.
    :type node: `Node`
    :param list[str] sender_ids: A list of ids of the `Node` s that will stop sending to self by the end of the migration.
    :param list[str] receiver_ids: A list of ids of the `Node` s that will stop receiving from self by the end of the migration.
    '''
    self._migration = migration
    self._node = node

    self._importers = {sender_id: self._node._importers[sender_id] for sender_id in sender_ids}
    self._exporters = {receiver_id: self._node._exporters[receiver_id] for receiver_id in receiver_ids}

    self._sender_id_to_first_flow_sequence_number = {sender_id: None for sender_id in self._importers}
    self._sender_id_to_first_live_sequence_number = {sender_id: None for sender_id in self._importers}

    self._flow_is_started = False
    self._swapped = False

  @staticmethod
  def from_config(migrator_config, node):
    '''
    Create and return a new `RemovalMigrator` from a config.

    :param dict migrator_config: Configuration dictionary for the `Migrator` instance.
    :param node: The `Node` instance on which the new `Migrator` will run.
    :type node: `Node`

    :return: The appropriate `RemovalMigrator` instance.
    :rtype: `RemovalMigrator`
    '''
    return RemovalMigrator(
        migration=migrator_config['migration'],
        node=node,
        sender_ids=migrator_config['sender_ids'],
        receiver_ids=migrator_config['receiver_ids'],
    )

  def receive(self, sender_id, message):
    if message['type'] == 'replacing_flow':
      self._sender_id_to_first_flow_sequence_number[sender_id] = message['sequence_number']
      self._maybe_send_replacing_flow()
    elif message['type'] == 'swapped_from_duplicate':
      self._sender_id_to_first_live_sequence_number[sender_id] = message['first_live_sequence_number']
      self._maybe_send_swapped()
    elif message['type'] == 'terminate_migrator':
      self._node.remove_migrator(self.migration_id)
      self._node.send(self._migration, messages.migration.migrator_terminated())
      self._node._controller.terminate_node(self._node.id)
    else:
      raise errors.InternalError('Unrecognized migration message type "{}"'.format(message['type']))

  def _maybe_send_swapped(self):
    if not self._swapped and \
        all(sn is not None for sn in self._sender_id_to_first_live_sequence_number.values()) and \
        self._node._deltas.covers(self._sender_id_to_first_live_sequence_number):
      self._swapped = True
      self._node.send_forward_messages(before=self._sender_id_to_first_live_sequence_number)
      for exporter in self._exporters.values():
        self._node.send(exporter.receiver,
                        messages.migration.swapped_from_duplicate(
                            migration_id=self.migration_id,
                            first_live_sequence_number=exporter.internal_sequence_number))

  def _maybe_send_replacing_flow(self):
    if not self._flow_is_started and \
        all(sn is not None for sn in self._sender_id_to_first_flow_sequence_number.values()) and \
        self._node._deltas.covers(self._sender_id_to_first_flow_sequence_number):
      self._flow_is_started = True
      # Exit deltas only mode as in the cases so far (7/19/2018) removal nodes do not sync.
      # FIXME(KK): In the event that this node will sync, we should stay in deltas_only mode at this point.
      self._node.deltas_only.remove(self.migration_id)
      self._node.send_forward_messages(before=self._sender_id_to_first_flow_sequence_number)
      for exporter in self._exporters.values():
        self._node.send(exporter.receiver,
                        messages.migration.replacing_flow(
                            migration_id=self.migration_id, sequence_number=exporter.internal_sequence_number))

  @property
  def migration_id(self):
    return self._migration['id']

  def elapse(self, ms):
    self._maybe_send_replacing_flow()
    self._maybe_send_swapped()

  def initialize(self):
    self._node.deltas_only.add(self.migration_id)
    self._node.send(self._migration, messages.migration.attached_migrator())
