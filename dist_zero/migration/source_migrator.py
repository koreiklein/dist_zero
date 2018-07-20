from dist_zero import errors, settings, linker, messages

from . import migrator


class SourceMigrator(migrator.Migrator):
  def __init__(self, migration, node, exporter_swaps):
    '''
    :param migration: The :ref:`handle` of the `MigrationNode` running the migration.
    :type migration: :ref:`handle`
    :param node: The `Node` on which this migrator runs.
    :type node: `Node`

    :param list exporter_swaps: A list of pairs (receiver_id, new_receiver_handles) giving for each existing
      receiver, the set of new receivers that it should duplicate to.
    '''
    self._migration = migration
    self._node = node

    def _deliver(message, sequence_number, sender_id):
      # Impossible! Source migrators do not add any importers to their linkers, and thus the linker
      # should never call deliver.
      raise errors.InternalError("No messages should be delivered to a `SourceMigrator` by its linker.")

    self._linker = linker.Linker(self._node, logger=self._node.logger, deliver=_deliver)

    self._exporter_swaps = exporter_swaps

    self._old_exporters = {
        old_receiver_id: self._node._exporters[old_receiver_id]
        for old_receiver_id, new_receivers in self._exporter_swaps
    }

    self._new_exporters = {}

    self._swapped = False

  @staticmethod
  def from_config(migrator_config, node):
    '''
    Create and return a new `SourceMigrator` from a config.

    :param dict migrator_config: Configuration dictionary for the `Migrator` instance.
    :param node: The `Node` instance on which the new `Migrator` will run.
    :type node: `Node`

    :return: The appropriate `SourceMigrator` instance.
    :rtype: `SourceMigrator`
    '''
    return SourceMigrator(
        migration=migrator_config['migration'], node=node, exporter_swaps=migrator_config['exporter_swaps'])

  def _receive_start_flow(self, sender_id, message):
    self._node.send_forward_messages()
    for old_receiver_id, new_receivers in self._exporter_swaps:
      self._node.logger.info(
          "Starting duplication phase for {cur_node_id} . {new_receivers} will now receive duplicates from {old_receiver_id}.",
          extra={
              'new_receivers': new_receivers,
              'old_receiver_id': old_receiver_id,
          })
      exporter = self._node._exporters[old_receiver_id]
      if exporter.duplicated_exporters is not None:
        raise errors.InternalError("Refusing to start duplicating an exporter that is already duplicating.")

      exporter.duplicated_exporters = []
      for new_receiver in new_receivers:
        new_exporter = self._linker.new_exporter(new_receiver, migration_id=self.migration_id)
        self._new_exporters[new_receiver['id']] = new_exporter
        exporter.duplicated_exporters.append(new_exporter)
        self._node.send(new_exporter.receiver,
                        messages.migration.started_flow(
                            migration_id=self.migration_id,
                            sequence_number=new_exporter.internal_sequence_number,
                            sender=self._node.new_handle(new_exporter.receiver_id),
                        ))

      self._node.send(exporter.receiver,
                      messages.migration.replacing_flow(
                          migration_id=self.migration_id, sequence_number=exporter.internal_sequence_number))

  def _receive_switch_flows(self, sender_id, message):
    # Clear out any messages that can still be sent on the old flow.
    self._node.send_forward_messages()

    for receiver_id in self._old_exporters:
      exporter = self._node._exporters.pop(receiver_id)
      self._node.send(exporter.receiver,
                      messages.migration.swapped_from_duplicate(
                          self.migration_id, first_live_sequence_number=exporter.internal_sequence_number))

    for exporter in self._new_exporters.values():
      self._node.send(exporter.receiver,
                      messages.migration.swapped_to_duplicate(
                          self.migration_id, first_live_sequence_number=exporter.internal_sequence_number))

    self._node._exporters.update(self._new_exporters)
    self._node.linker.absorb_linker(self._linker)
    self._swapped = True
    if settings.IS_TESTING_ENV:
      self._node._TESTING_swapped_once = True

  def _receive_terminate_migrator(self, sender_id, message):
    self._node.linker.remove_exporters(set(self._old_exporters.keys()))
    self._node.remove_migrator(self.migration_id)
    for exporter in self._new_exporters.values():
      exporter._migration_id = None
    self._node.send(self._migration, messages.migration.migrator_terminated())

  def receive(self, sender_id, message):
    if message['type'] == 'start_flow':
      self._receive_start_flow(sender_id, message)
    elif message['type'] == 'switch_flows':
      self._receive_switch_flows(sender_id, message)
    elif message['type'] == 'terminate_migrator':
      self._receive_terminate_migrator(sender_id, message)
    else:
      raise errors.InternalError('Unrecognized migration message type "{}"'.format(message['type']))

  @property
  def migration_id(self):
    return self._migration['id']

  def elapse(self, ms):
    if not self._swapped:
      self._linker.elapse(ms)

  def initialize(self):
    self._node.send(self._migration, messages.migration.attached_migrator())
