from dist_zero import errors, settings, linker, messages

from . import migrator


class SourceMigrator(migrator.Migrator):
  def __init__(self, migration, node, will_sync):
    '''
    :param migration: The :ref:`handle` of the `MigrationNode` running the migration.
    :type migration: :ref:`handle`
    :param node: The `Node` on which this migrator runs.
    :type node: `Node`

    :param bool will_sync: True iff this migrator will need to sync as part of the migration
    '''
    self._migration = migration
    self._node = node
    self._will_sync = will_sync

    self._right_configurations = None

    def _deliver(message, sequence_number, sender_id):
      # Impossible! Source migrators do not add any importers to their linkers, and thus the linker
      # should never call deliver.
      raise errors.InternalError("No messages should be delivered to a `SourceMigrator` by its linker.")

    self._linker = linker.Linker(self._node, logger=self._node.logger, deliver=_deliver)

    self._kid_has_migrator = {kid_id: False for kid_id in self._node._kids.keys()}

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
        migration=migrator_config['migration'],
        node=node,
        will_sync=migrator_config['will_sync'],
    )

  # FIXME(KK): Perhaps this method should be removed entirely, especially if it is unused.
  def _receive_start_flow(self, sender_id, message):
    self._node.send_forward_messages()

    for new_receiver in self._new_receivers:
      self._create_new_exporter(new_receiver)

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

      exporter.duplicated_exporters = [self._create_new_exporter(new_receiver) for new_receiver in new_receivers]

      self._node.send(exporter.receiver,
                      messages.migration.replacing_flow(
                          migration_id=self.migration_id, sequence_number=exporter.internal_sequence_number))

  def _create_new_exporter(new_receiver):
    new_exporter = self._linker.new_exporter(new_receiver, migration_id=self.migration_id)
    self._new_exporters[new_receiver['id']] = new_exporter
    self._node.send(new_receiver,
                    messages.migration.started_flow(
                        migration_id=self.migration_id,
                        sequence_number=new_exporter.internal_sequence_number,
                        sender=self._node.new_handle(new_receiver['id'])))
    return new_exporter

  def _receive_switch_flows(self, sender_id, message):
    # Clear out any messages that can still be sent on the old flow.
    self._node.checkpoint()

    self._node.switch_flows(
        self.migration_id, old_exporters=[], new_exporters=[], new_receivers=list(self._new_receivers.values()))

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

  def _maybe_has_right_configurations(self):
    if all(val is not None for val in self._right_configurations):
      self._new_receivers = {
          config['parent_handle']['id']: config['parent_handle']
          for config in self._right_configurations
      }
      self._send_configure_new_flow_left_to_right()

  def _send_configure_new_flow_left_to_right(self):
    for new_receiver in self._new_receivers.values():
      from dist_zero.node.io.internal import InternalNode
      if self._node.__class__ == InternalNode:
        kids = [{'handle': kid, 'connection_limit': 1} for kid in self._node._kids.values() if kid is not None]
      else:
        # FIXME(KK): Test and implement!
        import ipdb
        ipdb.set_trace()
        raise RuntimeError("Not Yet Implemented")

      self._node.send(new_receiver,
                      messages.migration.configure_new_flow_left(
                          self.migration_id, kids=kids, is_data=self._node.is_data(), depth=self._node.depth))

  def receive(self, sender_id, message):
    if message['type'] == 'attached_migrator':
      self._kid_has_migrator[sender_id] = True
      self._maybe_send_attached_migrator()
    elif message['type'] == 'configure_new_flow_right':
      right_config_index, n_total_right_configs = message['configuration_place']
      if self._right_configurations is None:
        self._right_configurations = [None for i in range(n_total_right_configs)]
      self._right_configurations[right_config_index] = message
      self._maybe_has_right_configurations()

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

  def _maybe_send_attached_migrator(self):
    if all(self._kid_has_migrator.values()):
      self._node.send(self._node._parent or self._migration, messages.migration.attached_migrator(self.migration_id))

  def initialize(self):
    for kid in self._node._kids.values():
      self._kid_has_migrator[kid['id']] = False
      self._node.send(kid,
                      messages.migration.attach_migrator(
                          messages.migration.source_migrator_config(
                              migration=self._node.transfer_handle(self._migration, kid['id']),
                              will_sync=self._will_sync)))

    self._maybe_send_attached_migrator()
