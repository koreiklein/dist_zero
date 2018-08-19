from dist_zero import errors, settings, linker, messages

from . import migrator
from .right_configuration import RightConfigurationReceiver


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

    self._right_config_receiver = RightConfigurationReceiver(has_parents=self._node._parent is not None)

    def _deliver(message, sequence_number, sender_id):
      # Impossible! Source migrators do not add any importers to their linkers, and thus the linker
      # should never call deliver.
      raise errors.InternalError("No messages should be delivered to a `SourceMigrator` by its linker.")

    self._linker = linker.Linker(self._node, logger=self._node.logger, deliver=_deliver)

    self._kid_has_migrator = {kid_id: False for kid_id in self._node._kids.keys()}
    self._kid_migrator_is_terminated = {}

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
    # FIXME(KK): Test and implement the logic regarding setting up duplications.  Then there may actually
    # be a reason to remove old exporters that are done duplicating.
    #self._node.linker.remove_exporters(set(self._old_exporters.keys()))
    for exporter in self._new_exporters.values():
      exporter._migration_id = None
    for kid in self._node._kids.values():
      self._kid_migrator_is_terminated[kid['id']] = False
      self._node.send(kid, messages.migration.terminate_migrator(self.migration_id))
    self._maybe_kids_are_terminated()

  def _maybe_kids_are_terminated(self):
    if all(self._kid_migrator_is_terminated.values()):
      self._node.remove_migrator(self.migration_id)
      self._node.send(self.parent, messages.migration.migrator_terminated(self.migration_id))

  def _maybe_has_right_configurations(self):
    if self._right_config_receiver.ready:
      self._new_receivers = {
          config['parent_handle']['id']: config['parent_handle']
          for config in self._right_config_receiver.configs.values()
      }
      self._send_configure_new_flow_left_to_right()

  def _send_configure_new_flow_left_to_right(self):
    self._node.logger.info("Sending configure_new_flow_left")
    if len(self._new_receivers) == 1:
      new_receiver_id = list(self._new_receivers.keys())[0]
      for kid in self._node._kids.values():
        self._node.send(kid,
                        messages.migration.set_source_right_parents(
                            migration_id=self.migration_id, configure_right_parent_ids=[new_receiver_id]))
    else:
      # FIXME(KK): Figure out what to do about this
      raise RuntimeError("Not Yet Implemented")

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
                          self.migration_id,
                          kids=kids,
                          node=self._node.new_handle(new_receiver['id']),
                          is_data=self._node.is_data(),
                          height=self._node.height))

  def receive(self, sender_id, message):
    if message['type'] == 'attached_migrator':
      self._kid_has_migrator[sender_id] = True
      self._maybe_send_attached_migrator()
    elif message['type'] == 'set_source_right_parents':
      self._right_config_receiver.set_parents(parent_ids=message['configure_right_parent_ids'])
      self._maybe_has_right_configurations()
    elif message['type'] == 'configure_right_parent':
      self._right_config_receiver.got_parent_configuration(sender_id, kid_ids=message['kid_ids'])
      self._maybe_has_right_configurations()
    elif message['type'] == 'configure_new_flow_right':
      self._node.logger.info("Received configure_new_flow_right")
      self._right_config_receiver.got_configuration(sender_id, message)
      self._maybe_has_right_configurations()
    elif message['type'] == 'switch_flows':
      self._receive_switch_flows(sender_id, message)
    elif message['type'] == 'terminate_migrator':
      self._receive_terminate_migrator(sender_id, message)
    elif message['type'] == 'migrator_terminated':
      self._kid_migrator_is_terminated[sender_id] = True
      self._maybe_kids_are_terminated()
    else:
      raise errors.InternalError('Unrecognized migration message type "{}"'.format(message['type']))

  @property
  def migration_id(self):
    return self._migration['id']

  def elapse(self, ms):
    if not self._swapped:
      self._linker.elapse(ms)

  @property
  def parent(self):
    if self._node._parent is not None:
      return self._node._parent
    else:
      return self._migration

  def _maybe_send_attached_migrator(self):
    if all(self._kid_has_migrator.values()):
      self._node.logger.info("sending attached_migrator")
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
