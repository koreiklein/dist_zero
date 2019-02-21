import itertools

from dist_zero import errors, deltas, messages, linker, settings
from dist_zero.network_graph import NetworkGraph

from . import migrator
from .. import connector
from .right_configuration import RightConfigurationReceiver


class InsertionMigrator(migrator.Migrator):
  def __init__(self, migration, node):
    '''
    :param migration: The :ref:`handle` of the `MigrationNode` running the migration.
    :type migration: :ref:`handle`
    :param list[str] configure_right_parent_ids: The ids of the nodes that will send 'configure_right_parent' to this
      insertion node.
    :param node: The `Node` on which this migrator runs.
    :type node: `Node`
    '''
    self._migration = migration
    self._node = node

    self._height = self._node.height

    # When true, the swap has been prepared, and the migrator should be checking whether it's time to swap.
    self._waiting_for_swap = False

    self._kids = {} # node_id to either None (if the node has not yet reported that it is live) or the kid's handle.

    self._new_sender_id_to_first_live_sequence_number = {}
    self._flow_is_started = False

    self._kids_ready_for_switch = None
    self._kid_migrator_is_terminated = {}

  @property
  def parent(self):
    if self._node.parent is not None:
      return self._node.parent
    else:
      return self._migration

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
    return InsertionMigrator(migration=migrator_config['migration'], node=node)

  def _maybe_prepared_for_switch(self):
    if all(self._kids_ready_for_switch.values()):
      self._node.logger.info("Migrator is prepared for switch. Sending 'prepared_for_switch' to parent")
      self._node.send(self.parent, messages.migration.prepared_for_switch(self.migration_id))
      self._waiting_for_swap = True

  def receive(self, sender_id, message):
    if message['type'] == 'sequence_message':
      self._node.linker.receive_sequence_message(message['value'], sender_id=sender_id)
      self._maybe_swap()
    elif message['type'] == 'attached_migrator':
      node = message['insertion_node_handle']
      self._kids[node['id']] = node
    elif message['type'] == 'set_sum_total':
      if not self._flow_is_started:
        raise errors.InternalError("Migrator should not receive set_sum_total before the flow has started.")
      # Set the starting state, as of the state on inputs when the new flow started
      self._node._current_state = message['total']

      # Exit deltas only, add the exporters and start sending to them.
      for nid, receiver in self._node._receivers.items():
        self._node._exporters[nid] = self._node.linker.new_exporter(receiver, migration_id=self.migration_id)
      self._node.deltas_only.remove(self.migration_id)
      self._node.send_forward_messages()
      self._node.send(message['from_node'], messages.migration.sum_total_set(self.migration_id))
    elif message['type'] == 'prepare_for_switch':
      self._node.logger.info("Migrator received 'prepare_for_switch' from a parent")
      self._node.deltas_only.add(self.migration_id)
      self._kids_ready_for_switch = {}
      for kid in self._kids.values():
        self._kids_ready_for_switch[kid['id']] = False
        self._node.send(kid, messages.migration.prepare_for_switch(self.migration_id))
      self._maybe_prepared_for_switch()
    elif message['type'] == 'prepared_for_switch':
      self._node.logger.info("Migrator received 'prepared_for_switch' from a child")
      self._kids_ready_for_switch[sender_id] = True
      self._maybe_prepared_for_switch()
    elif message['type'] == 'swapped_to_duplicate':
      self._new_sender_id_to_first_live_sequence_number[sender_id] = message['first_live_sequence_number']
      self._maybe_swap()
    elif message['type'] == 'terminate_migrator':
      for kid in self._kids.values():
        self._kid_migrator_is_terminated[kid['id']] = False
        self._node.send(kid, messages.migration.terminate_migrator(self.migration_id))
      self._maybe_kids_are_terminated()
    elif message['type'] == 'migrator_terminated':
      self._kid_migrator_is_terminated[sender_id] = True
      self._maybe_kids_are_terminated()
    elif message['type'] in [
        'configure_new_flow_right', 'configure_new_flow_left', 'configure_right_parent', 'substitute_right_parent'
    ]:
      self._node.receive(message=message, sender_id=sender_id)
    else:
      raise errors.InternalError('Unrecognized migration message type "{}"'.format(message['type']))

  def _maybe_kids_are_terminated(self):
    if all(self._kid_migrator_is_terminated.values()):
      self._node.activate_swap(self.migration_id, kids=self._kids)
      self._node.remove_migrator(self.migration_id)
      self._node.send(self.parent, messages.migration.migrator_terminated(self.migration_id))

  @property
  def migration(self):
    return self._migration

  def _maybe_swap(self):
    if self._waiting_for_swap and \
        len(self._new_sender_id_to_first_live_sequence_number) == len(self._node.left_ids) and \
        self._node._deltas.covers(self._new_sender_id_to_first_live_sequence_number):

      self._node.logger.info("Insertion node is swapping flows.")
      self._node.checkpoint(self._new_sender_id_to_first_live_sequence_number)
      self._node.deltas_only.remove(self.migration_id)

      if settings.IS_TESTING_ENV:
        self._node._TESTING_swapped_once = True

      for exporter in self._node._exporters.values():
        self._node.send(
            exporter.receiver,
            messages.migration.swapped_to_duplicate(
                self.migration_id, first_live_sequence_number=exporter._internal_sequence_number))

      self._waiting_for_swap = False
      self._node.activate_swap(self.migration_id, kids=self._kids)

  @property
  def migration_id(self):
    return self._migration['id']

  def elapse(self, ms):
    self._maybe_swap()

  def initialize(self):
    self._node.deltas_only.add(self.migration_id)

    self._node.send(
        self.parent,
        message=messages.migration.attached_migrator(self.migration_id, self._node.new_handle(self.parent['id'])))
