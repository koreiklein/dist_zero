from dist_zero import errors, messages

from .state import State


class SyncingNewNodesState(State):
  STATE = State.SYNCING_NEW_NODES

  def __init__(self, migration, controller, migration_config, insertion_nodes):
    self._migration = migration
    self._controller = controller

    self._sync_pairs = migration_config['sync_pairs']

    self._insertion_nodes = insertion_nodes

    self._syncer_to_state = {}

  def initialize(self):
    for sender, target_node_ids in self._sync_pairs:
      self._syncer_to_state[sender['id']] = 'pending'
      self._migration.send(sender,
                           messages.migration.start_syncing(self._migration.id, [
                               self._migration.transfer_handle(self._insertion_nodes[target_node_id], sender['id'])
                               for target_node_id in target_node_ids
                           ]))

    self._maybe_finish()

  def _maybe_finish(self):
    if all(state == 'synced' for state in self._syncer_to_state.values()):
      self._migration.finish_state_syncing_new_nodes()

  def receive(self, message, sender_id):
    if message['type'] == 'syncer_is_synced':
      self._syncer_to_state[sender_id] = 'synced'
      self._maybe_finish()
    else:
      raise errors.InternalError('Unrecognized message type "{}"'.format(message['type']))
