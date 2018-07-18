from dist_zero import errors

from .state import State


class StartingNewNodesState(State):
  STATE = State.STARTING_NEW_NODES

  def __init__(self, migration, controller, migration_config):
    self._migration = migration
    self._controller = controller

    self._insertion_node_configs = migration_config['insertion_node_configs']
    self._insertion_nodes = {}

  def initialize(self):
    for insertion_node_config in self._insertion_node_configs:
      insertion_node_config['migrator']['migration'] = self._migration.new_handle(insertion_node_config['id'])
      insertion_node_id = self._controller.spawn_node(insertion_node_config)

    self._maybe_finish()

  def _maybe_finish(self):
    if len(self._insertion_nodes) == len(self._insertion_node_configs):
      self._migration.finish_state_starting_new_nodes(self._insertion_nodes)

  def receive(self, message, sender_id):
    if message['type'] == 'attached_migrator':
      insertion_node_handle = message['insertion_node_handle']
      self._insertion_nodes[insertion_node_handle['id']] = insertion_node_handle
      self._maybe_finish()
    else:
      raise errors.InternalError('Unrecognized message type "{}"'.format(message['type']))
