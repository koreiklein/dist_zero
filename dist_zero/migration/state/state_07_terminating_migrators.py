from dist_zero import errors, messages

from .state import State


class TerminatingMigratorsState(State):
  STATE = State.TERMINATING_MIGRATORS

  def __init__(self, migration, controller, migration_config, all_nodes):
    self._migration = migration
    self._controller = controller

    self._all_nodes = all_nodes

  def initialize(self):
    self._node_is_terminated = {}
    for node in self._all_nodes:
      self._node_is_terminated[node['id']] = False
      self._migration.send(node, messages.migration.terminate_migrator(self._migration.id))

  def receive(self, message, sender_id):
    if message['type'] == 'migrator_terminated':
      self._node_is_terminated[sender_id] = True
      if all(self._node_is_terminated.values()):
        self._migration.finish_state_terminating_migrators()
    else:
      raise errors.InternalError('Unrecognized message type "{}"'.format(message['type']))
