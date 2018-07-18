from dist_zero import errors, messages

from .state import State


class PreparingSwitchingState(State):
  STATE = State.PREPARING_SWITCHING

  def __init__(self, migration, controller, migration_config, insertion_nodes, sink_nodes):
    self._migration = migration
    self._controller = controller

    self._insertion_nodes = insertion_nodes
    self._sink_nodes = sink_nodes

    self._nodes_prepared_for_switch = {}

  def initialize(self):
    for node in self._insertion_nodes.values():
      self._nodes_prepared_for_switch[node['id']] = False
      self._migration.send(node, messages.migration.prepare_for_switch(self._migration.id))

    for node in self._sink_nodes.values():
      self._nodes_prepared_for_switch[node['id']] = False
      self._migration.send(node, messages.migration.prepare_for_switch(self._migration.id))

    self._maybe_finish()

  def _maybe_finish(self):
    if all(self._nodes_prepared_for_switch.values()):
      self._migration.finish_state_preparing_switching()

  def receive(self, message, sender_id):
    if message['type'] == 'prepared_for_switch':
      self._nodes_prepared_for_switch[sender_id] = True
      self._maybe_finish()
    else:
      raise errors.InternalError('Unrecognized message type "{}"'.format(message['type']))
