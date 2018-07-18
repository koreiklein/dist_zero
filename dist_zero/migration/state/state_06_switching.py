from dist_zero import errors, messages

from .state import State


class SwitchingState(State):
  STATE = State.SWITCHING

  def __init__(self, migration, controller, migration_config, source_nodes, sink_nodes):
    self._migration = migration
    self._controller = controller

    self._source_nodes = source_nodes
    self._sink_nodes = sink_nodes

    self._sink_id_to_new_flow_state = {}

  def initialize(self):
    for source_node in self._source_nodes.values():
      self._migration.send(source_node, messages.migration.switch_flows(self._migration.id))

    for sink_id in self._sink_nodes.keys():
      self._sink_id_to_new_flow_state[sink_id] = 'pending'

    self._maybe_finish()

  def _maybe_finish(self):
    if all(state == 'switched_flows' for state in self._sink_id_to_new_flow_state.values()):
      self._migration.finish_state_switching()

  def receive(self, message, sender_id):
    if message['type'] == 'switched_flows':
      self._sink_id_to_new_flow_state[sender_id] = 'switched_flows'
      self._maybe_finish()
    else:
      raise errors.InternalError('Unrecognized message type "{}"'.format(message['type']))
