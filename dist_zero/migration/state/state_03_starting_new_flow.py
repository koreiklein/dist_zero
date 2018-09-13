from dist_zero import errors, messages

from .state import State


class StartingNewFlowState(State):
  STATE = State.STARTING_NEW_FLOW

  def __init__(self, migration, controller, migration_config, sink_nodes, insertion_nodes):
    self._migration = migration
    self._controller = controller

    self._sink_nodes = sink_nodes
    self._insertion_nodes = insertion_nodes

    self._sink_id_to_new_flow_state = {}

  def initialize(self):
    for sink_id, sink in self._sink_nodes.items():
      self._migration.send(sink, messages.migration.start_flow(self._migration.id))
      self._sink_id_to_new_flow_state[sink_id] = 'pending'

    if len(self._insertion_nodes) != 1:
      raise errors.InternalError("Unsure how to send configure_right_parent messages for an InsertionNode "
                                 "when there are more than one root insertion node.")

    for node in self._insertion_nodes.values():
      self._migration.send(node,
                           messages.migration.configure_right_parent(
                               self._migration.id, kid_ids=list(self._sink_nodes.keys())))

    self._maybe_finish()

  def _maybe_finish(self):
    if all(state == 'completed_flow' for state in self._sink_id_to_new_flow_state.values()):
      self._migration.finish_state_starting_new_flow()

  def receive(self, message, sender_id):
    if message['type'] == 'started_flow':
      self._sink_id_to_new_flow_state[sender_id] = 'completed_flow'
      self._maybe_finish()
    else:
      raise errors.InternalError('Unrecognized message type "{}"'.format(message['type']))
