import itertools

from dist_zero import messages, errors
from dist_zero.node import node

import logging

from dist_zero.migration.state.state import State
from dist_zero.migration.state.state_01_starting_node_migrators import StartingNodeMigratorsState
from dist_zero.migration.state.state_03_starting_new_flow import StartingNewFlowState
from dist_zero.migration.state.state_04_syncing_new_nodes import SyncingNewNodesState
from dist_zero.migration.state.state_05_preparing_switching import PreparingSwitchingState
from dist_zero.migration.state.state_06_switching import SwitchingState
from dist_zero.migration.state.state_07_terminating_migrators import TerminatingMigratorsState
from dist_zero.migration.state.state_08_finished import FinishedState

logger = logging.getLogger(__name__)


class MigrationNode(node.Node):
  '''
  Each migration node will manage a single migration.

  The migration will set up a new flow of data from a nonempty set of existing source
  nodes to a nonempty set of existing sink nodes.  In setting up the new flow, the migration
  may spawn many new "insertion" nodes.

  The migration will also remove an old flow of data from the source nodes to the sink nodes.

  Importantly, not a single message arriving at the inputs should be lost over the course of the migration.

  The migration node itself will store the overall state of the migration.  Each node involved in the migration
  will be attached to a separate `Migrator` subclass will run that will control how that node behaves during the migration.
  '''

  def __init__(self, migration_id, controller, migration_config):
    '''
    :param str migration_id: The node id of this migration.

    :param object migration_config: Configuration parameters for the migration.
      These parameters will be parsed by the different `State` classes responsible for each state of the migration.

    :param `MachineController` controller: the controller for this node's machine.
    '''
    self.id = migration_id
    self._controller = controller

    self._state = State.NEW
    self._state_controller = None

    self._migration_config = migration_config

    super(MigrationNode, self).__init__(logger)

  @staticmethod
  def from_config(node_config, controller):
    return MigrationNode(migration_id=node_config['id'], controller=controller, migration_config=node_config)

  def receive(self, message, sender_id):
    if message['type'] == 'migration':
      if message['migration_id'] != self.id:
        raise errors.InternalError("MigrationNode received a message from  a different migration.")
      message = message['message']

    self._state_controller.receive(message=message, sender_id=sender_id)

  def _transition_state(self, from_state, to_state):
    '''Move from one state to another.'''
    if self._state != from_state:
      raise RuntimeError("Must be in state {} to transition".format(from_state))
    self.logger.info(
        "Migration transition {from_state} -> {to_state} for {cur_node_id}",
        extra={
            'from_state': from_state,
            'to_state': to_state,
        })
    self._state = to_state

  def initialize(self):
    self._transition_state(State.NEW, State.STARTING_NODE_MIGRATORS)

    self._state_controller = StartingNodeMigratorsState(self, self._controller, self._migration_config)
    self._state_controller.initialize()

  def finish_state_starting_node_migrators(self, source_nodes, sink_nodes, insertion_nodes, removal_nodes):
    self._transition_state(State.STARTING_NODE_MIGRATORS, State.STARTING_NEW_FLOW)
    self.source_nodes = source_nodes
    self.sink_nodes = sink_nodes
    self.insertion_nodes = insertion_nodes
    self.removal_nodes = removal_nodes

    self._state_controller = StartingNewFlowState(
        self,
        self._controller,
        self._migration_config,
        sink_nodes=self.sink_nodes,
        insertion_nodes=self.insertion_nodes)
    self._state_controller.initialize()

  def finish_state_starting_new_flow(self):
    self._transition_state(State.STARTING_NEW_FLOW, State.SYNCING_NEW_NODES)

    self._state_controller = SyncingNewNodesState(
        self, self._controller, self._migration_config, insertion_nodes=self.insertion_nodes)
    self._state_controller.initialize()

  def finish_state_syncing_new_nodes(self):
    self._transition_state(State.SYNCING_NEW_NODES, State.PREPARING_SWITCHING)

    self._state_controller = PreparingSwitchingState(
        self,
        self._controller,
        self._migration_config,
        insertion_nodes=self.insertion_nodes,
        sink_nodes=self.sink_nodes)
    self._state_controller.initialize()

  def finish_state_preparing_switching(self):
    self._transition_state(State.PREPARING_SWITCHING, State.SWITCHING)
    # At this point, the migration is "over the hump", nde new flow is now destined to become active, and the
    # old flow is destined to be removed.  The migration can no longer be reversed.
    self._state_controller = SwitchingState(
        self, self._controller, self._migration_config, source_nodes=self.source_nodes, sink_nodes=self.sink_nodes)
    self._state_controller.initialize()

  def finish_state_switching(self):
    self._transition_state(State.SWITCHING, State.TERMINATING_MIGRATORS)

    self._state_controller = TerminatingMigratorsState(
        self,
        self._controller,
        self._migration_config,
        all_nodes=list(
            itertools.chain(self.source_nodes.values(), self.insertion_nodes.values(), self.removal_nodes.values(),
                            self.sink_nodes.values())))
    self._state_controller.initialize()

  def finish_state_terminating_migrators(self):
    self._transition_state(State.TERMINATING_MIGRATORS, State.FINISHED)

    self._state_controller = FinishedState(self, self._controller, self._migration_config)
    self._state_controller.initialize()

    self._controller.terminate_node(self.id)

  def elapse(self, ms):
    # TODO(KK): Add retransmission to migration nodes when it is taking to long to move into the next state.
    #   Perhaps it would help to use exporters/importers?  Alternatively, the messages to resend in each state
    #   are simple enough, that it would make sense to set up the processing of each message to be idempotent
    #   and just have the migration node periodically resent messages.
    pass
