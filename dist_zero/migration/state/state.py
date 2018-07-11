from dist_zero import errors


class State(object):
  '''
  Abstract base class for migration states.  Each `State` instance represents a phase of a migration,
  that runs for a period of time on a `MigrationNode`.

  Each state begins with a call to `State.initialize`  Starting then, the `MigrationNode` will be in that state,
  and forward all its messages to the state's `State.receive` method.

  For some period of time, the `MigrationNode` will remain in that state. During this time, the `State` instance may
  process messages, send messages, and collect data.

  Finally, the state ends when the `State` instance makes a call to one of the ``finish_state_*`` methods
  of a `MigrationNode`.  At the point, the `MigrationNode` leaves that state and enters a new state (or terminates).
  '''

  # Names for the possible states, in the order in which they occur.

  NEW = 'NEW'
  '''The initial state of the migration.'''

  STARTING_NEW_NODES = 'STARTING_NEW_NODES'
  '''
  Trigger: The migration enters this state immediately upon initialization.
  Action: The migrator spawns new nodes each with an attached `InsertionMigrator`.
    These nodes are part of the new flow only.

  Description: The migration is waiting until the new nodes have started.
  '''

  STARTING_NODE_MIGRATORS = 'STARTING_NODE_MIGRATORS'
  '''
  Trigger: The migrator has received confirmations that all the new nodes have started.
  Action: The migrator
    a) Adds one `SourceMigrator` to each node that is part of the source set of the migration.
       These nodes are part of both the new and old flows of the migration.
    b) Adds one `RemovalMigrator` to each node that is internal to the old flow.
    c) Adds one `SinkMigrator` to each node that is part of the sink set of the migration.
       These nodes are part of both the new and old flows of the migration.

  Description: The migration is waiting until all `Migrator` subclasses are attached and have
    sent an attached_migrator message.
  '''

  STARTING_NEW_FLOW = 'STARTING_NEW_FLOW'
  '''
  Trigger: attached_migrator messages have arrived from every migrator.
  Action: The migration node sends start_flow message to the sources.  This will simultaneously
    a) trigger a cascade of started_flow messages for the new flow
    b) trigger a cascade of replacing_flow messages for the old flow
  Description: The cascades of started_flow and replacing_flow messages are propogating through the network.
    Once each sink receives both the requisite started_flow and replacing_flow messages, it will send a
    completed_flow message to the migration node.  The migration node is waiting for all those completed_flow messages.
  '''

  SYNCING_NEW_NODES = 'SYNCING_NEW_NODES'
  '''
  Trigger: The migration node has received started_flow messages from all the sink migrators.
  Action: The migration syncs some data between the old and new data nodes.  The details depend on the type of nodes
    involved in the migration.  Any data synced will reflect precisely the source node messages with sequence numbers
    strictly less than the first sequence number of the new flow.
  Description: Data is being synced with the new data nodes.
  '''

  PREPARING_SWITCHING = 'PREPARING_SWITCHING'
  '''
  Trigger: The sync is finished.
  Action: The migration node sends prepare_for_switch message to insertion, removal, and sink nodes.
  Description: The migration waits until all the recipients have confirmed that they are prepared to switch.
  '''

  SWITCHING = 'SWITCHING'
  '''
  Trigger: The migration has received confirmations that the insertion, removal, and sink nodes are prepared to switch.
  Action: The migration node sends switch_flow messages to the sources.  The will simultaneously
    a) trigger a cascade of activate_flow messages for the new flow
    b) trigger a cascade of deactivate_flow messages for the old flow
  Description: The old flow is being deactivated while the new flow is being activated.
  '''

  TERMINATING_MIGRATORS = 'TERMINATING_MIGRATORS'
  '''
  Trigger: The migration node has received switched_flows messages from all the sinks.
  Action: Send terminate_migrator messages to all migrators.
  Description: The migration is over, remove the migrators entirely.
  '''

  FINISHED = 'FINISHED'
  '''
  Trigger: A migrator_terminated message from each migrator has arrived on the migration node.
  Action: None.
  Description: The migration is over.
  '''

  def initialize(self):
    raise RuntimeError("Abstract Superclass")

  def receive(self, message, sender_id):
    raise RuntimeError("Abstract Superclass")
