from dist_zero import errors, messages

from .state import State


class StartingNodeMigratorsState(State):
  STATE = State.STARTING_NODE_MIGRATORS

  def __init__(self, migration, controller, migration_config):
    self._migration = migration
    self._controller = controller

    self._source_handles_and_configs = migration_config['source_nodes']
    self._sink_handles_and_configs = migration_config['sink_nodes']
    self._removal_handles_and_configs = migration_config['removal_nodes']

    self._sink_id_to_handle = {handle['id']: handle for handle, config in self._sink_handles_and_configs}

    self._insertion_node_configs = migration_config['insertion_node_configs']
    self._insertion_nodes = {}

    self._migrator_states = {}

    self._started_source_and_removals = False
    self._started_insertions = False

  def _start_sources(self):
    for node, migrator_config in self._source_handles_and_configs:
      migrator_config['migration'] = self._migration.new_handle(node['id'])
      # Source node configs need special preparation
      self._prepare_source_node_migrator_config(node, migrator_config)
      self._migrator_states[node['id']] = 'pending'
      self._migration.send(node, messages.migration.attach_migrator(migrator_config))

  def _start_sinks(self):
    for node, migrator_config in self._sink_handles_and_configs:
      migrator_config['migration'] = self._migration.new_handle(node['id'])
      self._prepare_sink_node_migrator_config(node, migrator_config)
      self._migrator_states[node['id']] = 'pending'
      self._migration.send(node, messages.migration.attach_migrator(migrator_config))

  def _start_removals(self):
    for node, migrator_config in self._removal_handles_and_configs:
      migrator_config['migration'] = self._migration.new_handle(node['id'])
      self._migrator_states[node['id']] = 'pending'
      self._migration.send(node, messages.migration.attach_migrator(migrator_config))

  def initialize(self):
    self._start_sources()
    self._start_removals()
    self._maybe_start_insertions()

  def _maybe_start_insertions(self):
    if all(self._migrator_states[node['id']] == 'attached' for node, migrator_config in self._source_handles_and_configs) \
        and all(self._migrator_states[node['id']] == 'attached' for node, migrator_config in self._removal_handles_and_configs):
      self._started_source_and_removals = True
      self._start_insertions()
      self._maybe_start_sinks()

  def _start_insertions(self):
    for insertion_node_config in self._insertion_node_configs:
      if 'senders' in insertion_node_config:
        insertion_node_config['senders'] = [
            self._migration.transfer_handle(sender, for_node_id=insertion_node_config['id'])
            for sender in insertion_node_config['senders']
        ]
      if 'receivers' in insertion_node_config:
        insertion_node_config['receivers'] = [
            self._migration.transfer_handle(receiver, for_node_id=insertion_node_config['id'])
            for receiver in insertion_node_config['receivers']
        ]
      migrator = insertion_node_config['migrator']
      migrator['migration'] = self._migration.new_handle(insertion_node_config['id'])
      if 'senders' in migrator:
        migrator['senders'] = [
            self._migration.transfer_handle(sender, for_node_id=insertion_node_config['id'])
            for sender in migrator['senders']
        ]
      if 'receivers' in migrator:
        migrator['receivers'] = [
            self._migration.transfer_handle(receiver, for_node_id=insertion_node_config['id'])
            for receiver in migrator['receivers']
        ]
      insertion_node_config['migrator'] = migrator
      insertion_node_id = self._controller.spawn_node(insertion_node_config)
      self._migrator_states[insertion_node_id] = 'pending'
      self._insertion_nodes[insertion_node_id] = None

    self._maybe_finish()

  def _maybe_start_sinks(self):
    if all(self._migrator_states[node_config['id']] == 'attached' for node_config in self._insertion_node_configs):
      self._started_insertions = True
      self._start_sinks()

  def receive(self, message, sender_id):
    if message['type'] == 'attached_migrator':
      if sender_id not in self._migrator_states:
        self._migration.logger.warning("Received an attached_migrator message for a migrator not spawned "
                                       "while in the STARTING_NODE_MIGRATORS state.")
      else:
        self._migrator_states[sender_id] = 'attached'
        if sender_id in self._insertion_nodes:
          insertion_node_handle = message['insertion_node_handle']
          self._insertion_nodes[insertion_node_handle['id']] = insertion_node_handle
          self._maybe_start_sinks()
        elif not self._started_source_and_removals:
          self._maybe_start_insertions()
        else:
          self._maybe_finish()
    else:
      raise errors.InternalError('Unrecognized message type "{}"'.format(message['type']))

  def _maybe_finish(self):
    if all(state == 'attached' for state in self._migrator_states.values()):
      self._migration.finish_state_starting_node_migrators(
          source_nodes={handle['id']: handle
                        for handle, config in self._source_handles_and_configs},
          sink_nodes={handle['id']: handle
                      for handle, config in self._sink_handles_and_configs},
          removal_nodes={handle['id']: handle
                         for handle, config in self._removal_handles_and_configs},
          insertion_nodes=self._insertion_nodes,
      )

  def _node_to_handle(self, receiver_id):
    if receiver_id in self._sink_id_to_handle:
      return self._sink_id_to_handle[receiver_id]
    elif receiver_id in self._insertion_nodes:
      return self._insertion_nodes[receiver_id]
    else:
      raise errors.InternalError("Receiver could not be found among the receiving nodes of the migration.")

  def _prepare_sink_node_migrator_config(self, sink_node, sink_migrator_config):
    sink_migrator_config['new_flow_senders'] = [
        self._migration.transfer_handle(self._node_to_handle(sender_id), for_node_id=sink_node['id'])
        for sender_id in sink_migrator_config['new_flow_senders']
    ]

  def _prepare_source_node_migrator_config(self, source_node, source_migrator_config):
    '''
    Update source_migrator_config so that it is ready to be used to spawn a new migrator.

    :param source_node: Identifies the `Node` on which the new `SourceMigrator` will run.
    :type source_node: :ref:`handle`

    :param source_migrator_config: A :ref:`message` that is almost ready to attach a new `SourceMigrator`
    :type source_migrator_config: :ref:`message`
    '''
    if 'exporter_swaps' in source_migrator_config:
      source_migrator_config['exporter_swaps'] = [(receiver_id, [
          self._migration.transfer_handle(self._receiver_to_handle(new_receiver_id), for_node_id=source_node['id'])
          for new_receiver_id in new_receiver_ids
      ]) for receiver_id, new_receiver_ids in source_migrator_config['exporter_swaps']]

    if 'new_receivers' in source_migrator_config:
      source_migrator_config['new_receivers'] = [
          self._migration.transfer_handle(self._receiver_to_handle(new_receiver_id), for_node_id=source_node['id'])
          for new_receiver_id in source_migrator_config['new_receivers']
      ]
