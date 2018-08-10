from dist_zero import errors, messages

from .state import State


class StartingNodeMigratorsState(State):
  STATE = State.STARTING_NODE_MIGRATORS

  def __init__(self, migration, controller, migration_config, insertion_nodes):
    self._migration = migration
    self._controller = controller

    self._insertion_nodes = insertion_nodes

    self._source_handles_and_configs = migration_config['source_nodes']
    self._sink_handles_and_configs = migration_config['sink_nodes']
    self._removal_handles_and_configs = migration_config['removal_nodes']

    self._sink_id_to_handle = {handle['id']: handle for handle, config in self._sink_handles_and_configs}

    self._migrator_states = {}

  def initialize(self):
    for node, migrator_config in self._source_handles_and_configs:
      migrator_config['migration'] = self._migration.new_handle(node['id'])
      # Source node configs need special preparation
      self._prepare_source_node_migrator_config(node, migrator_config)
      self._migrator_states[node['id']] = 'pending'
      self._migration.send(node, messages.migration.attach_migrator(migrator_config))

    for node, migrator_config in self._sink_handles_and_configs:
      migrator_config['migration'] = self._migration.new_handle(node['id'])
      self._prepare_sink_node_migrator_config(node, migrator_config)
      self._migrator_states[node['id']] = 'pending'
      self._migration.send(node, messages.migration.attach_migrator(migrator_config))

    for node, migrator_config in self._removal_handles_and_configs:
      migrator_config['migration'] = self._migration.new_handle(node['id'])
      self._migrator_states[node['id']] = 'pending'
      self._migration.send(node, messages.migration.attach_migrator(migrator_config))

    self._maybe_finish()

  def receive(self, message, sender_id):
    if message['type'] == 'attached_migrator':
      if sender_id not in self._migrator_states:
        self._migration.logger.warning("Received an attached_migrator message for a migrator not spawned "
                                       "while in the STARTING_NODE_MIGRATORS state.")
      else:
        self._migrator_states[sender_id] = 'attached'
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
      )

  def _receiver_to_handle(self, receiver_id):
    if receiver_id in self._insertion_nodes:
      return self._insertion_nodes[receiver_id]
    elif receiver_id in self._sink_id_to_handle:
      return self._sink_id_to_handle[receiver_id]
    else:
      raise errors.InternalError("Receiver could not be found among the receiving nodes of the migration.")

  def _prepare_sink_node_migrator_config(self, sink_node, sink_migrator_config):
    sink_migrator_config['new_flow_senders'] = [
        self._migration.transfer_handle(self._receiver_to_handle(sender_id), for_node_id=sink_node['id'])
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
