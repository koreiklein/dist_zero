from dist_zero import errors, deltas, messages, linker, settings
from dist_zero.network_graph import NetworkGraph

from . import migrator, topology_picker
from .right_configuration import RightConfigurationReceiver


class InsertionMigrator(migrator.Migrator):
  def __init__(self, migration, configure_right_parent_ids, senders, left_configurations, right_configurations,
               receivers, node):
    '''
    :param migration: The :ref:`handle` of the `MigrationNode` running the migration.
    :type migration: :ref:`handle`
    :param list[str] configure_right_parent_ids: The ids of the nodes that will send 'configure_right_parent' to this
      insertion node.
    :param list senders: A list of :ref:`handle` of the `Node` s that will send to self by the end of the migration.
    :param dict[str, object] left_configurations: In same cases, an insertion node can be preconfigured with left_configurations
      given from the parent node that spawned it.   In that case, ``left_configurations`` is a dictionary mapping sender id
      to the prexisting left_configuration.  Otherwise, ``left_configurations`` is `None`
    :param dict[str, object] right_configurations: In same cases, an insertion node can be preconfigured with right_configurations
      given from the parent node that spawned it.   In that case, ``right_configurations`` is a dictionary mapping right node ids
      to the prexisting right_configuration.  Otherwise, ``right_configurations`` is `None`
    :param list receivers: A list of :ref:`handle` of the `Node` s that will receive from self by the end of the migration.
    :param node: The `Node` on which this migrator runs.
    :type node: `Node`
    '''
    self._migration = migration
    self._node = node

    self._height = self._node.height

    self._right_map = None

    # When true, the swap has been prepared, and the migrator should be checking whether it's time to swap.
    self._waiting_for_swap = False

    self._graph = None
    self._picker = None

    self._senders = {sender['id']: sender for sender in senders}
    self._receivers = {receiver['id']: receiver for receiver in receivers}

    self._kids = {} # node_id to either None (if the node has not yet reported that it is live) or the kid's handle.

    # If the topology picker has determined multiple layers of nodes to spawn, this tracks which layer we are
    # currently spawning
    self._current_spawning_layer = None

    self._configure_right_parent_ids = configure_right_parent_ids
    if right_configurations is None:
      self._preconfigured_right_config_receiver = False
      self._right_config_receiver = RightConfigurationReceiver()
      self._right_config_receiver.set_parents(configure_right_parent_ids) # configure_right_parent_ids may be empty
    else:
      for right_config in right_configurations.values():
        self._node.export_to_node(right_config['parent_handle'])
      self._preconfigured_right_config_receiver = True
      self._right_config_receiver = RightConfigurationReceiver(configs=right_configurations)

    self._left_configurations = {sender_id: None for sender_id in self._senders.keys()}
    if left_configurations is not None:
      self._left_configurations.update(left_configurations.items())
      self._right_configurations_are_sent = True
    else:
      self._right_configurations_are_sent = False

    self._left_configurations_are_sent = False

    self._new_sender_id_to_first_live_sequence_number = {}
    self._flow_is_started = False

    self._kids_ready_for_switch = None
    self._kid_migrator_is_terminated = {}

  @property
  def parent(self):
    if self._node.parent is not None:
      return self._node.parent
    else:
      return self._migration

  @staticmethod
  def from_config(migrator_config, node):
    '''
    Create and return a new `InsertionMigrator` from a config.

    :param dict migrator_config: Configuration dictionary for the `Migrator` instance.
    :param node: The `Node` instance on which the new `Migrator` will run.
    :type node: `Node`

    :return: The appropriate `InsertionMigrator` instance.
    :rtype: `InsertionMigrator`
    '''
    return InsertionMigrator(
        migration=migrator_config['migration'],
        configure_right_parent_ids=migrator_config['configure_right_parent_ids'],
        senders=migrator_config['senders'],
        left_configurations=migrator_config['left_configurations'],
        right_configurations=migrator_config['right_configurations'],
        receivers=migrator_config['receivers'],
        node=node)

  def _maybe_prepared_for_switch(self):
    if all(self._kids_ready_for_switch.values()):
      self._node.logger.info("Migrator is prepared for switch. Sending 'prepared_for_switch' to parent")
      self._node.send(self.parent, messages.migration.prepared_for_switch(self.migration_id))
      self._waiting_for_swap = True

  def receive(self, sender_id, message):
    if message['type'] == 'sequence_message':
      self._node.linker.receive_sequence_message(message['value'], sender_id=sender_id)
      self._maybe_swap()
    elif message['type'] == 'attached_migrator':
      self._kids[sender_id] = message['insertion_node_handle']
      self._maybe_spawned_kids()
    elif message['type'] == 'set_sum_total':
      if not self._flow_is_started:
        raise errors.InternalError("Migrator should not receive set_sum_total before the flow has started.")
      # Set the starting state, as of the state on inputs when the new flow started
      self._node._current_state = message['total']

      # Exit deltas only, add the exporters and start sending to them.
      for nid, receiver in self._receivers.items():
        self._node._exporters[nid] = self._node.linker.new_exporter(receiver, migration_id=self.migration_id)
      self._node.deltas_only.remove(self.migration_id)
      self._node.send_forward_messages()
      self._node.send(message['from_node'], messages.migration.sum_total_set(self.migration_id))
    elif message['type'] == 'prepare_for_switch':
      self._node.logger.info("Migrator received 'prepare_for_switch' from a parent")
      self._node.deltas_only.add(self.migration_id)
      self._kids_ready_for_switch = {}
      for kid in self._kids.values():
        self._kids_ready_for_switch[kid['id']] = False
        self._node.send(kid, messages.migration.prepare_for_switch(self.migration_id))
      self._maybe_prepared_for_switch()
    elif message['type'] == 'prepared_for_switch':
      self._node.logger.info("Migrator received 'prepared_for_switch' from a child")
      self._kids_ready_for_switch[sender_id] = True
      self._maybe_prepared_for_switch()
    elif message['type'] == 'swapped_to_duplicate':
      self._new_sender_id_to_first_live_sequence_number[sender_id] = message['first_live_sequence_number']
      self._maybe_swap()
    elif message['type'] == 'terminate_migrator':
      for kid in self._kids.values():
        self._kid_migrator_is_terminated[kid['id']] = False
        self._node.send(kid, messages.migration.terminate_migrator(self.migration_id))
      self._maybe_kids_are_terminated()
    elif message['type'] == 'migrator_terminated':
      self._kid_migrator_is_terminated[sender_id] = True
      self._maybe_kids_are_terminated()
    elif message['type'] == 'configure_new_flow_right':
      self._node.logger.info("Received 'configure_new_flow_right'", extra={'sender_id': sender_id})
      for right_configuration in message['right_configurations']:
        parent = right_configuration['parent_handle']
        self._node.export_to_node(parent)
        self._right_config_receiver.got_configuration(right_configuration)
        self._receivers[parent['id']] = parent

      if self._node.parent is None and self._right_config_receiver.ready:
        self._height = max(self._height, self._max_right_height())
        self._send_configure_right_to_left()
      self._maybe_has_left_and_right_configurations()
    elif message['type'] == 'configure_new_flow_left':
      self._node.logger.info("Received 'configure_new_flow_left'", extra={'sender_id': sender_id})
      for left_configuration in message['left_configurations']:
        self._left_configurations[left_configuration['node']['id']] = left_configuration
      self._maybe_has_left_and_right_configurations()
    elif message['type'] == 'configure_right_parent':
      self._right_config_receiver.got_parent_configuration(sender_id, kid_ids=message['kid_ids'])
      self._maybe_has_left_and_right_configurations()
    elif message['type'] == 'substitute_right_parent':
      self._right_config_receiver.substitute_right_parent(sender_id, new_parent_id=message['new_parent_id'])
      self._maybe_has_left_and_right_configurations()
    else:
      raise errors.InternalError('Unrecognized migration message type "{}"'.format(message['type']))

  def _maybe_kids_are_terminated(self):
    if all(self._kid_migrator_is_terminated.values()):
      self._node.activate_swap(self.migration_id, new_receivers=self._receivers, kids=self._kids)
      self._node.remove_migrator(self.migration_id)
      self._node.send(self.parent, messages.migration.migrator_terminated(self.migration_id))

  def _send_configure_right_to_left(self):
    if not self._right_configurations_are_sent:
      self._node.logger.info("Sending configure_new_flow_right", extra={'receiver_ids': list(self._senders.keys())})
      for sender in self._senders.values():
        self._node.send(sender,
                        messages.migration.configure_new_flow_right(self.migration_id, [
                            messages.migration.right_configuration(
                                n_kids=None,
                                parent_handle=self._node.new_handle(sender['id']),
                                height=self._height,
                                is_data=self._node.is_data(),
                                connection_limit=self._node.system_config['SUM_NODE_SENDER_LIMIT'],
                            )
                        ]))
      self._right_configurations_are_sent = True

  def _new_topology_picker(self):
    return topology_picker.TopologyPicker(
        graph=self._graph,
        # TODO(KK): There is probably a better way to configure these standard limits than the below.
        # Look into it, write up some notes, and fix it.
        new_node_max_outputs=self._node.system_config['SUM_NODE_RECEIVER_LIMIT'],
        new_node_max_inputs=self._node.system_config['SUM_NODE_SENDER_LIMIT'],
        new_node_name_prefix='SumNode' if self._height == 0 else 'ComputationNode',
    )

  def _maybe_has_left_and_right_configurations(self):
    if not self._flow_is_started and \
        self._right_config_receiver.ready and \
        all(val is not None for val in self._left_configurations.values()):

      from dist_zero.node.computation import ComputationNode
      self._node.logger.info("Insertion migrator has received all Left and Right configurations. Ready to spawn.")
      self._left_layer = {
          kid['handle']['id']: kid['handle']
          for left_configuration in self._left_configurations.values() for kid in left_configuration['kids']
      }
      self._graph = NetworkGraph()

      for left_id in self._left_layer.keys():
        self._graph.add_node(left_id)

      self._left_is_data = any(config['is_data'] for config in self._left_configurations.values())
      self._right_is_data = any(config['is_data'] for config in self._right_config_receiver.configs.values())
      self._picker = self._new_topology_picker()

      if self._node.__class__ != ComputationNode:
        self._height = 0
        self._all_kids_are_spawned()
      else:
        max_left_height, max_right_height = self._max_left_height(), self._max_right_height()
        self._height = max(max_left_height, max_right_height)

        # Decide on a network topology and spawn new kids
        self._right_map = self._picker.fill_graph(
            left_is_data=self._left_is_data,
            left_height=max_left_height,
            right_is_data=self._right_is_data,
            right_height=max_right_height,
            right_configurations=self._right_config_receiver.configs.values())

        self._spawn_layer(1)

  def _max_left_height(self):
    return max(config['height'] for config in self._left_configurations.values())

  def _max_right_height(self):
    return max(config['height'] for config in self._right_config_receiver.configs.values())

  def _id_to_handle(self, node_id):
    '''
    Get the handle for a child or left-child node.

    :param str node_id: The id of a kid node, or a node to the left of a kid node.
    :return: The :ref:`handle` associated with ``node_id``
    :rtype: :ref:`handle`
    '''
    if node_id in self._left_layer:
      return self._left_layer[node_id]
    elif node_id in self._kids:
      return self._kids[node_id]
    else:
      raise errors.InternalError('Node id "{}" was not found in the kids on left-kids.'.format(node_id))

  def _spawn_kid(self, layer_index, node_id, senders, migrator):
    self._kids[node_id] = None
    if self._height == 0:
      self._node._controller.spawn_node(
          messages.sum.sum_node_config(
              node_id=node_id,
              left_is_data=self._node.left_is_data and layer_index == 1,
              # TODO(KK): This business about right_map here is very ugly.  Think hard and try to come up with a way to avoid it.
              right_is_data=self._node.right_is_data and layer_index + 1 == self._picker.n_layers
              and bool(self._right_map[node_id]),
              senders=senders,
              receivers=[],
              parent=self._node.new_handle(node_id),
              migrator=migrator,
          ))
    else:
      self._node._controller.spawn_node(
          messages.computation.computation_node_config(
              node_id=node_id,
              parent=self._node.new_handle(node_id),
              height=self._height - 1,
              left_is_data=self._node.left_is_data and layer_index == 1,
              # TODO(KK): This business about right_map here is very ugly.  Think hard and try to come up with a way to avoid it.
              right_is_data=self._node.right_is_data and layer_index + 1 == self._picker.n_layers
              and bool(self._right_map[node_id]),
              senders=senders,
              receivers=[],
              migrator=migrator))

  def _send_configure_right_parent(self, layer_index):
    '''Send configure_riht_parent to all nodes in layer_index.'''
    for left_node_id in self._picker.get_layer(layer_index):
      self._node.send(
          self._id_to_handle(left_node_id),
          messages.migration.configure_right_parent(
              migration_id=self.migration_id, kid_ids=self._graph.node_receivers(left_node_id)))

  def _spawn_layer_without_gap(self, layer_index):
    self._send_configure_right_parent(layer_index - 1)

    for node_id in self._picker.get_layer(layer_index):
      senders = [self._id_to_handle(sender_id) for sender_id in self._graph.node_senders(node_id)]
      self._spawn_kid(
          layer_index=layer_index,
          node_id=node_id,
          senders=senders,
          migrator=messages.migration.insertion_migrator_config(
              senders=senders,
              left_configurations=None,
              configure_right_parent_ids=self._get_right_parent_ids_for_kid(node_id, layer_index),
              receivers=[],
              migration=self._node.transfer_handle(self._migration, node_id),
          ))

  def _get_unique_node_in_layer(self, layer_index):
    node_ids = self._picker.get_layer(layer_index)
    if len(node_ids) != 1:
      raise errors.InternalError("Layer is expected to have exactly one node.")

    return node_ids[0]

  def _spawn_layer_with_right_gap(self, layer_index):
    '''Spawn the rightmost layer of nodes, when it contains a single node, and should be spawned
    to re-use the right-configuration of self.'''
    self._left_configurations_are_sent = True # No need to send any more left configurations
    self._send_configure_right_parent(layer_index - 1)
    node_id = self._get_unique_node_in_layer(layer_index)
    for right_config in self._right_config_receiver.configs.values():
      # Let right parents know to listen for a left_config from the new node instead of from self.
      self._node.send(right_config['parent_handle'],
                      messages.migration.substitute_left_configuration(self.migration_id, new_node_id=node_id))

    senders = [self._id_to_handle(sender_id) for sender_id in self._graph.node_senders(node_id)]
    self._spawn_kid(
        layer_index=layer_index,
        node_id=node_id,
        senders=senders,
        migrator=messages.migration.insertion_migrator_config(
            senders=senders,
            left_configurations=None,
            right_configurations=self._right_config_receiver.configs,
            configure_right_parent_ids=self._configure_right_parent_ids,
            receivers=[],
            migration=self._node.transfer_handle(self._migration, node_id),
        ))

  def _spawn_layer_with_left_gap(self, layer_index):
    '''
    We want to keep the left height the same in the next layer.
    Spawn a layer with the same left configurations as self, and inform the nodes
    to the left to listen to new parents for their right_configurations.

    :param int layer_index: The layer of nodes to spawn.
    '''
    node_id = self._get_unique_node_in_layer(layer_index)

    for left_node_id in self._picker.get_layer(layer_index - 1):
      # Let nodes to the left know that they must listen for configure_right_parent from ``node_id``
      # instead of from self.
      self._node.send(
          self._id_to_handle(left_node_id),
          messages.migration.substitute_right_parent(
              migration_id=self.migration_id,
              new_parent_id=node_id,
          ))

    self._spawn_kid(
        layer_index=layer_index,
        node_id=node_id,
        senders=[],
        migrator=messages.migration.insertion_migrator_config(
            senders=[],
            left_configurations=self._left_configurations,
            configure_right_parent_ids=self._get_right_parent_ids_for_kid(node_id, layer_index),
            receivers=[],
            migration=self._node.transfer_handle(self._migration, node_id),
        ))

  def _get_right_parent_ids_for_kid(self, node_id, layer_index):
    if layer_index + 1 < self._picker.n_layers:
      return [self._node.id]
    else:
      return self._right_map[node_id]

  def _spawn_layer(self, layer_index):
    self._node.logger.info(
        "Insertion migrator is spawning layer {layer_index} of {n_nodes_to_spawn} nodes",
        extra={
            'layer_index': layer_index,
            'n_nodes_to_spawn': len(self._picker.get_layer(layer_index))
        })
    self._current_spawning_layer = layer_index

    if self._max_left_height() < self._max_right_height() and layer_index == 1:
      self._spawn_layer_with_left_gap(layer_index)
    elif self._max_left_height() > self._max_right_height() and layer_index == self._picker.n_layers - 1:
      self._spawn_layer_with_right_gap(layer_index)
    else:
      self._spawn_layer_without_gap(layer_index)

    self._maybe_spawned_kids()

  def _maybe_spawned_kids(self):
    if not self._flow_is_started and \
        all(val is not None for val in self._kids.values()):
      if self._current_spawning_layer + 1 < self._picker.n_layers:
        self._spawn_layer(self._current_spawning_layer + 1)
      else:
        self._all_kids_are_spawned()

  def _all_kids_are_spawned(self):
    if not self._left_configurations_are_sent:
      self._node.set_picker(self._picker)
      self._send_configure_left_to_right()

  def _receiver_to_kids(self):
    if self._right_map is not None:
      result = {}
      for right_node_id, receiver_ids in self._right_map.items():
        for receiver_id in receiver_ids:
          if receiver_id not in result:
            result[receiver_id] = [right_node_id]
          else:
            result[receiver_id].append(right_node_id)
      return result
    else:
      return {receiver_id: [] for receiver_id in self._receivers.keys()}

  def _send_configure_left_to_right(self):
    self._left_configurations_are_sent = True
    self._node.logger.info("Sending configure_new_flow_left", extra={'receiver_ids': list(self._receivers.keys())})

    receiver_to_kids = self._receiver_to_kids()

    for right_config in self._right_config_receiver.configs.values():
      receiver = right_config['parent_handle']
      message = messages.migration.configure_new_flow_left(self.migration_id, [
          messages.migration.left_configuration(
              node=self._node.new_handle(receiver['id']),
              height=self._height,
              is_data=self._node.is_data(),
              kids=[{
                  'handle': self._node.transfer_handle(self._kids[kid_id], receiver['id']),
                  'connection_limit': self._node.system_config['SUM_NODE_RECEIVER_LIMIT']
              } for kid_id in receiver_to_kids.get(receiver['id'], [])],
          )
      ])

      self._node.send(receiver, message)

  def _maybe_swap(self):
    if self._waiting_for_swap and \
        len(self._new_sender_id_to_first_live_sequence_number) == len(self._senders) and \
        self._node._deltas.covers(self._new_sender_id_to_first_live_sequence_number):

      self._node.logger.info("Insertion node is swapping flows.")
      self._node.checkpoint(self._new_sender_id_to_first_live_sequence_number)
      self._node.deltas_only.remove(self.migration_id)

      if settings.IS_TESTING_ENV:
        self._node._TESTING_swapped_once = True

      for receiver_id in self._receivers.keys():
        exporter = self._node._exporters[receiver_id]
        self._node.send(exporter.receiver,
                        messages.migration.swapped_to_duplicate(
                            self.migration_id, first_live_sequence_number=exporter._internal_sequence_number))

      self._waiting_for_swap = False
      self._node.activate_swap(self.migration_id, new_receivers=self._receivers, kids=self._kids,
          use_output=len(self._right_config_receiver.configs) == 1 and \
              next(iter(self._right_config_receiver.configs.values()))['is_data'],
          use_input=len(self._left_configurations) == 1 and next(iter(self._left_configurations.values()))['is_data'])

  @property
  def migration_id(self):
    return self._migration['id']

  def elapse(self, ms):
    self._maybe_swap()

  def initialize(self):
    self._node.deltas_only.add(self.migration_id)

    self._node.send(
        self.parent,
        message=messages.migration.attached_migrator(self.migration_id, self._node.new_handle(self.parent['id'])))

    if self._node.parent is not None:
      self._send_configure_right_to_left()
