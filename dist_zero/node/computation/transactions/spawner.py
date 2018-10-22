from dist_zero import errors, messages


class SpawnerTransaction(object):
  '''
  For spawning, one at a time, the layers generated by a newly initialized `Connector` instance.
  '''

  def __init__(self, connector, node):
    self._connector = connector
    self._node = node

    self._left_configurations_are_sent = False

    # These will be set to true later on if we determine we should add left/right gaps.
    self._node._left_gap = False
    self._node._right_gap = False

    self._current_spawning_layer = None
    self.finished = False

    # When the subgraph has a gap on the left, this will be the id of the unique leftmost node of the subgraph
    self._left_gap_child_id = None
    # When the subgraph has a gap on the right, this will be the id of the unique right node of the subgraph
    self._right_gap_child_id = None

    self._layer_kid_ids = set(node_id for layer in self._connector.layers for node_id in layer)

  def receive(self, message, sender_id):
    if message['type'] == 'hello_parent' and sender_id in self._layer_kid_ids:
      self.spawned_a_kid(message['kid'])
      return True

    return False

  def _id_to_handle(self, node_id):
    '''
    Get the handle for a child or left-child node.

    :param str node_id: The id of a kid node, or a node to the left of a kid node.
    :return: The :ref:`handle` associated with ``node_id``
    :rtype: :ref:`handle`
    '''
    if node_id in self._connector._left_layer:
      return self._connector._left_layer[node_id]
    elif node_id in self._node.kids:
      return self._node.kids[node_id]
    else:
      raise errors.InternalError('Node id "{}" was not found in the kids on left-kids.'.format(node_id))

  @property
  def graph(self):
    return self._connector.graph

  def _spawn_layer_without_gap(self, layer_index):
    self._send_configure_right_parent(layer_index - 1)

    for node_id in self._connector.layers[layer_index]:
      left_ids = self.graph.node_senders(node_id)
      if self._node._initial_migrator is None:
        migrator = None
      else:
        migrator = messages.migration.insertion_migrator_config(
            self._node.transfer_handle(self._node._initial_migrator.migration, node_id))

      self._node.spawn_kid(
          layer_index=layer_index,
          configure_right_parent_ids=self._get_right_parent_ids_for_kid(node_id, layer_index),
          left_ids=left_ids,
          node_id=node_id,
          senders=[self._id_to_handle(sender_id) for sender_id in left_ids],
          migrator=migrator)

  def _get_unique_node_in_layer(self, layer_index):
    node_ids = self._connector.layers[layer_index]
    if len(node_ids) != 1:
      raise errors.InternalError("Layer is expected to have exactly one node.")

    return node_ids[0]

  def _spawn_layer_with_right_gap(self, layer_index):
    '''Spawn the rightmost layer of nodes, when it contains a single node, and should be spawned
    to re-use the right-configuration of self.'''
    self._node._right_gap = True
    self._left_configurations_are_sent = True # No need to send any more left configurations
    self._send_configure_right_parent(layer_index - 1)
    node_id = self._get_unique_node_in_layer(layer_index)
    for right_config in self._connector._right_configurations.values():
      # Let right parents know to listen for a left_config from the new node instead of from self.
      self._node.send(right_config['parent_handle'],
                      messages.migration.substitute_left_configuration(self._node.migration_id, new_node_id=node_id))

    left_ids = self._connector.graph.node_senders(node_id)
    self._right_gap_child_id = node_id
    self._node.spawn_kid(
        layer_index=layer_index,
        node_id=node_id,
        left_ids=left_ids,
        senders=[self._id_to_handle(sender_id) for sender_id in left_ids],
        configure_right_parent_ids=[self._node.id],
        migrator=self._node._initial_migrator and messages.migration.insertion_migrator_config(
            self._node.transfer_handle(self._node._initial_migrator.migration, node_id)))

  def _spawn_layer_with_left_gap(self, layer_index):
    '''
    We want to keep the left height the same in the next layer.
    Spawn a layer with the same left configurations as self, and inform the nodes
    to the left to listen to new parents for their right_configurations.

    :param int layer_index: The layer of nodes to spawn.
    '''
    self._node._left_gap = True
    node_id = self._get_unique_node_in_layer(layer_index)

    for left_node_id in self._connector.layers[layer_index - 1]:
      # Let nodes to the left know that they must listen for configure_right_parent from ``node_id``
      # instead of from self.
      self._node.send(
          self._id_to_handle(left_node_id),
          messages.migration.substitute_right_parent(
              migration_id=self._node.migration_id,
              new_parent_id=node_id,
          ))

    self._left_gap_child_id = node_id

    self._node.spawn_kid(
        layer_index=layer_index,
        node_id=node_id,
        senders=[],
        # By adding left_ids, we ensure the child waits for self to send left configurations.
        left_ids=[left_config['node']['id'] for left_config in self._connector._left_configurations.values()],
        configure_right_parent_ids=self._get_right_parent_ids_for_kid(node_id, layer_index),
        migrator=messages.migration.insertion_migrator_config(
            self._node.transfer_handle(self._node._initial_migrator.migration, node_id)))

  def _get_right_parent_ids_for_kid(self, node_id, layer_index):
    if layer_index + 1 < len(self._connector.layers):
      return [self._node.id]
    else:
      return self._connector.right_to_parent_ids[node_id]

  def start(self):
    self._spawn_layer(1)

  def _spawn_layer(self, layer_index):
    self._node.logger.info(
        "Insertion migrator is spawning layer {layer_index} of {n_nodes_to_spawn} nodes",
        extra={
            'layer_index': layer_index,
            'n_nodes_to_spawn': len(self._connector.layers[layer_index])
        })
    self._current_spawning_layer = layer_index

    if self._connector._left_configurations and self._connector._right_configurations:
      if layer_index == 1 and self._connector.max_left_height() < self._connector.max_right_height():
        self._spawn_layer_with_left_gap(layer_index)
      elif layer_index == len(self._connector.layers) - 1 and \
          self._connector.max_left_height() > self._connector.max_right_height():
        self._spawn_layer_with_right_gap(layer_index)
      else:
        self._spawn_layer_without_gap(layer_index)
    else:
      self._spawn_layer_without_gap(layer_index)

    self._maybe_spawned_kids()

  def spawned_a_kid(self, node):
    self._node.kids[node['id']] = node
    if node['id'] == self._left_gap_child_id:
      self._node.send(node,
                      messages.migration.configure_new_flow_left(self._node.migration_id, [
                          messages.migration.left_configuration(
                              height=left_config['height'],
                              is_data=left_config['is_data'],
                              node=self._node.transfer_handle(left_config['node'], node['id']),
                              kids=[{
                                  'handle': self._node.transfer_handle(kid['handle'], node['id']),
                                  'connection_limit': kid['connection_limit']
                              } for kid in left_config['kids']],
                          ) for left_config in self._connector._left_configurations.values()
                      ]))
    elif node['id'] == self._right_gap_child_id:
      self._node.send(node,
                      messages.migration.configure_right_parent(
                          self._node.migration_id, kid_ids=list(self._connector._right_configurations.keys())))
      self._node.send(node,
                      messages.migration.configure_new_flow_right(self._node.migration_id, [
                          messages.migration.right_configuration(
                              parent_handle=self._node.transfer_handle(right_config['parent_handle'], node['id']),
                              height=right_config['height'],
                              is_data=right_config['is_data'],
                              n_kids=right_config['n_kids'],
                              connection_limit=right_config['connection_limit'],
                          ) for right_config in self._connector._right_configurations.values()
                      ]))
    self._maybe_spawned_kids()

  def _send_configure_right_parent(self, layer_index):
    '''Send configure_right_parent to all nodes in layer_index.'''
    for left_node_id in self._connector.layers[layer_index]:
      self._node.send(
          self._id_to_handle(left_node_id),
          messages.migration.configure_right_parent(
              migration_id=self._node.migration_id, kid_ids=self._connector._graph.node_receivers(left_node_id)))

  def _maybe_spawned_kids(self):
    if not self.finished and \
        all(val is not None for val in self._node.kids.values()):
      if self._current_spawning_layer + 1 < len(self._connector.layers):
        self._spawn_layer(self._current_spawning_layer + 1)
      else:
        self.finished = True

        if self._node._left_gap and self._node._right_gap:
          raise errors.InternalError("There may not be both a left and a right gap.")
        if self._left_configurations_are_sent:
          raise errors.InternalError("all_kids_are_spawned should only be called before left_configurations are sent.")

        self._node._send_configure_left_to_right()
        if self._node.parent:
          self._node._send_hello_parent()

        self._node.end_transaction()
