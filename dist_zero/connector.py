from collections import defaultdict

from dist_zero import errors, topology_picker, network_graph, ids, messages


class Connector(object):
  def __init__(self, height, left_configurations, right_configurations, max_outputs, max_inputs):
    self._height = height
    self._left_configurations = left_configurations
    self._right_configurations = right_configurations
    self._left_is_data = any(config['is_data'] for config in self._left_configurations.values())
    self._right_is_data = any(config['is_data'] for config in self._right_configurations.values())

    self._max_outputs = max_outputs
    self._max_inputs = max_inputs

    self._name_prefix = 'ComputationNode' if self._height > 0 else 'SumNode'

    # If left is not data, the left nodes are identically the picker left nodes,
    # and this variable must be `None`.
    self._left_node_to_picker_left_node = None

    self._lefts = None
    self._rights = None
    self._picker = None

    self._layers = []
    self._right_to_parent_ids = None
    self._graph = network_graph.NetworkGraph()

    self._fill_in()

  @property
  def right_siblings(self):
    return {
        right_parent_id: right_config['parent_handle']
        for right_parent_id, right_config in self._right_configurations.items()
    }

  def max_height(self):
    return max(self.max_left_height(), self.max_right_height())

  def max_left_height(self):
    return max((config['height'] for config in self._left_configurations.values()), default=-1)

  def max_right_height(self):
    return max((config['height'] for config in self._right_configurations.values()), default=-1)

  def add_kid_to_left_configuration(self, parent_id, kid):
    '''
    Update the set of connections so that there is a new node in
    the leftmost layer.

    :return: A triplet (new_nodes, new_edges, hourglasses)
      where new_nodes are triplets (node_id, senders, receivers) to spawn
      new_edges are pairs (src_node_id, tgt_node_id) defining connections
      not defined by new_nodes, and hourglasses are triplets
      (node_id, senders, receivers) giving the hourglass substitutions.
    '''
    left_config = self._left_configurations[parent_id]
    left_config['kids'].append(kid)
    node_id = kid['id']

    self._layers[0].append(node_id)

    if self._left_is_data:
      adjacent_id = ids.new_id("{}_adjacent".format(self._name_prefix))
      self._layers[1].append(adjacent_id)
      self._graph.add_node(node_id)
      new_nodes, hourglasses = self._picker.append_left(adjacent_id)
      self._graph.add_edge(node_id, adjacent_id)
      result_nodes = [((nid, senders, receivers) if nid != adjacent_id else (nid, [node_id] + senders, receivers))
                      for nid, senders, receivers in new_nodes]

      return result_nodes, [], hourglasses
    else:
      new_nodes, hourglasses = self._picker.append_left(node_id)
      result_nodes = [(nid, senders, receivers) for nid, senders, receivers in new_nodes if nid != node_id]
      if not result_nodes:
        # There must be a single new node.
        nid, senders, receivers = new_nodes[0]
        edges = [(nid, sender_id) for sender_id in senders]
      else:
        edges = []
      return result_nodes, edges, hourglasses

  @property
  def layers(self):
    return self._layers

  @property
  def graph(self):
    return self._graph

  def _initialize_picker_lefts(self):
    '''
    Called once at startup to initialize self._left_node_to_picker_left_node and return the
    list of left ids to be used by the picker.
    '''
    if self._left_is_data:
      self._layers.append([])
      self._left_node_to_picker_left_node = {}
      for left in self._lefts:
        self._layers[0].append(left['id'])
        adjacent_id = ids.new_id("{}_adjacent".format(self._name_prefix))
        self._graph.add_node(left['id'])
        self._graph.add_node(adjacent_id)
        self._graph.add_edge(left['id'], adjacent_id)
        self._left_node_to_picker_left_node[left['id']] = adjacent_id
      return list(self._left_node_to_picker_left_node.values())
    else:
      self._left_node_to_picker_left_node = None
      return [left['id'] for left in self._lefts]

  @property
  def right_to_parent_ids(self):
    return self._right_to_parent_ids

  def _initialize_picker_rights(self):
    '''
    Called once at startup to initialize self._right_to_parent_ids and return the
    list of right ids to be used by the picker.
    '''
    self._right_to_parent_ids = defaultdict(list)
    if self._right_is_data:
      for right_config in self._right_configurations.values():
        n_kids = right_config['n_kids']
        if n_kids is None:
          raise errors.InternalError("When the right nodes are data nodes, the right configs must contain"
                                     " a not None 'n_kids' parameter.")
        for i in range(n_kids):
          adjacent_id = ids.new_id("{}_adjacent".format(self._name_prefix))
          self._right_to_parent_ids[adjacent_id].append(right_config['parent_handle']['id'])
    else:
      for right_config in self._right_configurations.values():
        right_id = ids.new_id("{}_rightmost".format(self._name_prefix))
        self._right_to_parent_ids[right_id].append(right_config['parent_handle']['id'])

    return list(self._right_to_parent_ids.keys())

  def _fill_in(self):
    self._lefts = [kid['handle'] for left_config in self._left_configurations.values() for kid in left_config['kids']]

    picker_lefts = self._initialize_picker_lefts()
    picker_rights = self._initialize_picker_rights()

    self._picker = topology_picker.TopologyPicker(
        graph=self._graph,
        lefts=picker_lefts,
        rights=picker_rights,
        max_outputs=self._max_outputs,
        max_inputs=self._max_inputs,
        name_prefix=self._name_prefix)

    self._layers.extend(self._picker.layers)


class Spawner(object):
  def __init__(self, connector, node):
    self._connector = connector
    self._node = node

    self._current_spawning_layer = None
    self.finished = False

    # When the subgraph has a gap on the left, this will be the id of the unique leftmost node of the subgraph
    self._left_gap_child_id = None
    # When the subgraph has a gap on the right, this will be the id of the unique right node of the subgraph
    self._right_gap_child_id = None

  def _id_to_handle(self, node_id):
    '''
    Get the handle for a child or left-child node.

    :param str node_id: The id of a kid node, or a node to the left of a kid node.
    :return: The :ref:`handle` associated with ``node_id``
    :rtype: :ref:`handle`
    '''
    if node_id in self._left_layer:
      return self._left_layer[node_id]
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
      senders = [self._id_to_handle(sender_id) for sender_id in self.graph.node_senders(node_id)]
      if self._node._initial_migrator is not None:
        migrator = messages.migration.insertion_migrator_config(
            senders=senders,
            receivers=[],
            migration=self._node.transfer_handle(self._node._initial_migrator.migration, node_id),
        )
      else:
        migrator = None
      self._node.spawn_kid(
          layer_index=layer_index,
          configure_right_parent_ids=self._get_right_parent_ids_for_kid(node_id, layer_index),
          node_id=node_id,
          senders=senders,
          migrator=migrator)

  def _get_unique_node_in_layer(self, layer_index):
    node_ids = self._connector.layers[layer_index]
    if len(node_ids) != 1:
      raise errors.InternalError("Layer is expected to have exactly one node.")

    return node_ids[0]

  def _spawn_layer_with_right_gap(self, layer_index):
    '''Spawn the rightmost layer of nodes, when it contains a single node, and should be spawned
    to re-use the right-configuration of self.'''
    self._left_configurations_are_sent = True # No need to send any more left configurations
    self._send_configure_right_parent(layer_index - 1)
    node_id = self._get_unique_node_in_layer(layer_index)
    for right_config in self._connector._right_configurations.values():
      # Let right parents know to listen for a left_config from the new node instead of from self.
      self._node.send(right_config['parent_handle'],
                      messages.migration.substitute_left_configuration(self._node.migration_id, new_node_id=node_id))

    senders = [self._id_to_handle(sender_id) for sender_id in self._connector.graph.node_senders(node_id)]
    self._right_gap_child_id = node_id
    self._node.spawn_kid(
        layer_index=layer_index,
        node_id=node_id,
        senders=senders,
        configure_right_parent_ids=[self._node.id],
        migrator=messages.migration.insertion_migrator_config(
            senders=senders,
            receivers=[],
            migration=self._node.transfer_handle(self._node._initial_migrator.migration, node_id),
        ))

  def _spawn_layer_with_left_gap(self, layer_index):
    '''
    We want to keep the left height the same in the next layer.
    Spawn a layer with the same left configurations as self, and inform the nodes
    to the left to listen to new parents for their right_configurations.

    :param int layer_index: The layer of nodes to spawn.
    '''
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
        configure_right_parent_ids=self._get_right_parent_ids_for_kid(node_id, layer_index),
        migrator=messages.migration.insertion_migrator_config(
            senders=[],
            receivers=[],
            migration=self._node.transfer_handle(self._node._initial_migrator.migration, node_id),
        ))

  def _get_right_parent_ids_for_kid(self, node_id, layer_index):
    if layer_index + 1 < len(self._connector.layers):
      return [self._node.id]
    else:
      return self._connector.right_to_parent_ids[node_id]

  def start_spawning(self):
    self._left_layer = {
        kid['handle']['id']: kid['handle']
        for left_config in self._connector._left_configurations.values() for kid in left_config['kids']
    }
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
      if self._connector.max_left_height() < self._connector.max_right_height() and layer_index == 1:
        self._spawn_layer_with_left_gap(layer_index)
      elif self._connector.max_left_height() > self._connector.max_right_height() and layer_index == len(
          self._connector.layers) - 1:
        self._spawn_layer_with_right_gap(layer_index)
      else:
        self._spawn_layer_without_gap(layer_index)
    else:
      self._spawn_layer_without_gap(layer_index)

    self._maybe_spawned_kids()

  def spawned_a_kid(self, node):
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
        self._node.all_kids_are_spawned()
