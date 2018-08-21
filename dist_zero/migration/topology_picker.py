from dist_zero import errors, ids


class TopologyPicker(object):
  '''
  Each instance of `TopologyPicker` will take some constraints about nodes in a network topology,
  and determine a new set of nodes and their connections in such a way that it meets the constraints.
  '''

  def __init__(self, graph, max_outputs, max_inputs, new_node_max_outputs, new_node_max_inputs, new_node_name_prefix):
    self._graph = graph
    self._left_layer = [node for node in graph.nodes()]
    self._layers = [self._left_layer]

    self._max_outputs = max_outputs
    self._max_inputs = max_inputs

    self._new_node_max_outputs = new_node_max_outputs
    self._new_node_max_inputs = new_node_max_inputs

    self._new_node_name_prefix = new_node_name_prefix

  @property
  def n_layers(self):
    return len(self._layers)

  def get_layer(self, i):
    return self._layers[i]

  def _add_left_adjacents_layer(self):
    left_layer = []
    for left in self._graph.nodes():
      node_id = self._new_node()
      self._graph.add_edge(left, node_id)
      left_layer.append(node_id)

    self._layers.append(left_layer)

  def _add_partition_layer(self):
    import ipdb
    ipdb.set_trace()

  def _add_right_adjacents_layer(self, right_configurations):
    right_map = {}

    right_layer = []
    for right_config in right_configurations:
      if right_config['n_kids'] is None:
        raise errors.InternalError("right_config must have n_kids when right_is_data == True")
      for i in range(right_config['n_kids']):
        node_id = self._new_node()
        right_layer.append(node_id)
        right_map[node_id] = [right_config['parent_handle']['id']]

    if len(right_layer) == 0:
      # There must be at least one node in this last layer.
      node_id = self._new_node()
      right_layer.append(node_id)
      if len(right_configurations) != 1:
        import ipdb
        ipdb.set_trace()
        raise RuntimeError("Not Yet Implemented")
      right_map[node_id] = [right_config['parent_handle']['id'] for right_config in right_configurations]

    for left in self._layers[-1]:
      for right in right_layer:
        self._graph.add_edge(left, right)
    self._layers.append(right_layer)

    return right_map

  def fill_graph(self, left_is_data, right_is_data, right_configurations):
    if left_is_data:
      self._add_left_adjacents_layer()

    max_in_last_layer = max(right_config['connection_limit'] for right_config in right_configurations)

    while len(self._layers[-1]) > max_in_last_layer:
      self._add_partition_layer()

    if right_is_data:
      return self._add_right_adjacents_layer(right_configurations)
    else:
      if len(self._layers[-1]) == 0:
        singleton = self._new_node()
        self._layers.append([singleton])

      return {
          node: [right_config['parent_handle']['id'] for right_config in right_configurations]
          for node in self._layers[-1]
      }

  # FIXME(KK): Remove this
  def fill_graph_old(self, left_is_data, right_is_data, right_configurations):
    if left_is_data and right_is_data:
      left_layer = []
      for left in self._graph.nodes():
        node_id = self._new_node()
        self._graph.add_edge(left, node_id)
        left_layer.append(node_id)

      right_layer = []
      right_map = {}
      for right_config in right_configurations:
        for i in range(right_config['n_kids']):
          node_id = self._new_node()
          right_map[node_id] = [right_config['parent_handle']['id']]
          right_layer.append(node_id)

      if len(right_layer) <= self._new_node_max_outputs and len(left_layer) <= self._new_node_max_inputs:
        self._layers.append(left_layer)
        self._layers.append(right_layer)
        for right in right_layer:
          for left in left_layer:
            self._graph.add_edge(left, right)
        return right_map
      else:
        # FIXME(KK): Test and implement more cases.
        raise RuntimeError("Not Yet Implemented")
    elif left_is_data and not right_is_data:
      if len(self._graph.nodes()) == 0 and len(right_configurations) > 0:
        layer = [self._new_node()]
        self._layers.append(layer)
        return {layer[0]: [right_config['parent_handle']['id'] for right_config in right_configurations]}
      else:
        left_layer = []
        for left in self._graph.nodes():
          node_id = self._new_node()
          self._graph.add_edge(left, node_id)
          left_layer.append(node_id)
        if len(right_configurations) <= self._new_node_max_outputs:
          if all(config['connection_limit'] >= len(left_layer) for config in right_configurations):
            self._layers.append(left_layer)
            right_map = {
                left: [right_config['parent_handle']['id'] for right_config in right_configurations]
                for left in left_layer
            }
            return right_map
          else:
            # FIXME(KK): In this case, we should probably be spawning more layers.
            raise RuntimeError("Not Yet Implemented")
        else:
          # FIXME(KK): In this case, we should probably be spawning more layers.
          raise RuntimeError("Not Yet Implemented")
    elif not left_is_data and right_is_data:
      left_nodes = self._graph.nodes()
      right_layer = []
      right_map = {}
      for right_config in right_configurations:
        for i in range(right_config['n_kids']):
          node_id = self._new_node()
          right_map[node_id] = [right_config['parent_handle']['id']]
          right_layer.append(node_id)
      if len(right_layer) > 0:
        import ipdb
        ipdb.set_trace()
        # FIXME(KK): In this case, we should probably try a complete connection
        raise RuntimeError("Not Yet Implemented")
      else:
        if len(left_nodes) <= self._new_node_max_inputs:
          right_layer = [self._new_node()]
          self._layers.append(right_layer)
          for left in left_nodes:
            self._graph.add_edge(left, right_layer[0])
          return {right_layer[0]: [right_config['parent_handle']['id'] for right_config in right_configurations]}
        else:
          # FIXME(KK): In this case, we should probably try a complete connection
          raise RuntimeError("Not Yet Implemented")
    else:
      # FIXME(KK): We are currently implementing only one very specific special case.
      #   Test and implement the other more general cases.
      raise RuntimeError("Not Yet Implemented")

  def _new_node(self):
    node_id = ids.new_id(self._new_node_name_prefix)
    self._graph.add_node(node_id)
    return node_id

  def fill_graph_to_right_siblings(self, right_configurations):
    first_layer = list(self._graph.nodes())
    last_layer = []
    right_to_parent = {}
    for parent_id, right_config in right_configurations.items():
      for i in range(right_config['n_kids']):
        node_id = self._new_node()
        right_to_parent[node_id] = parent_id
        last_layer.append(node_id)

    # FIXME(KK): Write up a more general algorithm.  The current is a standin to pass
    #   the first round of test cases.
    self._layers.append(first_layer)
    while True:
      # Invariants:
      #   - self._layers[:filled_to] are totally connected from left to right
      #   - self._layers[filletd_to+1:] are totally connected from left to right
      if filled_to + 1 == len(self._layers):
        # We're at the right

        if any(right_config['n_kids'] is not None for right_config in self._right_configurations.values()):
          if any(right_config['n_kids'] is None for right_config in self._right_configurations.values()):
            raise errors.InternalError("If one right_configuration has n_kids, then they all should")

      raise RuntimeError("Not Yet Implemented")

    return self._get_left_configurations()

  def new_rightmost_nodes(self):
    if len(self._layers) <= 1:
      return []
    else:
      return self._layers[len(self._layers) - 1]

  def _add_complete_connection(self, i):
    self._connections.insert(i, {'type': 'complete'})
    for left_node in self._layers[i]:
      for right_node in self._layers[i + 1]:
        edge = (left_node, right_node)
        self._outgoing_edges[left_node].append(edge)
        self._incomming_edges[right_node].append(edge)

  def _get_violation(self):
    # Outgoing violations:
    for i in range(len(self._layers) - 1):
      violation = self._get_outgoing_violation(i)
      if violation is not None:
        return violation

    # Incomming violations
    for i in range(1, len(self._layers)):
      violation = self._get_incomming_violation(i)
      if violation is not None:
        return violation

    return None

  def _get_outgoing_violation(self, i):
    for node_id in self._layers[i]:
      if len(self._outgoing_edges[node_id]) > self._outgoing_edge_limit[node_id]:
        return {'type': 'too_many_outgoing_edges', 'layer': i}

    return None

  def _get_incomming_violation(self, i):
    for node_id in self._layers[i]:
      if (len(self._incomming_edges[node_id])
          if node_id in self._incomming_edges else 0) > self._incomming_edge_limit[node_id]:
        return {'type': 'too_many_incomming_edges', 'layer': i}

    return None

  def _connect_to_right(self, left, right):
    '''For connecting a node to a rightmost parent.'''
    self._rightmost_connections.append((left, right))

  def _add_right_layer(self, right_n_kids):
    new_layer_of_right_adjacents = []
    for right, n_kids in right_n_kids.items():
      for i in range(n_kids):
        kid = self._new_node()
        self._connect_to_right(kid, right)
        new_layer_of_right_adjacents.append(kid)

    self._layers.insert(len(self._layers) - 1, new_layer_of_right_adjacents)
    self._connections.insert(0, {'type': 'right_adjacency'})

  def _fix_violation(self, violation):
    # FIXME(KK): Test and implement all of these.
    if violation['type'] == 'too_many_incomming_edges':
      raise RuntimeError("Not Yet Implemented")
    elif violation['type'] == 'too_many_outgoing_edges':
      raise RuntimeError("Not Yet Implemented")
    else:
      raise errors.InternalError('Unrecognized node topology violation type "{}"'.format(violation['type']))
