import itertools
from collections import defaultdict

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

    self._cur_left_tree_index = 0
    self._cur_right_tree_index = 0

    self._max_outputs = max_outputs
    self._max_inputs = max_inputs

    self._new_node_max_outputs = new_node_max_outputs
    self._new_node_max_inputs = new_node_max_inputs

    self._new_node_name_prefix = new_node_name_prefix

    self._node_by_tree_coords = {}
    self._tree_coords_by_node = {}

    self._right_edge = None
    self._left_edge = None

  @property
  def n_layers(self):
    return len(self._layers)

  def get_layer(self, i):
    return self._layers[i]

  def _new_left_tree_index(self):
    result = self._cur_left_tree_index
    self._cur_left_tree_index += 1
    return result

  def _new_right_tree_index(self):
    result = self._cur_right_tree_index
    self._cur_right_tree_index += 1
    return result

  def _form_right_tree_from_right_configurations(self, right_config_by_id, right_is_data, n_left_layers):
    self._right_edge = defaultdict(list)
    current_tree_layer = list(right_config_by_id.keys())

    layers = [current_tree_layer]

    adjacent_layer = []
    for node in current_tree_layer:
      if right_is_data:
        n_kids = right_config_by_id[node]['n_kids']
      else:
        n_kids = 1 if n_left_layers <= 2 else right_config_by_id[node]['connection_limit']

      if n_kids is None:
        raise errors.InternalError("n_kids should not be None")

      for i in range(n_kids):
        next_layer_index = self._new_right_tree_index()
        adjacent_layer.append(next_layer_index)
        self._right_edge[next_layer_index].append(node)

    # Forbid empty layers
    if len(adjacent_layer) == 0:
      next_layer_index = self._new_right_tree_index()
      adjacent_layer.append(next_layer_index)

    current_tree_layer = adjacent_layer
    layers.append(current_tree_layer)

    while len(current_tree_layer) > 1:
      next_layer = []
      for i in range(0, len(current_tree_layer), self._new_node_max_inputs):
        next_layer_index = self._new_right_tree_index()
        next_layer.append(next_layer_index)
        for node in current_tree_layer[i:i + self._new_node_max_inputs]:
          self._right_edge[next_layer_index].append(node)

      current_tree_layer = next_layer
      layers.append(current_tree_layer)

    return list(reversed(layers))

  def _form_left_tree_from_nodes(self, nodes):
    self._left_edge = {}
    current_tree_layer = nodes

    layers = [current_tree_layer]

    while len(current_tree_layer) > 1:
      next_layer = []
      for i in range(0, len(current_tree_layer), self._new_node_max_outputs):
        next_layer_index = self._new_left_tree_index()
        next_layer.append(next_layer_index)
        for node in current_tree_layer[i:i + self._new_node_max_outputs]:
          self._left_edge[node] = next_layer_index
      current_tree_layer = next_layer
      layers.append(current_tree_layer)

    return layers

  def _set_node_coords(self, node, coords):
    self._node_by_tree_coords[coords] = node
    self._tree_coords_by_node[node] = coords

  def _outgoing_nodes(self, node):
    left_index, right_index = self._tree_coords_by_node[node]
    outgoing_left_index = self._left_edge[left_index]
    return (self._node_by_tree_coords[(outgoing_left_index, outgoing_right_index)]
            for outgoing_right_index in self._right_edge[right_index])

  def _add_left_adjacents_layer(self):
    left_layer = []
    for left in self._graph.nodes():
      node_id = self._new_node()
      self._graph.add_edge(left, node_id)
      left_layer.append(node_id)
    self._layers.append(left_layer)

  def fill_graph(self, left_is_data, left_height, right_is_data, right_height, right_configurations):
    if left_is_data:
      self._add_left_adjacents_layer()

    right_config_by_id = {right_config['parent_handle']['id']: right_config for right_config in right_configurations}

    # Create a tree of left coordinates and of right coordinates
    left_layers = self._form_left_tree_from_nodes(self._layers[-1])
    right_layers = self._form_right_tree_from_right_configurations(right_config_by_id, right_is_data, len(left_layers))

    # Adjust the trees so that the have the same height.
    # Also, in order that this picker spawn at least one new node, that common height must be at least 3.
    while len(left_layers) < len(right_layers) or len(left_layers) < 3:
      new_node = self._new_left_tree_index()
      if left_layers[-1]:
        self._left_edge[left_layers[-1][0]] = new_node
      left_layers.append([new_node])
    while len(left_layers) > len(right_layers):
      new_node = self._new_right_tree_index()
      if right_layers[0]:
        self._right_edge[new_node].append(right_layers[0][0])
      right_layers.insert(0, [new_node])

    right_root_index, = right_layers[0]
    left_root_index, = left_layers[-1]

    # Set the coordinates of nodes at the left and right edges of the network.
    for left_node in self._layers[-1]:
      self._set_node_coords(left_node, (left_node, right_root_index))
    for right_id in right_config_by_id.keys():
      self._set_node_coords(right_id, (left_root_index, right_id))

    for left_layer, right_layer in zip(left_layers[1:-1], right_layers[1:-1]):
      # Create all the nodes in the new layer.
      new_layer = []
      for left_index, right_index in itertools.product(left_layer, right_layer):
        node = self._new_node()
        new_layer.append(node)
        self._set_node_coords(node, (left_index, right_index))

      # Add all outgoing edges for nodes in the preceeding layer.
      for node in self._layers[-1]:
        for outgoing_node in self._outgoing_nodes(node):
          self._graph.add_edge(node, outgoing_node)

      self._layers.append(new_layer)

    return {node: list(self._outgoing_nodes(node)) for node in self._layers[-1]}

  def _new_node(self):
    node_id = ids.new_id(self._new_node_name_prefix)
    self._graph.add_node(node_id)
    return node_id
