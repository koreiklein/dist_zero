import itertools
from collections import defaultdict

from dist_zero import errors, ids


class NodeTree(object):
  def __init__(self, nodes, max_kids):
    self.layers = []

    self._base_nodes = nodes
    self._max_kids = max_kids
    self.parent = {}
    self.kids = defaultdict(list)

    self.index = {} # A map from node to its index in self.layers

    self._fill_in_tree()

  @property
  def height(self):
    return len(self.layers)

  @property
  def root(self):
    result, = self.layers[-1]
    return result

  @property
  def is_full(self):
    '''
    True iff there is no more room in the tree to fit new nodes.
    '''
    return len(self._base_nodes) >= self._max_kids**(self.height - 1)

  def _new_node(self):
    result = str(len(self.parent))
    self.parent[result] = None
    return result

  def _set_parent(self, node, parent):
    self.parent[node] = parent
    self.kids[parent].append(node)

  def bump_height(self):
    new_root = self._new_node()
    self.layers.append([new_root])
    for node in self.layers[-2]:
      self._set_parent(node, new_root)

  def append_base(self, node):
    '''
    Add a new node, to the base nodes of this tree,
    returning the necessary modifications to the tree to make the addition successfull.
    :return: A list of pairs (node_id, parent) defining a new node that should be added
        along with its parent.

    IMPORTANT: This method is allowed to break the invariant that there be empty space to add nodes
      to the tree.  When calling it, be sure to check `NodeTree.is_full` and repair the broken invariant
      if necessary.
    '''
    if len(self.layers) == 0:
      raise errors.InternalError("There must be at least one layer before appending new nodes.")
    results = []
    layer_index = 0
    best = None
    while True:
      # Invariants:
      #   ``node`` has not yet been put in the tree, but may have kids in layer_index - 1
      #   ``self.parent.get(node, None) is None``.
      #   ``node`` has not been added to a layer, but will be added later on.
      #   All nodes in layers < layer_index have proper parents/kids and meet the constraints.
      self._add_node_to_layer(layer_index, node)

      layer_index += 1
      self.layers[layer_index].sort(key=lambda parent: len(self.kids[parent]))
      best = self.layers[layer_index][0]
      if len(self.kids[best]) < self._max_kids:
        self._set_parent(node, best)
        results.append(node)
        break
      else:
        # All nodes in layer_index have the maximum number of kids
        if layer_index + 1 == len(self.layers):
          raise errors.InternalError("Impossible! The root node should never have the maximum number of kids.")
        new_parent = self._new_node()
        self._set_parent(node, new_parent)
        results.append(node)
        node = new_parent
        continue

    # The node was fit into the tree, given the parent ``best`` at ``layer_index``
    return results

  def insert_duplicate_layer_before_layer(self, layer_index):
    '''
    Insert a duplicate layer before the layer at ``layer_index``
    '''
    new_layer = []
    for node in self.layers[layer_index]:
      new_node = self._new_node()
      for kid in self.kids[node]:
        self._set_parent(kid, new_node)
      self.kids[node] = []
      self._set_parent(new_node, node)
      new_layer.append(new_node)

    self.layers.insert(layer_index, new_layer)
    # Update the values in self.index, as many of them are now out of date.
    for i in range(layer_index, len(self.layers)):
      for node in self.layers[i]:
        self.index[node] = i

    return new_layer

  def _add_node_to_layer(self, layer_index, node):
    while layer_index < 0:
      layer_index += len(self.layers)
    self.layers[layer_index].append(node)
    self.index[node] = layer_index

  def _fill_in_tree(self):
    self.layers.append([])
    for node in self._base_nodes:
      self._add_node_to_layer(0, node)
      self.parent[node] = None
    while len(self.layers[-1]) > 1:
      self.layers.append([])
      for i in range(0, len(self.layers[-2]), self._max_kids):
        next_node = self._new_node()
        self._add_node_to_layer(-1, next_node)
        for node in self.layers[-2][i:i + self._max_kids]:
          self._set_parent(node, next_node)

    if self.height == 0 or len(self.kids[self.root]) == self._max_kids:
      self.bump_height()


class TopologyPicker(object):
  def __init__(self, graph, lefts, rights, max_outputs, max_inputs, name_prefix):
    self._graph = graph
    self._lefts = lefts
    self._rights = rights
    self._max_outputs = max_outputs
    self._max_inputs = max_inputs
    self._name_prefix = name_prefix

    self._layers = []

    self._node_by_tree_coords = {}
    self._tree_coords_by_node = {}

    # Invariants:
    #   ``self._left_tree.is_full == False``
    #   ``self._right_tree.is_full == False``
    self._left_tree = NodeTree(self._lefts, max_kids=self._max_inputs)
    self._right_tree = NodeTree(self._rights, max_kids=self._max_outputs)

    self._fill_in()

  @property
  def layers(self):
    return self._layers

  @property
  def graph(self):
    return self._graph

  @property
  def lefts(self):
    return self._lefts

  @property
  def rights(self):
    return self._rights

  def render_network(self, filename='topology', view=False):
    from graphviz import Digraph

    dot = Digraph(comment='Network Topology')
    for height, layer in enumerate(self.layers):
      subgraph = Digraph('Layer {}'.format(height), graph_attr={'label': 'height {}'.format(height)})
      for node in layer:
        if height == 0 or height + 1 == len(self._layers):
          kwargs = {'shape': 'ellipse', 'color': 'black', 'fillcolor': '#c7faff', 'style': 'filled'}
        else:
          kwargs = {'shape': 'diamond', 'color': 'black'}
        subgraph.node(node, **kwargs)
        for tgt in self._outgoing_nodes(node):
          subgraph.edge(node, tgt)
      dot.subgraph(subgraph)
    dot.render(filename, view=view, cleanup=True)

  def _update_graph_edges(self, nodes):
    '''
    Add all the edges for all ``nodes`` to self._graph.
    '''
    for node in nodes:
      for tgt in self._outgoing_nodes(node):
        self._graph.add_edge(node, tgt)

      for src in self._incomming_nodes(node):
        self._graph.add_edge(src, node)

  def append_left(self, node):
    '''
    Add a new left node, returning the necessary modifications to the graph to make the addition successfull.
    :return: A tuple of (nodes, hourglasses) with the following specifications:
      nodes -- A list of triplets (node_id, senders, receivers) defining a new node that should be added
        along with the senders and receivers it should have
      hourglasses -- A list of hourglass operations.  Each hourglass operation consists of
        (node_id, senders, receivers) where senders and receivers are currently connected via a complete
          graph which should be removed in favor of an hourglass graph centered on node_id
    '''
    new_nodes = []
    self._lefts.append(node)
    self._graph.add_node(node)
    # Modify according the results in new_node_and_parent
    for layer_index, (left_index, right_layer) \
        in enumerate(zip(self._left_tree.append_base(node), reversed(self._right_tree.layers))):
      for right_index in right_layer:
        coords = (left_index, right_index)
        if layer_index == 0:
          self._set_node_coords(node, coords)
          node_id = node
        else:
          node_id = self._new_node(coords)
        new_nodes.append(node_id)

        self._layers[layer_index].append(node_id)

    self._update_graph_edges(new_nodes)

    result = [(node_id, self._incomming_nodes(node_id), self._outgoing_nodes(node_id)) for node_id in new_nodes]

    return result, (self._insert_hourglass_layer_right() if self._left_tree.is_full else [])

  def _insert_hourglass_layer_right(self):
    # Remove the complete graphs that are being replaced by hourglasses.
    for left_index in self._left_tree.layers[-2]:
      for right_index in self._right_tree.layers[1]:
        coords = (left_index, right_index)
        node = self._node_by_tree_coords[coords]
        for tgt in self._outgoing_nodes(node):
          self._graph.remove_edge(node, tgt)

    # Insert duplicate layers at opposite ends of the left and right trees.
    left_index, = self._left_tree.insert_duplicate_layer_before_layer(self._left_tree.height - 1)
    right_indices = self._right_tree.insert_duplicate_layer_before_layer(1)

    # Create the replacement hourglass sub-graphs.
    hourglass_layer = []
    for right_index in right_indices:
      coords = (left_index, right_index)
      new_node = self._new_node(coords)
      hourglass_layer.append(new_node)

    self._layers.insert(len(self._layers) - 1, hourglass_layer)
    self._update_graph_edges(hourglass_layer)

    return [(node_id, self._incomming_nodes(node_id), self._outgoing_nodes(node_id)) for node_id in hourglass_layer]

  def _set_node_coords(self, node, coords):
    self._node_by_tree_coords[coords] = node
    self._tree_coords_by_node[node] = coords

  def _new_node(self, coords):
    if coords in self._node_by_tree_coords:
      return self._node_by_tree_coords[coords]
    else:
      node = ids.new_id(self._name_prefix)
      self._graph.add_node(node)
      self._set_node_coords(node, coords)
      return node

  def _outgoing_nodes(self, src):
    src_left, src_right = self._tree_coords_by_node[src]
    tgt_left = self._left_tree.parent[src_left]
    return [self._node_by_tree_coords[(tgt_left, tgt_right)] for tgt_right in self._right_tree.kids[src_right]]

  def _incomming_nodes(self, tgt):
    tgt_left, tgt_right = self._tree_coords_by_node[tgt]
    src_right = self._right_tree.parent[tgt_right]
    return [self._node_by_tree_coords[(src_left, src_right)] for src_left in self._left_tree.kids[tgt_left]]

  def _fill_in(self):
    while self._left_tree.height < self._right_tree.height or self._left_tree.height < 3:
      self._left_tree.bump_height()
    while self._right_tree.height < self._left_tree.height:
      self._right_tree.bump_height()

    for node in self._lefts:
      self._graph.add_node(node)
      self._set_node_coords(node, (node, self._right_tree.root))
    for node in self._rights:
      self._graph.add_node(node)
      self._set_node_coords(node, (self._left_tree.root, node))

    for left_layer, right_layer in zip(self._left_tree.layers, reversed(self._right_tree.layers)):
      self._layers.append(
          [self._new_node((left_index, right_index)) for left_index in left_layer for right_index in right_layer])

    for layer in self._layers:
      for src in layer:
        for tgt in self._outgoing_nodes(src):
          self._graph.add_edge(src, tgt)


# FIXME(KK): Remove this class.
class OldTopologyPicker(object):
  '''
  Each instance of `TopologyPicker` will take some constraints about nodes in a network topology,
  and determine a new set of nodes and their connections in such a way that it meets the constraints.
  '''

  def __init__(self, graph, left_is_data, right_is_data, new_node_max_outputs, new_node_max_inputs,
               new_node_name_prefix):
    self._graph = graph
    self._left_layer = [node for node in graph.nodes()]
    self._layers = [self._left_layer]

    self.left_is_data = left_is_data
    self.right_is_data = right_is_data

    self._cur_left_tree_index = 0
    self._cur_right_tree_index = 0

    self._new_node_max_outputs = new_node_max_outputs
    self._new_node_max_inputs = new_node_max_inputs

    self._new_node_name_prefix = new_node_name_prefix

    self._node_by_tree_coords = {}
    self._tree_coords_by_node = {}

    self._right_edge = None
    self._left_edge = None

    self._left_layers = None
    self._right_layers = None

  @property
  def _right_root_index(self):
    return self._right_layers[0][0]

  @property
  def _left_root_index(self):
    return self._left_layers[-1][0]

  def complete_receivers_when_left_is_data(self, left_node_id, node_id, random):
    if not self.left_is_data:
      raise errors.InternalError("Left side must be a data side")

    self._graph.add_node(left_node_id)
    self._graph.add_node(node_id)
    self._layers[0].append(left_node_id)
    if len(self._layers) < 2:
      self._layers.append([])

    self._layers[1].append(node_id)
    self._graph.add_edge(left_node_id, node_id)

    if self._left_layers is None:
      return None
    self._left_layers[0].append(node_id)

    self._set_node_coords(node_id, (node_id, self._right_root_index))

    left_index = random.choice(self._left_layers[1])
    self._left_edge[node_id] = left_index
    receivers = [self._node_by_tree_coords[(left_index, right_index)] for right_index in self._right_layers[1]]
    for receiver in receivers:
      self._graph.add_edge(node_id, receiver)
    return receivers

  def complete_receivers(self, node_id, random):
    '''
    Add a new node to the left layer
    :return: A list of node ids that disjointly send to all outputs
    or `None` if no such list exists
    '''
    if self.left_is_data:
      raise errors.InternalError("Left side must not be a data side.")
    self._graph.add_node(node_id)
    self._set_node_coords(node_id, (node_id, self._right_root_index))

    self._left_layers[0].append(node_id)
    left_index = random.choice(self._left_layers[1])
    self._left_edge[node_id] = left_index
    receivers = [self._node_by_tree_coords[(left_index, right_index)] for right_index in self._right_layers[1]]
    for receiver in receivers:
      self._graph.add_edge(node_id, receiver)
    return receivers

  @property
  def graph(self):
    return self._graph

  @property
  def lefts(self):
    return self._lefts

  @property
  def rights(self):
    return self._rights

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

  def _form_right_tree_from_right_configurations(self, right_config_by_id, n_left_layers):
    self._right_edge = defaultdict(list)
    current_tree_layer = list(right_config_by_id.keys())

    layers = [current_tree_layer]

    adjacent_layer = []
    for node in current_tree_layer:
      if self.right_is_data:
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

  def fill_graph(self, right_configurations):
    if self.left_is_data:
      self._add_left_adjacents_layer()

    right_config_by_id = {right_config['parent_handle']['id']: right_config for right_config in right_configurations}

    # Create a tree of left coordinates and of right coordinates
    left_layers = self._form_left_tree_from_nodes(self._layers[-1])
    self._left_layers = left_layers
    right_layers = self._form_right_tree_from_right_configurations(right_config_by_id, len(left_layers))
    self._right_layers = right_layers

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
