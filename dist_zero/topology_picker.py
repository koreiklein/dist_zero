import itertools
from collections import defaultdict

from dist_zero import errors, ids


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
    self._left_tree.fill_in_tree()

    self._right_tree = NodeTree(self._rights, max_kids=self._max_outputs)
    self._right_tree.fill_in_tree()

  def to_json(self):
    return {
        'lefts': self._lefts,
        'rights': self._rights,
        'max_outputs': self._max_outputs,
        'max_inputs': self._max_inputs,
        'name_prefix': self._name_prefix,
        'tree_coords_by_node': self._tree_coords_by_node,
        'layers': self._layers,
        'left_tree': self._left_tree.to_json(),
        'right_tree': self._right_tree.to_json(),
    }

  @staticmethod
  def from_json(j, graph):
    result = TopologyPicker(
        graph=graph,
        lefts=j['lefts'],
        rights=j['rights'],
        max_outputs=j['max_outputs'],
        max_inputs=j['max_inputs'],
        name_prefix=j['name_prefix'])
    result._tree_coords_by_node = {node: tuple(coords) for node, coords in j['tree_coords_by_node'].items()}
    result._node_by_tree_coords = {}
    for node, coords in result._tree_coords_by_node.items():
      result._node_by_tree_coords[coords] = node
    result._layers = j['layers']
    result._left_tree = NodeTree.from_json(j['left_tree'])
    result._right_tree = NodeTree.from_json(j['right_tree'])
    return result

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

  def append_right(self, node):
    '''
    Add a new right node, returning the necessary modifications to the graph to make the addition successfull.
    :return: A tuple of (node_layers, hourglasses) with the following specifications:
      node_layers -- A list of layers of nodes.  Each layer is a list of triplets (node_id, senders, receivers)
        defining a new node that should be added along with the senders and receivers it should have
      hourglasses -- A list of hourglass operations.  Each hourglass operation consists of
        (node_id, senders, receivers) where senders and receivers are currently connected via a complete
          graph which should be removed in favor of an hourglass graph centered on node_id
    '''
    new_node_layers = []
    self._rights.append(node)
    self._graph.add_node(node)
    for layer_coindex, (left_layer, right_index) in enumerate(
        zip(reversed(self._left_tree.layers), self._right_tree.append_base(node))):
      new_node_layers.append([])
      layer_index = len(self._layers) - 1 - layer_coindex
      for left_index in left_layer:
        coords = (left_index, right_index)
        if layer_index == len(self._layers) - 1:
          self._set_node_coords(node, coords)
          node_id = node
        else:
          node_id = self._new_node(coords)
        new_node_layers[-1].append(node_id)

        self._layers[layer_index].append(node_id)

    self._update_graph_edges([node for layer in new_node_layers for node in layer])
    return new_node_layers, (self._insert_hourglass_layer_left() if self._right_tree.is_full else [])

  def append_left(self, node):
    '''
    Add a new left node, returning the necessary modifications to the graph to make the addition successfull.
    :return: A tuple of (node_layers, hourglasses) with the following specifications:
      node_layers -- A list of node layers, each layer consists of triplets (node_id, senders, receivers)
        defining a new node that should be added along with the senders and receivers it should have
      hourglasses -- A list of hourglass operations.  Each hourglass operation consists of
        (node_id, senders, receivers) where senders and receivers are currently connected via a complete
          graph which should be removed in favor of an hourglass graph centered on node_id
    '''
    new_node_layers = []
    self._lefts.append(node)
    self._graph.add_node(node)
    # Modify according the results in new_node_and_parent
    for layer_index, (left_index, right_layer) \
        in enumerate(zip(self._left_tree.append_base(node), reversed(self._right_tree.layers))):
      new_node_layers.append([])
      for right_index in right_layer:
        coords = (left_index, right_index)
        if layer_index == 0:
          self._set_node_coords(node, coords)
          node_id = node
        else:
          node_id = self._new_node(coords)
        new_node_layers[-1].append(node_id)

        self._layers[layer_index].append(node_id)

    self._update_graph_edges([node for layer in new_node_layers for node in layer])

    return new_node_layers, (self._insert_hourglass_layer_right() if self._left_tree.is_full else [])

  def _insert_hourglass_layer_left(self):
    # Remove the complete graphs that are being replaced by hourglasses.
    for right_index in self._right_tree.layers[-2]:
      for left_index in self._left_tree.layers[1]:
        coords = (left_index, right_index)
        node = self._node_by_tree_coords[coords]
        for src in self._incomming_nodes(node):
          self._graph.remove_edge(src, node)

    # Insert duplicate layers at opposite ends of the left and right trees.
    right_index, = self._right_tree.insert_duplicate_layer_before_layer(self._right_tree.height - 1)
    left_indices = self._left_tree.insert_duplicate_layer_before_layer(1)

    # Create the replacement hourglass sub-graphs.
    hourglass_layer = []
    for left_index in left_indices:
      coords = (left_index, right_index)
      new_node = self._new_node(coords)
      hourglass_layer.append(new_node)

    self._layers.insert(1, hourglass_layer)
    self._update_graph_edges(hourglass_layer)
    return [(node_id, self._incomming_nodes(node_id), self._outgoing_nodes(node_id)) for node_id in hourglass_layer]

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

  def fill_in(self, new_node_ids=None):
    while self._left_tree.height < self._right_tree.height or self._left_tree.height < 2:
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
      self._layers.append([])
      for left_index in left_layer:
        for right_index in right_layer:
          if new_node_ids:
            node = new_node_ids.pop(0)
            self._graph.add_node(node)
            self._set_node_coords(node, (left_index, right_index))
          else:
            node = self._new_node((left_index, right_index))
          self._layers[-1].append(node)

    for layer in self._layers:
      for src in layer:
        for tgt in self._outgoing_nodes(src):
          self._graph.add_edge(src, tgt)


class NodeTree(object):
  def __init__(self, nodes, max_kids):
    self.layers = []

    self._base_nodes = nodes
    if max_kids <= 1:
      raise errors.InternalError("Nodes must be allowed at least 2 kids.")
    self._max_kids = max_kids
    self.parent = {}
    self.kids = defaultdict(list)

    self.index = {} # A map from node to its index in self.layers

  def to_json(self):
    return {
        'layers': self.layers,
        'base_nodes': self._base_nodes,
        'max_kids': self._max_kids,
        'parent': self.parent,
        'index': self.index,
    }

  @staticmethod
  def from_json(j):
    result = NodeTree(nodes=j['base_nodes'], max_kids=j['max_kids'])
    result.layers = j['layers']
    result.parent = j['parent']
    result.kids = defaultdict(list)
    for kid, parent in result.parent.items():
      result.kids[parent].append(kid)
    result.index = j['index']
    return result

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
    :return: A list of nodes that should be added.
      Edges of the form ``result[i]`` -> ``result[i+1]`` should also be added for each possible ``i``

    IMPORTANT: This method is allowed to break the invariant that there be empty space to add nodes
      to the tree.  When calling it, be sure to check `NodeTree.is_full` and repair the broken invariant
      if necessary.
    '''
    if len(self.layers) == 0:
      import ipdb
      ipdb.set_trace()
      raise errors.InternalError("There must be at least one layer before appending new nodes.")
    self._base_nodes.append(node)
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

  def fill_in_tree(self):
    self.layers.append([])
    for node in self._base_nodes:
      self._add_node_to_layer(0, node)
      self.parent[node] = None

    if not self.layers[-1]:
      self.layers.append([])
      next_node = self._new_node()
      self._add_node_to_layer(-1, next_node)

    while len(self.layers[-1]) > 1:
      self.layers.append([])
      for i in range(0, len(self.layers[-2]), self._max_kids):
        next_node = self._new_node()
        self._add_node_to_layer(-1, next_node)
        for node in self.layers[-2][i:i + self._max_kids]:
          self._set_parent(node, next_node)

    if self.height == 0 or len(self.kids[self.root]) == self._max_kids:
      self.bump_height()
