from collections import defaultdict

from dist_zero import errors, topology_picker, network_graph, ids


class Connector(object):
  def __init__(self, height, left_configurations, left_is_data, right_configurations, right_is_data, max_outputs,
               max_inputs):
    self._height = height
    self._left_configurations = left_configurations
    self._right_configurations = right_configurations
    self._left_is_data = left_is_data
    self._right_is_data = right_is_data

    self._left_layer = {
        kid['handle']['id']: kid['handle']
        for left_config in self._left_configurations.values() for kid in left_config['kids']
    }

    self._max_outputs = max_outputs
    self._max_inputs = max_inputs

    self._name_prefix = 'ComputationNode' if self._height > 0 else 'SumNode'

    # If left is not data, the left nodes are identically the picker left nodes,
    # and this variable must be `None`.
    self._left_node_to_picker_left_node = None

    self._lefts = [kid['handle'] for left_config in self._left_configurations.values() for kid in left_config['kids']]

    self._rights = None
    self._picker = None

    self._layers = []
    self._right_to_parent_ids = None
    '''
    Maps the id of each of the rightmost nodes managed by the connector to the list of nodes to the right
    of the connector responsible for it.
    '''
    self._graph = network_graph.NetworkGraph()

  def non_left_part_json(self):
    if not self._left_is_data:
      raise errors.InternalError("Can only take the non-left part of a Connector instance when left_is_data.")

    graph = network_graph.NetworkGraph()
    for layer in self._layers[1:]:
      for node_id in layer:
        graph.add_node(node_id)
        for receiver_id in graph.node_receivers(node_id):
          graph.add_node(receiver_id)
          graph.add_edge(node_id, receiver_id)

    return {
        'layers': self._layers[1:],
        'right_to_parent_ids': self._right_to_parent_ids,
        'left_node_to_picker_left_node': None,
        'graph': graph.to_json(),
        'picker': self._picker.to_json(),
    }

  def left_part_json(self, parent_id):
    if not self._left_is_data:
      raise errors.InternalError("Can only take the left part of a Connector instance when left_is_data.")

    graph = network_graph.NetworkGraph()
    for node_id in self._layers[0]:
      receiver_id, = self._graph.node_receivers(node_id)
      graph.add_node(node_id)
      graph.add_node(receiver_id)
      graph.add_edge(receiver_id, node_id)

    return {
        'layers': self._layers[:1],
        'right_to_parent_ids': {node_id: [parent_id]
                                for node_id in self._layers[0]},
        'left_node_to_picker_left_node': self._left_node_to_picker_left_node,
        'graph': graph.to_json(),
        'picker': self._picker.to_json(),
    }

  @staticmethod
  def from_json(j, height, left_configurations, left_is_data, right_configurations, right_is_data, max_outputs,
                max_inputs):
    connector = Connector(
        height=height,
        left_configurations=left_configurations,
        left_is_data=left_is_data,
        right_configurations=right_configurations,
        right_is_data=right_is_data,
        max_outputs=max_outputs,
        max_inputs=max_inputs)
    connector._layers = j['layers']
    connector._right_to_parent_ids = defaultdict(list)
    for right, parent_ids in j['right_to_parent_ids'].items():
      connector._right_to_parent_ids[right].extend(parent_ids)
    connector._left_node_to_picker_left_node = j['left_node_to_picker_left_node']
    connector._graph = network_graph.NetworkGraph.from_json(j['graph'])
    connector._picker = topology_picker.TopologyPicker.from_json(j['picker'], graph=connector._graph)

    return connector

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

  def add_kids_to_right_configuration(self, parent_id_kid_pairs):
    '''
    Update the set of connections so that there is a new node in
    the rightmost layer.

    :param list[tuple] parent_id_kid_pairs: A list of pairs (parent_id, kid)
      where parent_id is the id of the parent sending a right_configuration,
      and kid is a :ref:`handle` now managed by that right parent.

    :return: A triplet (new_node_ids, new_edges, hourglasses)
      where new_node_ids are ids of nodes to spawn,
      new_edges are pairs (src_node_id, tgt_node_id) defining the rightmost connections
      not defined by new_nodes, and hourglasses are triplets
      (node_id, senders, receivers) giving the hourglass substitutions.
    '''
    if not self._right_is_data:
      raise errors.InternalError("Not possible, add_kids_to_right_configuration when the right is not a data node.")
    new_adjacent_ids = []
    last_edges = []
    for parent_id, kid in parent_id_kid_pairs:
      right_config = self._right_configurations[parent_id]
      right_config['n_kids'] += 1
      if 'known_kids' not in right_config:
        right_config['known_kids'] = [kid]
      else:
        right_config['known_kids'].append(kid)

      adjacent_id = ids.new_id("{}_right_adjacent".format(self._name_prefix))
      last_edges.append((adjacent_id, kid['id']))
      self._right_to_parent_ids[adjacent_id].append(parent_id)
      new_adjacent_ids.append(adjacent_id)

    if len(new_adjacent_ids) == 0:
      raise errors.InternalError("There must be at least one new node being added.")

    self._layers[-1].extend(new_adjacent_ids)
    new_layers, hourglasses = self._picker_append_all_right(new_adjacent_ids)
    return new_layers, last_edges, hourglasses

  def add_kids_to_left_configuration(self, parent_id_kid_pairs):
    '''
    Update the set of connections so that there is a new node in
    the leftmost layer.

    :param list[tuple] parent_id_kid_pairs: A list of pairs (parent_id, kid)
      where parent_id is the id of the parent sending a left_configuration,
      and kid is a kid configuration to add to that parent's left_configuration.

    :return: A triplet (new_nodes, new_edges, hourglasses)
      where new_nodes are triplets (node_id, senders, receivers) to spawn
      new_edges are pairs (src_node_id, tgt_node_id) defining the rightmost connections
      not defined by new_nodes, and hourglasses are triplets
      (node_id, senders, receivers) giving the hourglass substitutions.
    '''
    node_ids = []
    for parent_id, kid in parent_id_kid_pairs:
      left_config = self._left_configurations[parent_id]
      left_config['kids'].append(kid)
      node = kid['handle']
      self._left_layer[node['id']] = node
      node_ids.append(node['id'])

    return self._add_left_kids(node_ids)

  def add_left_configurations(self, left_configurations):
    node_ids = []
    for left_configuration in left_configurations:
      self._left_configurations[left_configuration['node']['id']] = left_configuration
      for kid_config in left_configuration['kids']:
        kid = kid_config['handle']
        self._left_layer[kid['id']] = kid
        node_ids.append(kid['id'])

    return self._add_left_kids(node_ids)

  def add_right_configurations(self, right_configurations):
    for right_configuration in right_configurations:
      self._right_configurations[right_configuration['node']['id']] = right_configuration

  def _picker_append_all_right(self, node_ids):
    new_layers, hourglasses = self._picker.append_right(node_ids[0])
    for node_id in node_ids[1:]:
      more_new_layers, more_hourglasses = self._picker.append_right(node_id)
      hourglasses.extend(more_hourglasses)
      if len(new_layers) != len(more_new_layers):
        raise errors.InternalError("Parallel calls to append_right must return layer lists of equal length")
      for new_layer, more_new_layer in zip(new_layers, more_new_layers):
        new_layer.extend(more_new_layer)

    return new_layers, hourglasses

  def _picker_append_all_left(self, node_ids):
    new_layers, hourglasses = self._picker.append_left(node_ids[0])
    for node_id in node_ids[1:]:
      more_new_layers, more_hourglasses = self._picker.append_left(node_id)
      hourglasses.extend(more_hourglasses)
      if len(new_layers) != len(more_new_layers):
        raise errors.InternalError("Parallel calls to append_left must return layer lists of equal length")
      for new_layer, more_new_layer in zip(new_layers, more_new_layers):
        new_layer.extend(more_new_layer)

    if len(new_layers) == len(self._picker.layers):
      # They go all the way through
      return new_layers, None, hourglasses
    else:
      last_edges = [(node_id, receiver_id) for node_id in new_layers[-1]
                    for receiver_id in self._graph.node_receivers(node_id)]
      return new_layers, last_edges, hourglasses

  def _add_left_kids(self, node_ids):
    if len(node_ids) == 0:
      return [], [], []

    self._layers[0].extend(node_ids)

    if self._left_is_data:
      adjacent_ids = []
      for node_id in node_ids:
        adjacent_id = ids.new_id("{}_adjacent".format(self._name_prefix))
        self._layers[1].append(adjacent_id)
        self._graph.add_node(node_id)
        self._graph.add_node(adjacent_id)
        self._graph.add_edge(node_id, adjacent_id)
        adjacent_ids.append(adjacent_id)

      new_layers, last_edges, hourglasses = self._picker_append_all_left(adjacent_ids)

      return new_layers, last_edges, hourglasses
    else:
      new_layers, last_edges, hourglasses = self._picker_append_all_left(node_ids)
      return new_layers[1:], last_edges, hourglasses

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

  def fill_in(self, new_node_ids=None):
    picker_lefts = self._initialize_picker_lefts()
    picker_rights = self._initialize_picker_rights()

    self._picker = topology_picker.TopologyPicker(
        graph=self._graph,
        lefts=picker_lefts,
        rights=picker_rights,
        max_outputs=self._max_outputs,
        max_inputs=self._max_inputs,
        name_prefix=self._name_prefix)
    self._picker.fill_in(new_node_ids=new_node_ids)

    self._layers.extend([list(x) for x in self._picker.layers])
