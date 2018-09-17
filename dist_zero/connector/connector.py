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
    self._left_layer[kid['id']] = kid
    node_id = kid['id']

    return self._add_left_kid(node_id)

  def add_left_configuration(self, left_configuration):
    self._left_configurations[left_configuration['node']['id']] = left_configuration

    if len(left_configuration['kids']) != 1:
      import ipdb
      ipdb.set_trace()
      raise errors.InternalError("Not Yet Implemented")

    for kid_config in left_configuration['kids']:
      kid = kid_config['handle']
      self._left_layer[kid['id']] = kid
      node_id = kid['id']
      return self._add_left_kid(node_id)

  def _add_left_kid(self, node_id):

    self._layers[0].append(node_id)

    if self._left_is_data:
      adjacent_id = ids.new_id("{}_adjacent".format(self._name_prefix))
      self._layers[1].append(adjacent_id)
      self._graph.add_node(node_id)
      new_layers, hourglasses = self._picker.append_left(adjacent_id)
      self._graph.add_edge(node_id, adjacent_id)
      return new_layers, [], hourglasses
    else:
      new_layers, hourglasses = self._picker.append_left(node_id)
      if not new_layers:
        import ipdb
        ipdb.set_trace()
        # FIXME(KK): Implement this properly
      else:
        return new_layers[1:], [(node_id, receiver_id)
                                for receiver_id in self._graph.node_receivers(node_id)], hourglasses

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

    self._layers.extend([list(x) for x in self._picker.layers])
