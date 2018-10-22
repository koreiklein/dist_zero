import random
from collections import defaultdict

from dist_zero import errors, topology_picker, network_graph, ids


def weighted_rr(kids, parents, weights):
  '''
  Perform a weighted round robin assignment.
  Kids will be assigned to parents in proportions that correspond roughtly to the weights of the parents.
  No parent may be assigned more kids than its weight.
  If the total weight for all parents is not enough to assign every kid to a parent,
  raise a `NoRemainingAvailability` error.

  :param list kids: A list
  :param list parents: A list
  :param dict weights: A dictionary mapping each parent to a non-negative number

  :return: A map assigning each kid to a parent.
  :rtype: dict
  '''
  weights = dict(weights)
  assignment = {}

  while kids:
    if not parents:
      raise errors.NoRemainingAvailability()

    partition = [0]
    for parent in parents:
      partition.append(partition[-1] + weights[parent])
    partition = partition[1:]

    increment = sum(weights[parent] for parent in parents) / len(kids)

    unmatched_kids = []
    counter = 0
    index = 0
    for kid in kids:
      while index < len(partition) and counter >= partition[index]:
        index += 1

      if index >= len(partition):
        unmatched_kids.append(kid)
      else:
        parent = parents[index]
        if weights[parent] <= 0:
          unmatched_kids.append(kid)
        else:
          assignment[kid] = parent
          weights[parent] -= 1

      counter += increment

    kids = unmatched_kids
    parents = [parent for parent in parents if weights[parent] > 0]

  return assignment


def from_json(j, *args, **kwargs):
  if j['type'] == 'all_to_all_connector':
    return AllToAllConnector.from_json(j, *args, **kwargs)
  elif j['type'] == 'all_to_one_available_connector':
    return AllToOneAvailableConnector.from_json(j, *args, **kwargs)
  else:
    raise errors.InternalError(f"Unrecognized connector type {j['type']}.")


def new_connector(connector_type, left_configurations, right_configurations, computation_node):
  if connector_type['type'] == 'all_to_all_connector':
    return AllToAllConnector(
        height=computation_node.height,
        left_configurations=left_configurations,
        right_configurations=right_configurations,
        left_is_data=computation_node.left_is_data,
        right_is_data=computation_node.right_is_data,
        max_outputs=computation_node.system_config['SUM_NODE_RECEIVER_LIMIT'],
        max_inputs=computation_node.system_config['SUM_NODE_SENDER_LIMIT'],
    )
  elif connector_type['type'] == 'all_to_one_available_connector':
    # FIXME(KK): Add the proper parameters here
    return AllToOneAvailableConnector(
        height=computation_node.height,
        left_configurations=left_configurations,
        right_configurations=right_configurations,
        left_is_data=computation_node.left_is_data,
        right_is_data=computation_node.right_is_data,
        max_outputs=computation_node.system_config['SUM_NODE_RECEIVER_LIMIT'],
        max_inputs=computation_node.system_config['SUM_NODE_SENDER_LIMIT'],
    )
  else:
    raise errors.InternalError(f'Unrecognized connector type "{connector_type["type"]}"')


class AllToOneAvailableConnector(object):
  def __init__(self, height, left_configurations, left_is_data, right_configurations, right_is_data, max_outputs,
               max_inputs):
    self._height = height
    self._left_configurations = left_configurations
    self._right_configurations = right_configurations
    self._left_is_data = left_is_data
    self._right_is_data = right_is_data

  def fill_in(self, new_node_ids=None):
    # Algorithm:
    #   Round robin allocate the kids of the left nodes to the right_configuration weighted by availability.
    if new_node_ids is not None:
      # FIXME(KK): Figure out whether this case occurs, and if so, what to do about it.
      import ipdb
      ipdb.set_trace()
      raise RuntimeError("Not Yet Implemented")

    self._left_layer = {
        kid['handle']['id']: kid['handle']
        for left_config in self._left_configurations.values() for kid in left_config['kids']
    }

    assignment = weighted_rr(
        kids=list(self._left_layer.keys()),
        parents=list(self._right_configurations.keys()),
        weights={
            right_id: right_config['availability']
            for right_id, right_config in self._right_configurations.items()
        },
    )

    single_layer = []
    self._right_to_parent_ids = {}
    self._graph = network_graph.NetworkGraph()
    for kid, parent_id in assignment.items():
      node_id = ids.new_id("ComputationNode_to_one_available_forwarder")
      self._graph.add_node(kid)
      self._graph.add_node(node_id)
      self._graph.add_edge(kid, node_id)
      single_layer.append(node_id)
      self._right_to_parent_ids[node_id] = [parent_id]

    self.layers = [list(self._left_layer.keys()), single_layer]

    inverse_assignment = defaultdict(list)
    for kid, parent in assignment.items():
      inverse_assignment[parent].append(kid)

  @property
  def graph(self):
    return self._graph

  @property
  def right_to_parent_ids(self):
    return self._right_to_parent_ids

  @staticmethod
  def from_json(j, height, left_configurations, left_is_data, right_configurations, right_is_data, max_outputs,
                max_inputs):
    return AllToOneAvailableConnector()

  def max_height(self):
    return max(self.max_left_height(), self.max_right_height())

  def max_left_height(self):
    return max((config['height'] for config in self._left_configurations.values()), default=-1)

  def max_right_height(self):
    return max((config['height'] for config in self._right_configurations.values()), default=-1)


class AllToAllConnector(object):
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
        for receiver_id in self._graph.node_receivers(node_id):
          graph.add_node(receiver_id)
          graph.add_edge(node_id, receiver_id)

    return {
        'type': 'all_to_all_connector',
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
      graph.add_edge(node_id, receiver_id)

    return {
        'type': 'all_to_all_connector',
        'layers': self._layers[:1],
        'right_to_parent_ids': {node_id: [parent_id]
                                for node_id in self._layers[1]},
        'left_node_to_picker_left_node': self._left_node_to_picker_left_node,
        'graph': graph.to_json(),
        'picker': self._picker.to_json(),
    }

  @staticmethod
  def from_json(j, height, left_configurations, left_is_data, right_configurations, right_is_data, max_outputs,
                max_inputs):
    connector = AllToAllConnector(
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

  def set_right_parent_ids(self, kid_ids, parent_ids):
    for kid_id in kid_ids:
      self._right_to_parent_ids[kid_id] = parent_ids

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
    right_parent_ids = []
    for right_configuration in right_configurations:
      right_parent_id = right_configuration['parent_handle']['id']
      right_parent_ids.append(right_parent_id)
      self._right_configurations[right_parent_id] = right_configuration

    right_nodes = list(self._right_to_parent_ids.keys())
    right_nodes.sort(key=lambda node: len(self._right_to_parent_ids[node]))
    first = right_nodes[0]
    if len(self._right_to_parent_ids[first]) + len(right_configurations) > 3:
      # FIXME(KK): Implement this
      raise errors.InternalError("Not Yet Implemented: assign multiple parents to the same child right node")

    self._right_to_parent_ids[first].extend(right_parent_ids)
    return right_parent_ids

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
    new_layers = self._picker.append_left(node_ids[0])
    for node_id in node_ids[1:]:
      more_new_layers = self._picker.append_left(node_id)
      if len(new_layers) != len(more_new_layers):
        raise errors.InternalError("Parallel calls to append_left must return layer lists of equal length")
      for new_layer, more_new_layer in zip(new_layers, more_new_layers):
        new_layer.extend(more_new_layer)

    if len(new_layers) == len(self._picker.layers):
      raise errors.InternalError("The new layers should never be as long all the picker's layers.")

    last_edges = [(node_id, receiver_id) for node_id in new_layers[-1]
                  for receiver_id in self._graph.node_receivers(node_id)]
    return new_layers, last_edges, (self._picker.insert_hourglass_layer_right()
                                    if self._picker.needs_right_hourglass else [])

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

  def _initialize_picker_lefts(self, new_node_ids=None):
    '''
    Called once at startup to initialize self._left_node_to_picker_left_node and return the
    list of left ids to be used by the picker.
    '''
    if self._left_is_data:
      self._layers.append([])
      self._left_node_to_picker_left_node = {}
      for left in self._lefts:
        self._layers[0].append(left['id'])
        adjacent_id = ids.new_id("{}_adjacent".format(
            self._name_prefix)) if new_node_ids is None else new_node_ids.pop(0)
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

  def _initialize_picker_rights(self, new_node_ids=None):
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
          adjacent_id = ids.new_id("{}_adjacent".format(
              self._name_prefix)) if new_node_ids is None else new_node_ids.pop(0)
          self._right_to_parent_ids[adjacent_id].append(right_config['parent_handle']['id'])
    else:
      for right_config in self._right_configurations.values():
        right_id = ids.new_id("{}_rightmost".format(self._name_prefix)) if new_node_ids is None else new_node_ids.pop(0)
        self._right_to_parent_ids[right_id].append(right_config['parent_handle']['id'])

    return list(self._right_to_parent_ids.keys())

  def fill_in(self, new_node_ids=None):
    picker_lefts = self._initialize_picker_lefts(new_node_ids)
    picker_rights = self._initialize_picker_rights(new_node_ids)

    self._picker = topology_picker.TopologyPicker(
        graph=self._graph,
        lefts=picker_lefts,
        rights=picker_rights,
        max_outputs=self._max_outputs,
        max_inputs=self._max_inputs,
        name_prefix=self._name_prefix)
    self._picker.fill_in(new_node_ids=new_node_ids)

    if new_node_ids is not None and new_node_ids:
      raise errors.InternalError("New all new_node_ids were used up.")

    self._layers.extend([list(x) for x in self._picker.layers])
