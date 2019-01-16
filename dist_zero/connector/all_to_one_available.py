import random
from collections import defaultdict

from dist_zero import errors, network_graph, ids
from .connector import Connector


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

  orig_kids = kids
  orig_parents = parents
  orig_weights = dict(weights)

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


class AllToOneAvailableConnector(Connector):
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

    self._layers = []

  def _initialize_picker_lefts(self):
    self._layers.append(list(self._left_layer.keys()))
    for left_id in self._layers[-1]:
      self._graph.add_node(left_id)

    if self._left_is_data:
      if self.max_left_height() >= self.max_right_height():
        adjacents = []
        for left_id in self._layers[-1]:
          adjacent_id = ids.new_id("LinkNode_to_one_available_left_adjacent")
          adjacents.append(adjacent_id)
          self._graph.add_node(left_id)
          self._graph.add_node(adjacent_id)
          self._graph.add_edge(left_id, adjacent_id)
        self._layers.append(adjacents)
        return adjacents
      else:
        gap_node_id = ids.new_id("LinkNode_to_one_available_left_gap_child")
        self._layers.append([gap_node_id])
        self._graph.add_node(gap_node_id)
        return [gap_node_id]
    else:
      return self._layers[-1]

  def _initialize_picker_rights(self):
    self._right_to_parent_ids = {}
    if self._right_is_data:
      if self.max_left_height() <= self.max_right_height():
        result = []
        weights = {}
        for parent_id, right_config in self._right_configurations.items():
          availability = right_config['availability']
          adjacents = []
          for i in range(right_config['n_kids']):
            per_kid = availability // right_config['n_kids']
            adjacent_id = ids.new_id(f"LinkNode_to_one_available_right_adjacent_{i}")
            adjacents.append(adjacent_id)
            self._right_to_parent_ids[adjacent_id] = [parent_id]
            self._graph.add_node(adjacent_id)
            weights[adjacent_id] = per_kid
            availability -= per_kid
          for x, adjacent_id in zip(range(availability), adjacents):
            weights[adjacent_id] += 1
          result.extend(adjacents)
        return result, weights
      else:
        gap_node_id = ids.new_id("LinkNode_to_one_available_right_gap_child")
        self._graph.add_node(gap_node_id)
        if len(self._right_configurations) != 1:
          raise errors.InternalError("There must be a unique right gap parent when spawning a gap child.")
        right_parent_id, = self._right_configurations.keys()
        self._right_to_parent_ids[gap_node_id] = [right_parent_id]
        return [gap_node_id], {gap_node_id: 1000000000} # Give it effectively infinite availability
    else:
      self._right_to_parent_ids = {}
      if len(self._right_configurations) == 0:
        return [], {}
      elif len(self._right_configurations) == 1:
        right_parent_id, = self._right_configurations.keys()
        for left_id in self._layers[-1]:
          self._right_to_parent_ids[left_id] = [right_parent_id]
        return [], {}
      else:
        raise errors.InternalError(
            "AllToOneAvailableConnector should not ever be initialized with more than one right_configuration.")

  @property
  def layers(self):
    return self._layers

  def fill_in(self, new_node_ids=None):
    # Algorithm:
    #   Round robin allocate the kids of the left nodes to the right_configuration weighted by availability.
    if new_node_ids is not None:
      # FIXME(KK): Figure out whether this case occurs, and if so, what to do about it.
      raise RuntimeError("Not Yet Implemented")

    self._graph = network_graph.NetworkGraph()
    # FIXME(KK): The below seems to be totally broken.  Fix it up or rewrite it.
    picker_lefts = self._initialize_picker_lefts()
    picker_rights, weights = self._initialize_picker_rights()

    if picker_rights:
      assignment = weighted_rr(
          kids=picker_lefts,
          parents=picker_rights,
          weights=weights,
      )

      for kid_id, parent_id in assignment.items():
        self._graph.add_edge(kid_id, parent_id)

      self._layers.append(picker_rights)

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
