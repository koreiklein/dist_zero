'''
Tests the `TopologyPicker` class.
'''

import pytest

from dist_zero import ids, messages
from dist_zero.network_graph import NetworkGraph
from dist_zero.migration.topology_picker import TopologyPicker


def test_many_right_configs():
  N_LEFT = 1
  graph = NetworkGraph()
  left_nodes = [graph.add_node(ids.new_id('Left_Node')) for i in range(N_LEFT)]
  picker = TopologyPicker(
      graph=graph,
      max_outputs={node: 5
                   for node in left_nodes},
      max_inputs={node: 5
                  for node in left_nodes},
      new_node_max_outputs=5,
      new_node_max_inputs=5,
      new_node_name_prefix='Picker_Node')

  right_configurations = [{
      'type': 'configure_new_flow_right',
      'parent_handle': {
          'id': ids.new_id('test_parent_node_id')
      },
      'is_data': False,
      'height': 1,
      'n_kids': 4,
      'connection_limit': 20,
  } for i in range(10)]

  right_map = picker.fill_graph(
      left_is_data=False, right_is_data=False, left_height=1, right_height=1, right_configurations=right_configurations)

  for right_most, right_configs in right_map.items():
    assert len(right_configs) <= 5


def test_picker():
  N_LEFT = 10
  graph = NetworkGraph()
  left_nodes = [graph.add_node(ids.new_id('Left_Node')) for i in range(N_LEFT)]
  picker = TopologyPicker(
      graph=graph,
      max_outputs={node: 20
                   for node in left_nodes},
      max_inputs={node: 20
                  for node in left_nodes},
      new_node_max_outputs=20,
      new_node_max_inputs=20,
      new_node_name_prefix='Picker_Node')

  assert 1 == picker.n_layers

  parent_id = ids.new_id('test_parent_node_id')
  right_configurations = [
      {
          'type': 'configure_new_flow_right',
          'parent_handle': {
              'id': parent_id
          },
          'is_data': False,
          'height': 1,
          'n_kids': 4,
          'connection_limit': 20,
      },
  ]
  right_map = picker.fill_graph(
      left_is_data=False, right_is_data=False, left_height=1, right_height=1, right_configurations=right_configurations)
  assert 2 == picker.n_layers
  assert len(left_nodes) == len(picker.get_layer(0))
  new_layer = picker.get_layer(1)
  assert 1 == len(new_layer)
  assert 1 == len(right_map)
  assert right_map[new_layer[0]] == [parent_id]
