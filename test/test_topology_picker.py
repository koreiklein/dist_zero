'''
Tests the `TopologyPicker` class.
'''

import pytest

from dist_zero import ids, messages
from dist_zero.network_graph import NetworkGraph
from dist_zero.topology_picker import TopologyPicker, NodeTree, OldTopologyPicker


def test_tree_on_empty_base():
  tree = NodeTree(
      nodes=[],
      max_kids=3,
  )
  assert 2 == tree.height
  assert 0 == len(tree.layers[0])
  assert 1 == len(tree.layers[1])


def test_append_base_to_tree():
  tree = NodeTree(
      nodes=[ids.new_id('TestNode_{}'.format(i)) for i in range(14)],
      max_kids=3,
  )
  assert 4 == tree.height
  root = tree.root
  root_kids = tree.kids[tree.root]
  assert 2 == len(root_kids)
  assert not (tree.is_full)

  tree.bump_height()
  assert 5 == tree.height
  assert root != tree.root
  assert 1 == len(tree.kids[tree.root])
  kid, = tree.kids[tree.root]
  assert kid == root

  assert 14 == len(tree.layers[0])

  tree.append_base(ids.new_id('NewNode'))
  assert 5 == tree.height
  assert 1 == len(tree.kids[tree.root])

  assert 15 == len(tree.layers[0])

  _to_root = lambda node: node if tree.parent[node] == None else _to_root(tree.parent[node])

  for leaf in tree.layers[0]:
    assert tree.root == _to_root(leaf)


@pytest.mark.parametrize('n_lefts,n_rights', [
    (0, 17),
    (17, 0),
    (0, 0),
])
def test_no_picker_errors_on_empty_left_and_right_lists(n_lefts, n_rights):
  picker = TopologyPicker(
      graph=NetworkGraph(),
      lefts=[ids.new_id("TestNode_{}".format(i)) for i in range(n_lefts)],
      rights=[ids.new_id("TestNode_{}".format(i)) for i in range(n_rights)],
      max_outputs=3,
      max_inputs=3,
      name_prefix="TestInternalNode")


def test_append_left():
  picker = TopologyPicker(
      graph=NetworkGraph(),
      lefts=[ids.new_id("TestNode_{}".format(i)) for i in range(10)],
      rights=[ids.new_id("TestNode_{}".format(i)) for i in range(10)],
      max_outputs=4,
      max_inputs=3,
      name_prefix="TestInternalNode")

  # These nodes can all be added without bumping the size of the left tree.
  # They fit because the 10 existing nodes + another 16 nodes is 26 nodes,
  # That's less than 27 == 3 ** 3, the capacity of the left tree.
  for i in range(16):
    picker.append_left(ids.new_id("Inserted_TestNode_{}".format(i)))
    _assert_unique_paths(picker)

    assert 10 + i + 1 == len(picker.lefts)
    assert 4 == len(picker.layers)

  picker.append_left(ids.new_id("Inserted_TestNode_{}".format(i)))
  _assert_unique_paths(picker)
  assert 27 == len(picker.lefts)
  assert 5 == len(picker.layers)


def test_graph_outgoing_with_duplicates():
  graph = NetworkGraph()
  x, y_left, y_right, z, r, l = [ids.new_id("TestNode_{}".format(i)) for i in range(6)]
  graph.add_node(x)
  graph.add_node(y_left)
  graph.add_node(z)
  graph.add_edge(x, y_left)
  graph.add_edge(y_left, z)

  assert [z] == graph.transitive_outgoing_with_duplicates(y_left)
  assert [x] == graph.transitive_incomming_with_duplicates(y_left)
  assert [z] == graph.transitive_outgoing_with_duplicates(x)
  assert [x] == graph.transitive_incomming_with_duplicates(z)

  graph.add_node(y_right)
  graph.add_edge(x, y_right)
  graph.add_edge(y_right, z)

  for y in [y_left, y_right]:
    assert [z] == graph.transitive_outgoing_with_duplicates(y)
    assert [x] == graph.transitive_incomming_with_duplicates(y)
  assert [z, z] == graph.transitive_outgoing_with_duplicates(x)
  assert [x, x] == graph.transitive_incomming_with_duplicates(z)

  for last in [r, l]:
    graph.add_node(last)
    graph.add_edge(z, last)

  assert [x, x] == graph.transitive_incomming_with_duplicates(z)
  outgoing_from_x = graph.transitive_outgoing_with_duplicates(x)
  assert 4 == len(outgoing_from_x)
  for last in [r, l]:
    assert 2 == len([n for n in outgoing_from_x if n == last])


def test_unique_path():
  _assert_unique_paths(
      TopologyPicker(
          graph=NetworkGraph(),
          lefts=[ids.new_id("edge_left_{}".format(i)) for i in range(30)],
          rights=[ids.new_id("edge_right_{}".format(i)) for i in range(30)],
          max_outputs=3,
          max_inputs=4,
          name_prefix="TestInternalNode"))


def _assert_unique_paths(picker):
  for left in picker.lefts:
    outgoing = picker.graph.transitive_outgoing_with_duplicates(left)
    assert len(outgoing) == len(picker.rights)
    assert set(outgoing) == set(picker.rights)

  for right in picker.rights:
    incomming = picker.graph.transitive_incomming_with_duplicates(right)
    assert len(incomming) == len(picker.lefts)
    assert set(incomming) == set(picker.lefts)


# FIXME(KK): Remove or rewrite this test.
def test_many_right_configs():
  N_LEFT = 1
  graph = NetworkGraph()
  left_nodes = [graph.add_node(ids.new_id('Left_Node')) for i in range(N_LEFT)]
  picker = OldTopologyPicker(
      graph=graph,
      left_is_data=False,
      right_is_data=False,
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

  right_map = picker.fill_graph(right_configurations=right_configurations)

  for right_most, right_configs in right_map.items():
    assert len(right_configs) <= 5


# FIXME(KK): Remove or rewrite this test.
def test_picker():
  N_LEFT = 10
  graph = NetworkGraph()
  left_nodes = [graph.add_node(ids.new_id('Left_Node')) for i in range(N_LEFT)]
  picker = OldTopologyPicker(
      graph=graph,
      left_is_data=False,
      right_is_data=False,
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
  right_map = picker.fill_graph(right_configurations=right_configurations)
  assert 2 == picker.n_layers
  assert len(left_nodes) == len(picker.get_layer(0))
  new_layer = picker.get_layer(1)
  assert 1 == len(new_layer)
  assert 1 == len(right_map)
  assert right_map[new_layer[0]] == [parent_id]
