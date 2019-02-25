import pytest

import dist_zero.ids
from dist_zero import messages, types, errors, intervals
from dist_zero.recorded import RecordedUser

from .common import Utils


@pytest.mark.asyncio
async def test_add_one_leaf_to_empty_input_tree(demo):
  system_config = messages.machine.std_system_config()
  system_config['DATA_NODE_KIDS_LIMIT'] = 3
  system_config['TOTAL_KID_CAPACITY_TRIGGER'] = 0
  machine, = await demo.new_machine_controllers(
      1,
      base_config={
          'system_config': system_config,
          'network_errors_config': messages.machine.std_simulated_network_errors_config(),
      },
      random_seed='test_add_one_leaf_to_empty_input_tree')
  await demo.run_for(ms=200)
  root_input_node_id = dist_zero.ids.new_id('DataNode_input')
  demo.system.spawn_dataset(
      on_machine=machine,
      node_config=messages.io.data_node_config(
          root_input_node_id, parent=None, height=2, leaf_config=messages.io.sum_leaf_config(0)))
  await demo.run_for(ms=2000)

  leaves = demo.get_leaves(root_input_node_id)
  assert 0 == len(leaves)

  leaf_ids = []

  create_new_leaf = lambda name: leaf_ids.append(demo.system.create_descendant(
      data_node_id=root_input_node_id,
      new_node_name=name,
      machine_id=machine,
      recorded_user=RecordedUser('user b', 0, types.Int32, [
          (330, messages.io.input_action(2)),
          (660, messages.io.input_action(1)),
      ]),
      ))

  create_new_leaf('LeafNode_test')
  await demo.run_for(ms=2000)

  leaves = demo.get_leaves(root_input_node_id)
  assert 1 == len(leaves)


@pytest.mark.asyncio
async def test_single_node(demo):
  system_config = messages.machine.std_system_config()
  machine, = await demo.new_machine_controllers(
      1,
      base_config={
          'system_config': system_config,
          'network_errors_config': messages.machine.std_simulated_network_errors_config(),
      },
      random_seed='test_single_node')
  await demo.run_for(ms=200)
  root_input_node_id = dist_zero.ids.new_id('DataNode_input')
  demo.system.spawn_dataset(
      on_machine=machine,
      node_config=messages.io.data_node_config(
          root_input_node_id, parent=None, height=0, leaf_config=messages.io.sum_leaf_config(0)))

  await demo.run_for(ms=200)

  kids = demo.system.get_kids(root_input_node_id)
  assert 0 == len(kids)


def _validate_intervals(demo, node_id):
  starts, stops = set(), set()
  start, stop = demo.system.get_interval(node_id)
  for kid_id in demo.system.get_kids(node_id):
    kid_start, kid_stop = _validate_intervals(demo, kid_id)
    assert start <= kid_start
    assert kid_start not in starts
    starts.add(kid_start)
    if kid_stop == None:
      assert kid_start <= stop
    else:
      assert kid_start <= kid_stop
      assert kid_stop <= stop
      assert kid_stop not in stops
      stops.add(kid_stop)

  return start, stop


@pytest.mark.asyncio
async def test_scale_unconnected_io_tree(demo):
  system_config = messages.machine.std_system_config()
  system_config['DATA_NODE_KIDS_LIMIT'] = 3
  system_config['TOTAL_KID_CAPACITY_TRIGGER'] = 0
  machine, = await demo.new_machine_controllers(
      1,
      base_config={
          'system_config': system_config,
          'network_errors_config': messages.machine.std_simulated_network_errors_config(),
      },
      random_seed='test_scale_unconnected_io_tree')
  await demo.run_for(ms=200)
  root_input_node_id = dist_zero.ids.new_id('DataNode_input')
  demo.system.spawn_dataset(
      on_machine=machine,
      node_config=messages.io.data_node_config(
          root_input_node_id, parent=None, height=2, leaf_config=messages.io.sum_leaf_config(0)))
  await demo.run_for(ms=2000)

  leaf_ids = []

  async def create_new_leaf(name):
    leaf_ids.append(
        demo.system.create_descendant(data_node_id=root_input_node_id, new_node_name=name, machine_id=machine))

  assert 2 == demo.system.get_capacity(root_input_node_id)['height']

  n_new_leaves = 9
  for i in range(n_new_leaves):
    await create_new_leaf(name='test_leaf_{}'.format(i))
    await demo.run_for(ms=1000)

  await demo.run_for(ms=6000)

  assert 3 == demo.system.get_capacity(root_input_node_id)['height']

  n_new_leaves = 27 - 9
  for i in range(n_new_leaves):
    await create_new_leaf(name='test_leaf_{}'.format(i + 9))
    await demo.run_for(ms=1000)

  await demo.run_for(ms=6000)

  assert 4 == demo.system.get_capacity(root_input_node_id)['height']
  _validate_intervals(demo, root_input_node_id)
  root_start, root_stop = demo.system.get_interval(root_input_node_id)
  assert intervals.Min == root_start
  assert intervals.Max == root_stop

  for i in range(27):
    demo.system.kill_node(leaf_ids.pop())
    await demo.run_for(ms=1000)

  await demo.run_for(ms=50 * 1000)

  assert 2 == demo.system.get_capacity(root_input_node_id)['height']
