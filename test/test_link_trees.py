import pytest

from dist_zero import ids

#async def _spawn_tree


def _assert_unique_paths(demo, link_root_id):
  input_ids = demo.system.get_senders(link_root_id)
  assert 1 == len(input_ids)
  output_ids = demo.system.get_receivers(link_root_id)
  assert 1 == len(output_ids)

  leftmost_leaves = set(self.get_link_leftmost_leaves(link_root_id))
  output_leaves = set(demo.get_leaves(output_ids[0]))

  n_outgoing_paths = {}

  def _get_n_outgoing_paths(node_id):
    if node_id not in n_outgoing_paths:
      n_outgoing_paths[node_id] = (1 if node_id in output_leaves else sum(
          _get_n_outgoing_paths(receiver) for receiver in demo.system.get_receivers(node_id)))

    return n_outgoing_paths[node_id]

  for input_leaf in demo.get_leaves(input_ids[0]):
    adjacent = demo.system.get_subscribed_link(input_leaf, link_root_id)
    assert adjacent is not None
    assert adjacent in leftmost_leaves
    assert len(output_leaves) == _get_n_outgoing_paths(adjacent)


@pytest.mark.asyncio
async def test_link_one_one(demo):
  machine = await demo.new_machine_controller()
  link_key = 'my_link'
  root_input_id = demo.create_dataset(machine=machine, name='DataInputRoot', height=0, output_link_keys=[link_key])
  root_output_id = demo.create_dataset(machine=machine, name='DataOutputRoot', height=0, input_link_keys=[link_key])
  await demo.run_for(ms=500)
  link_id = demo.link_datasets(
      root_input_id=root_input_id, root_output_id=root_output_id, machine=machine, name='LinkRoot')
  await demo.run_for(ms=1000)
  _assert_unique_paths(demo, link_id)
