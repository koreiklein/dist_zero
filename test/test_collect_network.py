from collections import defaultdict

import pytest

from dist_zero import messages, connector, errors

from .common import Utils


def test_weighted_rr_error():
  with pytest.raises(errors.NoRemainingAvailability):
    connector.weighted_rr(kids=list(range(11)), parents=list(range(2)), weights={0: 1, 1: 9})


@pytest.mark.parametrize('n_kids,weights,tolerance', [
    (10, [1, 9], 0.101),
    (12, [21, 60, 120], 0.1),
    (200, [21, 0, 60, 120], 0.01),
    (200, [21, 28, 180, 30, 100, 10, 70], 0.01),
])
def test_weighted_rr(n_kids, weights, tolerance):
  weight_map = dict(enumerate(weights))

  def _to_interval_partition(keys, m):
    total = sum(m[key] for key in keys)
    return [m[key] / total for key in keys]

  kids = list(range(n_kids))
  parents = list(range(len(weight_map)))
  assignment = connector.weighted_rr(kids=kids, parents=parents, weights=weight_map)
  assert len(kids) == len(assignment)
  assert set(kids) == set(assignment.keys())

  assigned_numbers = {parent: 0 for parent in parents}
  for parent in assignment.values():
    assigned_numbers[parent] += 1

  for parent in parents:
    assert assigned_numbers[parent] <= weight_map[parent]

  weight_partition, assigned_partition = [_to_interval_partition(parents, x) for x in [weight_map, assigned_numbers]]
  assert len(weight_partition) == len(assigned_partition)
  for weight_fraction, assigned_fraction in zip(weight_partition, assigned_partition):
    assert abs(weight_fraction - assigned_fraction) < tolerance


class TestSpawnCollectComputationNetwork(Utils):
  @pytest.mark.asyncio
  async def test_simple_collect(self, demo):
    self.demo = demo
    self.machine_ids = await demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_small_small')

    root_input = await self.root_io_tree(
        machine=self.machine_ids[0], variant='input', leaf_config=messages.io.sum_leaf_config(0))
    self.root_input = root_input
    root_output = await self.root_io_tree(
        machine=self.machine_ids[0], variant='output', leaf_config=messages.io.collect_leaf_config())
    self.root_output = root_output

    await self.demo.run_for(ms=200)
    await self.spawn_users(root_output, n_users=2)
    await self.spawn_users(
        root_input, n_users=2, add_user=True, send_after=4000, ave_inter_message_time_ms=500, send_messages_for_ms=3000)

    # Need to wait for the new users to be fully connected.
    await self.demo.run_for(ms=1000)

    self.root_computation = self.demo.connect_trees_with_collect_network(
        root_input, root_output, machine=self.machine_ids[0])

    await self.demo.run_for(ms=8000)
    all_actions = [action['number'] for action in self.demo.all_recorded_actions()]
    self.output_leaves = self.demo.all_io_kids(self.root_output)
    assert 2 == len(self.output_leaves)

    all_outputs = [output for leaf in self.output_leaves for output in self.demo.system.get_output_state(leaf)]
    all_outputs.sort()
    all_actions.sort()
    assert all_outputs == all_actions
