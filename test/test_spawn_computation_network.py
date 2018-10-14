'''
Tests for spawning networks of sum nodes between variously sized input/output trees.
'''

import pytest

from .common import Utils


class TestSpawnComputationNetwork(Utils):
  async def _connect_and_test_io_trees(self, n_input_leaves, n_output_leaves):
    root_input = await self.root_io_tree(machine=self.machine_ids[0], variant='input', state_updater='sum')
    self.root_input = root_input
    root_output = await self.root_io_tree(machine=self.machine_ids[0], variant='output', state_updater='sum')
    self.root_output = root_output

    await self.demo.run_for(ms=200)
    await self.spawn_users(root_output, n_users=n_output_leaves)
    await self.spawn_users(
        root_input,
        n_users=n_input_leaves,
        add_user=True,
        send_after=9000,
        ave_inter_message_time_ms=500,
        send_messages_for_ms=3000)

    # Need to wait for the new users to be fully connected.
    await self.demo.run_for(ms=4000)

    self.root_computation = self.demo.connect_trees_with_sum_network(
        root_input, root_output, machine=self.machine_ids[0])
    # Ensure we haven't simulated any sends yet
    await self.demo.run_for(ms=6000)

    if n_input_leaves > 0:
      assert self.demo.total_simulated_amount > 10 # Smoke test that sends were in fact simulated
    else:
      assert self.demo.total_simulated_amount == 0

    output_leaves = self.demo.all_io_kids(root_output)
    self.output_leaves = output_leaves
    assert len(output_leaves) == n_output_leaves
    self.demo.system.get_senders(root_output)
    self.demo.system.get_receivers(root_input)
    for leaf in output_leaves:
      assert self.demo.total_simulated_amount == self.demo.system.get_output_state(leaf)

  @pytest.mark.asyncio
  async def test_dns(self, cloud_demo):
    self.demo = cloud_demo

    self.machine_ids = await self.demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_dns')

    root_input = await self.root_io_tree(machine=self.machine_ids[0], variant='input', state_updater='sum')
    await self.demo.run_for(ms=6000)
    await self.spawn_users(root_input, n_users=2)
    domain_name = 'www.distzerotesting.com'
    self.demo.system.route_dns(root_input, domain_name)

    await self.demo.run_for(ms=2000)

  @pytest.mark.asyncio
  async def test_spawn_small_small(self, demo):
    self.demo = demo
    self.machine_ids = await demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_small_small')

    await self._connect_and_test_io_trees(n_input_leaves=1, n_output_leaves=1)
    self.demo.render_network(self.root_computation)

  @pytest.mark.asyncio
  async def test_spawn_small_large(self, demo):
    self.demo = demo
    self.machine_ids = await demo.new_machine_controllers(
        4, base_config=self.base_config(), random_seed='test_spawn_small_large')

    await self._connect_and_test_io_trees(n_input_leaves=1, n_output_leaves=10)

  @pytest.mark.asyncio
  async def test_spawn_large_small(self, demo):
    self.demo = demo
    self.machine_ids = await demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_large_small')

    await self._connect_and_test_io_trees(n_input_leaves=10, n_output_leaves=1)

  @pytest.mark.asyncio
  async def test_spawn_large_large(self, demo):
    self.demo = demo
    self.machine_ids = await demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_large_large')

    await self._connect_and_test_io_trees(n_input_leaves=10, n_output_leaves=10)

  @pytest.mark.asyncio
  async def test_spawn_empty_small(self, demo):
    self.demo = demo
    self.machine_ids = await demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_large_large')

    await self._connect_and_test_io_trees(n_input_leaves=0, n_output_leaves=3)

  @pytest.mark.asyncio
  async def test_spawn_small_empty(self, demo):
    self.demo = demo
    self.machine_ids = await demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_large_large')

    await self._connect_and_test_io_trees(n_input_leaves=3, n_output_leaves=0)

  @pytest.mark.asyncio
  async def test_spawn_small_very_large(self, demo):
    self.demo = demo
    self.machine_ids = await demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_small_very_large')

    await self._connect_and_test_io_trees(n_input_leaves=1, n_output_leaves=20)

  @pytest.mark.parametrize(
      'name,start_inputs,start_outputs,new_inputs,new_outputs,ending_input_height,ending_output_height,sender_limit',
      [
          ('grow_input', 2, 2, 5, 0, 1, 1, 10), # Just add inputs
          ('grow_output', 2, 1, 0, 5, 1, 1, 10), # Just add outputs
          ('bump_input_once', 2, 2, 10, 0, 2, 1, 10), # Add enough inputs that the tree bumps its height
          ('bump_input_twice', 2, 2, 29, 0, 3, 1, 10), # Add enough inputs that the tree bumps its height twice
          ('cause_hourglass', 2, 2, 10, 0, 2, 1, 3), # Restrict sender_limit to cause hourglass operations
      ])
  @pytest.mark.asyncio
  async def test_grow_trees(self, demo, name, start_inputs, start_outputs, new_inputs, new_outputs, ending_input_height,
                            ending_output_height, sender_limit):
    self.demo = demo
    config = self.base_config()
    # Make sure not to cause any hourglasses
    config['system_config']['SUM_NODE_SENDER_LIMIT'] = sender_limit

    self.machine_ids = await demo.new_machine_controllers(
        1, base_config=config, random_seed='test_add_leaves_after_spawn')

    await self._connect_and_test_io_trees(n_input_leaves=start_inputs, n_output_leaves=start_outputs)
    await demo.run_for(ms=7000)
    await self.spawn_users(self.root_output, n_users=new_outputs)

    await self.spawn_users(
        self.root_input,
        n_users=new_inputs,
        add_user=True,
        send_after=0,
        ave_inter_message_time_ms=500,
        send_messages_for_ms=3000)

    await demo.run_for(ms=5000)

    self.output_leaves = self.demo.all_io_kids(self.root_output)
    self.input_leaves = self.demo.all_io_kids(self.root_input)
    assert start_outputs + new_outputs == len(self.output_leaves)

    for leaf in self.output_leaves:
      assert self.demo.total_simulated_amount == self.demo.system.get_output_state(leaf)
      senders = self.demo.system.get_senders(leaf)
      assert 1 == len(senders)
      assert 1 == len(self.demo.system.get_receivers(senders[0]))

    for leaf in self.input_leaves:
      receivers = self.demo.system.get_receivers(leaf)
      assert 1 == len(receivers)
      assert 1 == len(self.demo.system.get_senders(receivers[0]))

    assert ending_input_height == demo.system.get_stats(self.root_input)['height']
    assert ending_output_height == demo.system.get_stats(self.root_output)['height']
