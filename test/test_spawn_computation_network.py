'''
Tests for spawning networks of computation nodes between variously sized input/output trees.
'''

import pytest

import dist_zero.ids

from dist_zero import messages, errors, spawners
from dist_zero.spawners.docker import DockerSpawner
from dist_zero.spawners.simulator import SimulatedSpawner
from dist_zero.recorded import RecordedUser
from dist_zero.system_controller import SystemController


class TestSpawnComputationNetwork(object):
  def base_config(self):
    system_config = messages.machine.std_system_config()
    system_config['INTERNAL_NODE_KIDS_LIMIT'] = 3
    system_config['TOTAL_KID_CAPACITY_TRIGGER'] = 0
    system_config['SUM_NODE_SENDER_LIMIT'] = 6
    system_config['SUM_NODE_RECEIVER_LIMIT'] = 6
    return {
        'system_config': system_config,
        'network_errors_config': messages.machine.std_simulated_network_errors_config(),
    }

  def root_io_tree(self, machine, variant):
    '''spawn a new io tree and return the id of the root.'''
    node_id = dist_zero.ids.new_id('InternalNode_{}_root'.format(variant))
    self.demo.system.spawn_node(
        on_machine=machine,
        node_config=messages.io.internal_node_config(
            node_id, parent=None, height=1, variant=variant, initial_state=0 if variant == 'output' else None))
    self.demo.run_for(ms=200)
    return node_id

  def spawn_users(self,
                  root_input,
                  n_users,
                  ave_inter_message_time_ms=0,
                  send_messages_for_ms=0,
                  send_after=0,
                  add_user=False):
    wait_per_loop = 1700
    total_wait = n_users * wait_per_loop
    waited_so_far = 0
    for i in range(n_users):
      self.demo.system.create_descendant(
          internal_node_id=root_input,
          new_node_name='user_{}'.format(i),
          machine_id=self.machine_ids[i % len(self.machine_ids)],
          recorded_user=None if not add_user else self.demo.new_recorded_user(
              name='user_{}'.format(i),
              send_after=send_after + total_wait - waited_so_far,
              ave_inter_message_time_ms=ave_inter_message_time_ms,
              send_messages_for_ms=send_messages_for_ms,
          ))
      self.demo.run_for(ms=wait_per_loop)
      waited_so_far += wait_per_loop

  def _connect_and_test_io_trees(self, n_input_leaves, n_output_leaves):
    root_input = self.root_io_tree(machine=self.machine_ids[0], variant='input')
    self.root_input = root_input
    root_output = self.root_io_tree(machine=self.machine_ids[0], variant='output')
    self.root_output = root_output

    self.demo.run_for(ms=200)
    self.spawn_users(root_output, n_users=n_output_leaves)
    self.demo.run_for(ms=6000)
    self.spawn_users(
        root_input,
        n_users=n_input_leaves,
        add_user=True,
        send_after=9000,
        ave_inter_message_time_ms=500,
        send_messages_for_ms=3000)
    self.demo.run_for(ms=6000)

    self.root_computation = self.demo.connect_trees_with_sum_network(
        root_input, root_output, machine=self.machine_ids[0])
    # Ensure we haven't simulated any sends yet
    self.demo.run_for(ms=6000)

    if n_input_leaves > 0:
      assert self.demo.total_simulated_amount > 10 # Smoke test that sends were in fact simulated
    else:
      assert self.demo.total_simulated_amount == 0

    output_leaves = self.demo.all_io_kids(root_output)
    self.output_leaves = output_leaves
    assert len(output_leaves) == n_output_leaves
    self.demo.system.get_senders(root_output)
    self.demo.system.get_receivers(root_input)
    self.demo.render_network(self.root_output)
    for leaf in output_leaves:
      assert self.demo.total_simulated_amount == self.demo.system.get_output_state(leaf)

  def test_spawn_small_small(self, demo):
    self.demo = demo
    self.machine_ids = demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_small_small')

    self._connect_and_test_io_trees(n_input_leaves=1, n_output_leaves=1)

  def test_spawn_small_large(self, demo):
    self.demo = demo
    self.machine_ids = demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_small_large')

    self._connect_and_test_io_trees(n_input_leaves=1, n_output_leaves=10)

  def test_spawn_large_small(self, demo):
    self.demo = demo
    self.machine_ids = demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_large_small')

    self._connect_and_test_io_trees(n_input_leaves=10, n_output_leaves=1)

  def test_spawn_large_large(self, demo):
    self.demo = demo
    self.machine_ids = demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_large_large')

    self._connect_and_test_io_trees(n_input_leaves=10, n_output_leaves=10)

  def test_spawn_empty_small(self, demo):
    self.demo = demo
    self.machine_ids = demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_large_large')

    self._connect_and_test_io_trees(n_input_leaves=0, n_output_leaves=3)

  def test_spawn_small_empty(self, demo):
    self.demo = demo
    self.machine_ids = demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_large_large')

    self._connect_and_test_io_trees(n_input_leaves=3, n_output_leaves=0)

  def test_spawn_small_very_large(self, demo):
    self.demo = demo
    self.machine_ids = demo.new_machine_controllers(
        1, base_config=self.base_config(), random_seed='test_spawn_small_very_large')

    self._connect_and_test_io_trees(n_input_leaves=1, n_output_leaves=20)

  @pytest.mark.parametrize(
      'name,start_inputs,start_outputs,new_inputs,new_outputs,ending_input_height,ending_output_height,sender_limit',
      [
          ('grow_input', 2, 2, 5, 0, 1, 1, 10), # Just add inputs
          ('grow_output', 2, 2, 0, 5, 1, 1, 6), # Just add outputs
          ('bump_input', 2, 2, 10, 0, 2, 1, 10), # Add enough inputs that the tree bumps its height
          ('cause_hourglass', 2, 2, 10, 0, 2, 1, 6), # Restrict sender_limit to cause hourglass operations
      ])
  def test_grow_trees(self, demo, name, start_inputs, start_outputs, new_inputs, new_outputs, ending_input_height,
                      ending_output_height, sender_limit):
    self.demo = demo
    config = self.base_config()
    # Make sure not to cause any hourglasses
    config['system_config']['SUM_NODE_SENDER_LIMIT'] = sender_limit

    self.machine_ids = demo.new_machine_controllers(1, base_config=config, random_seed='test_add_leaves_after_spawn')

    self._connect_and_test_io_trees(n_input_leaves=start_inputs, n_output_leaves=start_outputs)
    demo.run_for(ms=7000)
    demo.render_network(self.root_output)
    self.spawn_users(self.root_output, n_users=new_outputs)

    self.spawn_users(
        self.root_input,
        n_users=new_inputs,
        add_user=True,
        send_after=0,
        ave_inter_message_time_ms=500,
        send_messages_for_ms=3000)

    demo.run_for(ms=5000)
    self.demo.render_network(self.root_computation)
    assert start_outputs + new_outputs == len(self.output_leaves)
    for leaf in self.output_leaves:
      assert self.demo.total_simulated_amount == self.demo.system.get_output_state(leaf)

    assert ending_input_height == demo.system.get_stats(self.root_input)['height']
    assert ending_output_height == demo.system.get_stats(self.root_output)['height']
