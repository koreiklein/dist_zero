'''
Long running demos involving sum nodes.
'''

import pytest
import time
import random

import dist_zero.ids

from dist_zero import messages, errors, spawners
from dist_zero.spawners.docker import DockerSpawner
from dist_zero.spawners.simulator import SimulatedSpawner
from dist_zero.recorded import RecordedUser
from dist_zero.system_controller import SystemController


class TestLongRunningSum(object):
  @pytest.mark.parametrize('error_regexp,drop_rate,network_error_type', [
      ('.*increment.*', 0.3, 'drop'),
      ('.*increment.*', 0.27, 'duplicate'),
      ('.*increment.*', 0.27, 'reorder'),
  ])
  def test_node_splitting(self, demo, error_regexp, drop_rate, network_error_type):
    '''
    Spawn a single sum node, and add senders until it splits.
    '''
    self.demo = demo
    self.n_machines = 6

    network_errors_config = messages.machine.std_simulated_network_errors_config()
    network_errors_config['outgoing'][network_error_type]['rate'] = drop_rate
    network_errors_config['outgoing'][network_error_type]['regexp'] = error_regexp

    n_inputs_at_split = 15

    self._base_config = {
        'system_config': {
            'SUM_NODE_SENDER_LIMIT': n_inputs_at_split,
        },
        'network_errors_config': network_errors_config,
    }

    self._rand = random.Random('test_node_splitting')
    self._total_simulated_amount = 0

    self.simulated = True
    self.system = demo.system
    self._spawn_initial_nodes()
    self.demo.run_for(ms=500)

    self.input_node_ids = []
    self._spawn_inputs_loop(n_inputs=n_inputs_at_split, total_time_ms=20 * 1000)
    self.demo.run_for(ms=2000)

    # Assert that each input node has received acknowledgments for all its sent messages.
    for input_node_id in self.input_node_ids:
      stats = self.demo.system.get_stats(input_node_id)
      assert stats['sent_messages'] == stats['acknowledged_messages']

    assert self._total_simulated_amount == self.demo.system.get_output_state(self.user_a_output_id)
    assert self._total_simulated_amount == self.demo.system.get_output_state(self.user_b_output_id)

  def test_single_node_hits_sender_limit(self, demo):
    '''
    Spawn a single sum node, and keep adding input nodes that send to it.
    When running this demo, a developer should -- through a log analysis tool -- be able to chart the
    number of senders on that node over time, watching as it continues to increase, until eventually it hits
    the node's limit for number of senders and some indication of the limit appears on the chart.
    '''
    self.n_machines = 6
    self.demo = demo
    self._base_config = None

    self._rand = random.Random('test_single_node_hits_sender_limit')
    self._total_simulated_amount = 0

    self.simulated = False
    self.system = self.demo.system

    self._spawn_initial_nodes()
    self.demo.run_for(ms=500)

    self.input_node_ids = []
    self._spawn_inputs_loop(n_inputs=20, total_time_ms=20 * 1000)

  def _spawn_initial_nodes(self):
    self.machine_ids = self.demo.new_machine_controllers(self.n_machines, base_config=self._base_config)

    machine_a = self.machine_ids[0]

    self.sum_node_id = self.system.spawn_node(
        on_machine=machine_a,
        node_config=messages.sum.sum_node_config(
            node_id=dist_zero.ids.new_id('SumNode_internal'),
            senders=[],
            receivers=[],
        ))

    self.root_input_node_id = dist_zero.ids.new_id('InternalNode_input')
    self.system.spawn_node(
        on_machine=machine_a,
        node_config=messages.io.internal_node_config(
            self.root_input_node_id,
            adjacent=self.system.generate_new_handle(
                new_node_id=self.root_input_node_id, existing_node_id=self.sum_node_id),
            variant='input'))
    self.root_output_node_id = dist_zero.ids.new_id('InternalNode_output')
    self.system.spawn_node(
        on_machine=machine_a,
        node_config=messages.io.internal_node_config(
            self.root_output_node_id,
            variant='output',
            adjacent=self.system.generate_new_handle(
                new_node_id=self.root_output_node_id, existing_node_id=self.sum_node_id),
            initial_state=0))

    self.demo.run_for(1000)

    self.user_a_output_id = self.demo.system.create_kid(
        parent_node_id=self.root_output_node_id, new_node_name='output_a', machine_id=self.machine_ids[1])
    self.user_b_output_id = self.demo.system.create_kid(
        parent_node_id=self.root_output_node_id, new_node_name='output_b', machine_id=self.machine_ids[2])

  def _spawn_inputs_loop(self, n_inputs, total_time_ms):
    '''
    Spawn n_inputs input leaf nodes over the course of total_time_ms.

    Each input should randomly send some increment messages.
    '''

    time_per_spawn_ms = total_time_ms // n_inputs
    start_time_ms = self.demo.now_ms()
    end_time_ms = start_time_ms + total_time_ms
    for i in range(n_inputs):
      expected_spawn_time = start_time_ms + time_per_spawn_ms * (i + 1)
      cur_time_ms = self.demo.now_ms()

      if cur_time_ms < expected_spawn_time:
        self.demo.run_for(expected_spawn_time - cur_time_ms)

      remaining_time_ms = (end_time_ms - cur_time_ms) - 30 # Send messages in almost the whole remaining time window.

      self.input_node_ids.append(
          self.system.create_kid(
              parent_node_id=self.root_input_node_id,
              new_node_name='input_{}'.format(i),
              # Place the new nodes on machines in a round-robin manner.
              machine_id=self.machine_ids[i % len(self.machine_ids)],
              recorded_user=self._new_recorded_user(
                  name='user {}'.format(i),
                  ave_inter_message_time_ms=1200,
                  send_messages_for_ms=remaining_time_ms + 2 * 1000)))

    # Let things settle down
    self.demo.run_for(ms=4000)

  def _new_recorded_user(self, name, ave_inter_message_time_ms, send_messages_for_ms):
    time_message_pairs = []
    times_to_send = sorted(
        self._rand.random() * send_messages_for_ms for x in range(send_messages_for_ms // ave_inter_message_time_ms))

    for t in times_to_send:
      amount_to_send = int(self._rand.random() * 20)
      self._total_simulated_amount += amount_to_send
      time_message_pairs.append((t, messages.io.input_action(amount_to_send)))

    return RecordedUser(name, time_message_pairs)
