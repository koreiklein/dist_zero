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
  def test_node_splitting(self, demo):
    '''
    Spawn a single sum node, and add senders until it splits.
    '''
    self.demo = demo
    self.n_machines = 6

    self.simulated = True
    self.system = demo.system
    self._spawn_initial_nodes()
    self.demo.run_for(ms=500)

    self.input_node_ids = []
    self._spawn_inputs_loop(n_inputs=15, total_time_ms=20 * 1000)

  def test_single_node_hits_sender_limit(self, demo):
    '''
    Spawn a single sum node, and keep adding input nodes that send to it.
    When running this demo, a developer should -- through a log analysis tool -- be able to chart the
    number of senders on that node over time, watching as it continues to increase, until eventually it hits
    the node's limit for number of senders and some indication of the limit appears on the chart.
    '''
    self.n_machines = 6
    self.demo = demo

    self.simulated = False
    self.system = self.demo.system

    self._spawn_initial_nodes()
    self.demo.run_for(ms=500)

    self.input_node_ids = []
    self._spawn_inputs_loop(n_inputs=20, total_time_ms=20 * 1000)

  def _spawn_initial_nodes(self):
    self.machine_ids = self.demo.new_machine_controllers(self.n_machines)

    machine_a = self.machine_ids[0]

    self.sum_node_id = self.system.spawn_node(
        on_machine=machine_a,
        node_config=messages.sum.sum_node_config(
            node_id=dist_zero.ids.new_id('SumNode'),
            senders=[],
            receivers=[],
        ))

    self.root_input_node_id = dist_zero.ids.new_id('InternalNode')
    self.system.spawn_node(
        on_machine=machine_a,
        node_config=messages.io.internal_node_config(
            self.root_input_node_id,
            adjacent=self.system.generate_new_handle(
                new_node_id=self.root_input_node_id, existing_node_id=self.sum_node_id),
            variant='input'))
    self.root_output_node_id = dist_zero.ids.new_id('InternalNode')
    self.system.spawn_node(
        on_machine=machine_a,
        node_config=messages.io.internal_node_config(
            self.root_output_node_id,
            variant='output',
            adjacent=self.system.generate_new_handle(
                new_node_id=self.root_output_node_id, existing_node_id=self.sum_node_id),
            initial_state=0))

  def _spawn_inputs_loop(self, n_inputs, total_time_ms):
    '''
    Spawn n_inputs input leaf nodes over the course of total_time_ms.

    Each input should randomly send some increment messages.
    '''
    rand = random.Random('static seed')

    time_per_spawn_ms = total_time_ms / n_inputs
    start_time_ms = self.demo.now_ms()
    end_time_ms = start_time_ms + total_time_ms
    for i in range(n_inputs):
      expected_spawn_time = start_time_ms + time_per_spawn_ms * (i + 1)
      cur_time_ms = self.demo.now_ms()

      if cur_time_ms < expected_spawn_time:
        self.demo.run_for(expected_spawn_time - cur_time_ms)

      remaining_time_ms = (end_time_ms - cur_time_ms) - 30 # Send messages in almost the whole remaining time window.
      AVE_INTER_MESSAGE_TIME_MS = 1200

      self.input_node_ids.append(
          self.system.create_kid(
              parent_node_id=self.root_input_node_id,
              new_node_name='input_{}'.format(i),
              # Place the new nodes on machines in a round-robin manner.
              machine_id=self.machine_ids[i % len(self.machine_ids)],
              recorded_user=RecordedUser(
                  'user {}'.format(i),
                  [(t, messages.io.input_action(int(rand.random() * 20)))
                   for t in sorted(rand.random() * remaining_time_ms
                                   for x in range(int(remaining_time_ms / AVE_INTER_MESSAGE_TIME_MS)))])))

    # Let things settle down
    self.demo.run_for(ms=4000)
