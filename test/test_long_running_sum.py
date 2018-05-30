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

    self.input_nodes = []
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

    self.input_nodes = []
    self._spawn_inputs_loop(n_inputs=20, total_time_ms=20 * 1000)

  def _spawn_initial_nodes(self):
    self.machine_handles = self.system.create_machines([
        messages.machine.machine_config(
            machine_name='machine {}'.format(i), machine_controller_id=dist_zero.ids.new_id())
        for i in range(self.n_machines)
    ])

    machine_a_handle = self.machine_handles[0]

    self.sum_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle,
        node_config=messages.sum.sum_node_config(
            node_id=dist_zero.ids.new_id(),
            senders=[],
            sender_transports=[],
            receivers=[],
            receiver_transports=[],
        ))

    self.root_input_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle,
        node_config=messages.io.internal_node_config(dist_zero.ids.new_id(), variant='input'))
    self.root_output_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle,
        node_config=messages.io.internal_node_config(dist_zero.ids.new_id(), variant='output', initial_state=0))

    self.system.send_to_node(self.sum_node_handle,
                             messages.migration.set_input(self.root_input_node_handle,
                                                          self.system.create_transport_for(
                                                              self.sum_node_handle, self.root_input_node_handle)))
    self.system.send_to_node(self.sum_node_handle,
                             messages.migration.set_output(self.root_output_node_handle,
                                                           self.system.create_transport_for(
                                                               self.sum_node_handle, self.root_output_node_handle)))

  def _spawn_inputs_loop(self, n_inputs, total_time_ms):
    '''
    Spawn n_inputs input leaf nodes over the course of total_time_ms.

    Each input should randomly send some increment messages.
    '''
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

      self.input_nodes.append(
          self.system.create_kid(
              parent_node=self.root_input_node_handle,
              new_node_name='input_{}'.format(i),
              # Place the new nodes on machines in a round-robin manner.
              machine_controller_handle=self.machine_handles[i % len(self.machine_handles)],
              recorded_user=RecordedUser(
                  'user {}'.format(i),
                  [(t, messages.sum.increment(int(random.random() * 20)))
                   for t in sorted(random.random() * remaining_time_ms
                                   for x in range(int(remaining_time_ms / AVE_INTER_MESSAGE_TIME_MS)))])))

    # Let things settle down
    self.demo.run_for(ms=4000)