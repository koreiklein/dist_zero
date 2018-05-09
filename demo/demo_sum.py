'''
Long running demos involving sum nodes.
'''

import time
import random

import dist_zero.ids

from dist_zero import messages, errors, spawners
from dist_zero.spawners.docker import DockerSpawner
from dist_zero.spawners.simulator import SimulatedSpawner
from dist_zero.recorded import RecordedUser
from dist_zero.system_controller import SystemController


class Demo(object):
  '''
  Common Superclass for running these demos.
  '''

  def run(self, name):
    self.setUp()
    try:
      getattr(self, name)()
    finally:
      self.tearDown()


class SumDemo(Demo):
  def setUp(self):
    self.n_machines = 6
    self.system_id = dist_zero.ids.new_id()

    self.virtual_spawner = DockerSpawner(system_id=self.system_id)
    self.simulated_spawner = SimulatedSpawner(system_id=self.system_id, random_seed='SimulatedSumTest')

  def tearDown(self):
    if self.virtual_spawner.started:
      self.virtual_spawner.clean_all()

  def demo_node_splitting(self):
    '''
    Spawn a single sum node, and add senders until it splits.
    '''
    self.simulated = True
    self.system = SystemController(system_id=self.system_id, spawner=self.simulated_spawner)
    self.system.configure_logging()
    self.simulated_spawner.start()
    self._spawn_initial_nodes()
    self._run_for(ms=500)

    self.input_nodes = []
    self._spawn_inputs_loop(n_inputs=15, total_time_ms=20 * 1000)

  def _now_ms(self):
    if self.simulated:
      return self.simulated_spawner.now_ms()
    else:
      return time.time()

  def _run_for(self, ms):
    if self.simulated:
      self.simulated_spawner.run_for(int(ms))
    else:
      time.sleep(ms / 1000)

  def demo_single_node_hits_sender_limit(self):
    '''
    Spawn a single sum node, and keep adding input nodes that send to it.
    When running this demo, a developer should -- through a log analysis tool -- be able to chart the
    number of senders on that node over time, watching as it continues to increase, until eventually it hits
    the node's limit for number of senders and some indication of the limit appears on the chart.
    '''
    self.simulated = False
    self.system = SystemController(system_id=self.system_id, spawner=self.virtual_spawner)
    self.system.configure_logging()

    self.virtual_spawner.start()
    self._spawn_initial_nodes()
    self._run_for(ms=500)

    self.input_nodes = []
    self._spawn_inputs_loop(n_inputs=20, total_time_ms=20 * 1000)

  def _spawn_initial_nodes(self):
    self.machine_handles = self.system.create_machines([
        messages.machine_config(machine_name='machine {}'.format(i), machine_controller_id=dist_zero.ids.new_id())
        for i in range(self.n_machines)
    ])

    machine_a_handle = self.machine_handles[0]

    self.root_input_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle, node_config=messages.input_node_config(dist_zero.ids.new_id()))
    self.root_output_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle, node_config=messages.output_node_config(dist_zero.ids.new_id(), initial_state=0))
    self.sum_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle,
        node_config=messages.sum_node_config(
            node_id=dist_zero.ids.new_id(),
            senders=[],
            receivers=[],
        ))

    self.system.send_to_node(self.root_input_node_handle,
                             messages.start_sending_to(
                                 new_receiver=self.sum_node_handle,
                                 transport=self.system.create_transport_for(
                                     sender=self.root_input_node_handle, receiver=self.sum_node_handle)))
    self.system.send_to_node(self.root_output_node_handle,
                             messages.start_receiving_from(
                                 new_sender=self.sum_node_handle,
                                 transport=self.system.create_transport_for(
                                     sender=self.root_output_node_handle, receiver=self.sum_node_handle),
                             ))

  def _spawn_inputs_loop(self, n_inputs, total_time_ms):
    '''
    Spawn n_inputs input leaf nodes over the course of total_time_ms.

    Each input should randomly send some increment messages.
    '''
    time_per_spawn_ms = total_time_ms / n_inputs
    start_time_ms = self._now_ms()
    end_time_ms = start_time_ms + total_time_ms
    for i in range(n_inputs):
      expected_spawn_time = start_time_ms + time_per_spawn_ms * (i + 1)
      cur_time_ms = self._now_ms()

      if cur_time_ms < expected_spawn_time:
        self._run_for(expected_spawn_time - cur_time_ms)

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
                  [(t, messages.increment(int(random.random() * 20)))
                   for t in sorted(random.random() * remaining_time_ms
                                   for x in range(int(remaining_time_ms / AVE_INTER_MESSAGE_TIME_MS)))])))

    # Let things settle down
    self._run_for(ms=4000)


if __name__ == '__main__':
  #SumDemo().run('demo_single_node_hits_sender_limit')
  SumDemo().run('demo_node_splitting')
