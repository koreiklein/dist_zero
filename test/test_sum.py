import time
import logging
import unittest

from nose.plugins.attrib import attr

import dist_zero.ids
from dist_zero import messages, errors, spawners
from dist_zero.node.sum import SumNode
from dist_zero.node.io import InputNode, OutputNode
from dist_zero.spawners.simulator import SimulatedSpawner
from dist_zero.spawners.docker import DockerSpawner
from dist_zero.spawners.cloud.aws import Ec2Spawner
from dist_zero.system_controller import SystemController
from dist_zero.recorded import RecordedUser

logger = logging.getLogger(__name__)


@attr(mode=spawners.MODE_CLOUD)
class CloudSumTest(unittest.TestCase):
  def setUp(self):
    system_id = dist_zero.ids.new_id()
    self.cloud_spawner = Ec2Spawner(system_id=system_id)
    self.system = SystemController(system_id=system_id, spawner=self.cloud_spawner)
    self.system.configure_logging()

  def tearDown(self):
    self.cloud_spawner.clean_all()

  # TODO(KK): Figure out whether there's a good way to share test code across different modes.
  def test_sum_one_cloud(self):
    machine_a_handle, machine_b_handle, machine_c_handle = self.system.create_machines([
        messages.machine_config(machine_name='machine a', machine_controller_id=dist_zero.ids.new_id()),
        messages.machine_config(machine_name='machine b', machine_controller_id=dist_zero.ids.new_id()),
        messages.machine_config(machine_name='machine c', machine_controller_id=dist_zero.ids.new_id()),
    ])

    self.assertEqual(machine_a_handle['type'], 'MachineController')

    # Low values for time to sleep have been observed to be too short for broken nodes to actually fail.
    time.sleep(0.4)

    # Configure the starting network topology
    root_input_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle, node_config=messages.input_node_config(dist_zero.ids.new_id()))
    root_output_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle, node_config=messages.output_node_config(dist_zero.ids.new_id(), initial_state=0))
    sum_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle,
        node_config=messages.sum_node_config(
            node_id=dist_zero.ids.new_id(),
            senders=[],
            receivers=[],
        ))

    self.system.send_to_node(root_input_node_handle,
                             messages.start_sending_to(
                                 new_receiver=sum_node_handle,
                                 transport=self.system.create_transport_for(
                                     sender=root_input_node_handle, receiver=sum_node_handle)))
    self.system.send_to_node(root_output_node_handle,
                             messages.start_receiving_from(
                                 new_sender=sum_node_handle,
                                 transport=self.system.create_transport_for(
                                     sender=root_output_node_handle, receiver=sum_node_handle),
                             ))
    time.sleep(0.5)

    user_b_input_handle = self.system.create_kid(
        parent_node=root_input_node_handle,
        new_node_name='input_b',
        machine_controller_handle=machine_b_handle,
        recorded_user=RecordedUser('user b', [
            (3030, messages.increment(2)),
            (3060, messages.increment(1)),
        ]))
    user_c_input_handle = self.system.create_kid(
        parent_node=root_input_node_handle,
        new_node_name='input_c',
        machine_controller_handle=machine_c_handle,
        recorded_user=RecordedUser('user c', [
            (3033, messages.increment(1)),
            (3043, messages.increment(1)),
            (3073, messages.increment(1)),
        ]))

    user_b_output_handle = self.system.create_kid(
        parent_node=root_output_node_handle, new_node_name='output_b', machine_controller_handle=machine_b_handle)
    user_c_output_handle = self.system.create_kid(
        parent_node=root_output_node_handle, new_node_name='output_c', machine_controller_handle=machine_c_handle)

    time.sleep(10)
    user_b_state = self.system.get_output_state(user_b_output_handle)
    user_c_state = self.system.get_output_state(user_c_output_handle)
    self.assertEqual(6, user_b_state)
    self.assertEqual(6, user_c_state)


@attr(mode=spawners.MODE_VIRTUAL)
class VirtualizedSumTest(unittest.TestCase):
  def setUp(self):
    system_id = dist_zero.ids.new_id()
    self.virtual_spawner = DockerSpawner(system_id=system_id)
    self.system = SystemController(system_id=system_id, spawner=self.virtual_spawner)
    self.system.configure_logging()

  def tearDown(self):
    if self.virtual_spawner.started:
      self.virtual_spawner.clean_all()

  def test_sum_one_virtual(self):
    self.virtual_spawner.start()
    machine_a_handle = self.system.create_machine(
        messages.machine_config(machine_name='machine a', machine_controller_id=dist_zero.ids.new_id()))
    machine_b_handle = self.system.create_machine(
        messages.machine_config(machine_name='machine b', machine_controller_id=dist_zero.ids.new_id()))
    machine_c_handle = self.system.create_machine(
        messages.machine_config(machine_name='machine c', machine_controller_id=dist_zero.ids.new_id()))
    self.assertEqual(machine_a_handle['type'], 'MachineController')

    # Low values for time to sleep have been observed to be too short for broken nodes to actually fail.
    time.sleep(0.4)
    self.assertEqual(3, len(self.virtual_spawner.get_running_containers()))
    self.assertEqual(3, len(self.virtual_spawner.all_spawned_containers()))

    # Configure the starting network topology
    root_input_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle, node_config=messages.input_node_config(dist_zero.ids.new_id()))
    root_output_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle, node_config=messages.output_node_config(dist_zero.ids.new_id(), initial_state=0))
    sum_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle,
        node_config=messages.sum_node_config(
            node_id=dist_zero.ids.new_id(),
            senders=[],
            receivers=[],
        ))

    self.system.send_to_node(root_input_node_handle,
                             messages.start_sending_to(
                                 new_receiver=sum_node_handle,
                                 transport=self.system.create_transport_for(
                                     sender=root_input_node_handle, receiver=sum_node_handle)))
    self.system.send_to_node(root_output_node_handle,
                             messages.start_receiving_from(
                                 new_sender=sum_node_handle,
                                 transport=self.system.create_transport_for(
                                     sender=root_output_node_handle, receiver=sum_node_handle),
                             ))
    time.sleep(0.5)

    user_b_input_handle = self.system.create_kid(
        parent_node=root_input_node_handle,
        new_node_name='input_b',
        machine_controller_handle=machine_b_handle,
        recorded_user=RecordedUser('user b', [
            (3030, messages.increment(2)),
            (3060, messages.increment(1)),
        ]))
    user_c_input_handle = self.system.create_kid(
        parent_node=root_input_node_handle,
        new_node_name='input_c',
        machine_controller_handle=machine_c_handle,
        recorded_user=RecordedUser('user c', [
            (3033, messages.increment(1)),
            (3043, messages.increment(1)),
            (3073, messages.increment(1)),
        ]))

    user_b_output_handle = self.system.create_kid(
        parent_node=root_output_node_handle, new_node_name='output_b', machine_controller_handle=machine_b_handle)
    user_c_output_handle = self.system.create_kid(
        parent_node=root_output_node_handle, new_node_name='output_c', machine_controller_handle=machine_c_handle)

    time.sleep(8)
    user_b_state = self.system.get_output_state(user_b_output_handle)
    user_c_state = self.system.get_output_state(user_c_output_handle)
    self.assertEqual(6, user_b_state)
    self.assertEqual(6, user_c_state)


@attr(mode=spawners.MODE_SIMULATED)
class SimulatedSumTest(unittest.TestCase):
  def setUp(self):
    system_id = dist_zero.ids.new_id()
    self.simulated_spawner = SimulatedSpawner(system_id=system_id, random_seed='SimulatedSumTest')
    self.system = SystemController(system_id=system_id, spawner=self.simulated_spawner)
    self.system.configure_logging()
    self.simulated_spawner.start()
    self.nodes = 0

  def test_times_must_be_in_order(self):
    RecordedUser('user b', [
        (60, messages.increment(1)),
        (80, messages.increment(2)),
    ])

    with self.assertRaises(errors.InternalError):
      RecordedUser('user b', [
          (80, messages.increment(2)),
          (60, messages.increment(1)),
      ])

  def new_machine_controller(self):
    name = 'machine {}'.format(self.nodes)
    self.nodes += 1
    return self.system.create_machine(
        messages.machine_config(machine_name=name, machine_controller_id=dist_zero.ids.new_id()))

  def test_send_no_transport(self):
    machine_handle = self.new_machine_controller()
    machine = self.simulated_spawner.get_machine_controller(machine_handle)

    node_a = machine.start_node(messages.input_node_config(dist_zero.ids.new_id()))
    node_b = machine.start_node(messages.output_node_config(dist_zero.ids.new_id(), initial_state=0))
    with self.assertRaises(errors.NoTransportError):
      machine.send(node_handle=node_b.handle(), message=messages.increment(3), sending_node_handle=node_a.handle())

  def test_sum_of_two(self):
    # Create node controllers (each simulates the behavior of a separate machine.
    self.machine_a_handle = self.new_machine_controller()
    self.machine_b_handle = self.new_machine_controller()
    self.machine_c_handle = self.new_machine_controller()

    # Configure the starting network topology
    self.root_input_node_handle = self.system.spawn_node(
        on_machine=self.machine_a_handle, node_config=messages.input_node_config(dist_zero.ids.new_id()))
    self.root_output_node_handle = self.system.spawn_node(
        on_machine=self.machine_a_handle,
        node_config=messages.output_node_config(dist_zero.ids.new_id(), initial_state=0))

    sum_node_handle = self.system.spawn_node(
        on_machine=self.machine_a_handle,
        node_config=messages.sum_node_config(
            node_id=dist_zero.ids.new_id(),
            senders=[],
            receivers=[],
        ))

    self.system.send_to_node(self.root_input_node_handle,
                             messages.start_sending_to(
                                 new_receiver=sum_node_handle,
                                 transport=self.system.create_transport_for(
                                     sender=self.root_input_node_handle, receiver=sum_node_handle)))
    self.system.send_to_node(self.root_output_node_handle,
                             messages.start_receiving_from(
                                 new_sender=sum_node_handle,
                                 transport=self.system.create_transport_for(
                                     sender=self.root_output_node_handle, receiver=sum_node_handle),
                             ))

    # Run the simulation
    self.simulated_spawner.run_for(ms=30)

    user_b_input_handle = self.system.create_kid(
        parent_node=self.root_input_node_handle,
        new_node_name='input_b',
        machine_controller_handle=self.machine_b_handle,
        recorded_user=RecordedUser('user b', [
            (3030, messages.increment(2)),
            (3060, messages.increment(1)),
        ]))
    user_c_input_handle = self.system.create_kid(
        parent_node=self.root_input_node_handle,
        new_node_name='input_c',
        machine_controller_handle=self.machine_c_handle,
        recorded_user=RecordedUser('user c', [
            (3033, messages.increment(1)),
            (3043, messages.increment(1)),
            (3073, messages.increment(1)),
        ]))

    user_b_output_handle = self.system.create_kid(
        parent_node=self.root_output_node_handle,
        new_node_name='output_b',
        machine_controller_handle=self.machine_b_handle)
    user_c_output_handle = self.system.create_kid(
        parent_node=self.root_output_node_handle,
        new_node_name='output_c',
        machine_controller_handle=self.machine_c_handle)

    self.simulated_spawner.run_for(ms=30)

    self.simulated_spawner.run_for(ms=5000)

    user_b_state = self.system.get_output_state(user_b_output_handle)
    user_c_state = self.system.get_output_state(user_c_output_handle)
    self.assertEqual(6, user_b_state)
    self.assertEqual(6, user_c_state)
