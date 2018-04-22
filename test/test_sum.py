import time
import logging
import unittest
import uuid

from nose.plugins.attrib import attr

from dist_zero import messages, errors, spawners
from dist_zero.node.sum import SumNode
from dist_zero.node.io import InputNode, OutputNode
from dist_zero.spawners.simulator import SimulatedHardware
from dist_zero.spawners.docker import DockerSpawner
from dist_zero.spawners.cloud.aws import Ec2Spawner
from dist_zero.system_controller import SystemController
from dist_zero.recorded import RecordedUser

logger = logging.getLogger(__name__)


@attr(mode=spawners.MODE_CLOUD)
class CloudSumTest(unittest.TestCase):
  def setUp(self):
    system_id = str(uuid.uuid4())
    self.cloud_spawner = Ec2Spawner(system_id=system_id)
    self.system = SystemController(system_id=system_id, spawner=self.cloud_spawner)
    self.system.configure_logging()

  def tearDown(self):
    self.cloud_spawner.clean_all()

  # TODO(KK): Figure out whether there's a good way to share test code across different modes.
  def test_sum_one_cloud(self):
    machine_a_handle, machine_b_handle, machine_c_handle = self.system.create_machines([
        messages.machine_config(machine_name='machine a', machine_controller_id=str(uuid.uuid4())),
        messages.machine_config(machine_name='machine b', machine_controller_id=str(uuid.uuid4())),
        messages.machine_config(machine_name='machine c', machine_controller_id=str(uuid.uuid4())),
    ])

    self.assertEqual(machine_a_handle['type'], 'OsMachineController')

    # Low values for time to sleep have been observed to be too short for broken nodes to actually fail.
    time.sleep(0.4)
    #self.assertEqual(3, len(self.virtual_spawner.get_running_containers()))
    #self.assertEqual(3, len(self.virtual_spawner.all_spawned_containers()))

    # Configure the starting network topology
    root_input_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle, node_config=messages.input_node_config(str(uuid.uuid4())))
    root_output_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle, node_config=messages.output_node_config(str(uuid.uuid4()), initial_state=0))
    sum_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle,
        node_config=messages.sum_node_config(
            node_id=str(uuid.uuid4()),
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
    self.virtual_spawner = DockerSpawner()
    system_id = str(uuid.uuid4())
    self.system = SystemController(system_id=system_id, spawner=self.virtual_spawner)
    self.system.configure_logging()

  def tearDown(self):
    if self.virtual_spawner.started:
      self.virtual_spawner.clean_all()

  def test_sum_one_virtual(self):
    self.virtual_spawner.start()
    machine_a_handle = self.system.create_machine(
        messages.machine_config(machine_name='machine a', machine_controller_id=str(uuid.uuid4())))
    machine_b_handle = self.system.create_machine(
        messages.machine_config(machine_name='machine b', machine_controller_id=str(uuid.uuid4())))
    machine_c_handle = self.system.create_machine(
        messages.machine_config(machine_name='machine c', machine_controller_id=str(uuid.uuid4())))
    self.assertEqual(machine_a_handle['type'], 'OsMachineController')

    # Low values for time to sleep have been observed to be too short for broken nodes to actually fail.
    time.sleep(0.4)
    self.assertEqual(3, len(self.virtual_spawner.get_running_containers()))
    self.assertEqual(3, len(self.virtual_spawner.all_spawned_containers()))

    # Configure the starting network topology
    root_input_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle, node_config=messages.input_node_config(str(uuid.uuid4())))
    root_output_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle, node_config=messages.output_node_config(str(uuid.uuid4()), initial_state=0))
    sum_node_handle = self.system.spawn_node(
        on_machine=machine_a_handle,
        node_config=messages.sum_node_config(
            node_id=str(uuid.uuid4()),
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
    self.simulated_hardware = SimulatedHardware()
    self.nodes = 0

  def new_machine_controller(self):
    result = self.simulated_hardware.new_simulated_machine_controller(name='Node {}'.format(self.nodes))
    self.nodes += 1

    return result

  def test_times_must_be_in_order(self):
    with self.assertRaises(errors.InternalError):
      RecordedUser('user b', [
          (80, messages.increment(2)),
          (60, messages.increment(1)),
      ])

  def test_user_simulator_sum_of_two(self):
    self._initialize_simple_sum_topology()

    # Start the simulation
    self.simulated_hardware.start()
    self.simulated_hardware.run_for(ms=2)

    # Create kid nodes with pre-recorded users.
    user_b_input_config = self.root_input_node.create_kid_config('input_b', self.machine_b_controller.handle())
    user_b_input_config['recorded_user_json'] = RecordedUser('user b', [
        (30, messages.increment(2)),
        (60, messages.increment(1)),
    ]).to_json()
    user_b_input = self.machine_b_controller.start_node(user_b_input_config)

    user_c_input_config = self.root_input_node.create_kid_config('input_c', self.machine_c_controller.handle())
    user_c_input_config['recorded_user_json'] = RecordedUser('user c', [
        (33, messages.increment(1)),
        (43, messages.increment(1)),
        (73, messages.increment(1)),
    ]).to_json()
    user_c_input = self.machine_c_controller.start_node(user_c_input_config)

    self.simulated_hardware.run_for(ms=6)

    user_b_output = self.machine_b_controller.start_node(
        self.root_output_node.create_kid_config('output_b', self.machine_b_controller.handle()))
    user_c_output = self.machine_c_controller.start_node(
        self.root_output_node.create_kid_config('output_c', self.machine_c_controller.handle()))

    self.assertEqual(0, self.machine_b_controller.get_output_state(user_b_output.handle()['id']))
    self.assertEqual(0, self.machine_c_controller.get_output_state(user_c_output.handle()['id']))

    logger.info('running a big testing simulation')
    self.simulated_hardware.run_for(ms=200)

    self.assertEqual(6, self.machine_b_controller.get_output_state(user_b_output.handle()['id']))
    self.assertEqual(6, self.machine_c_controller.get_output_state(user_c_output.handle()['id']))

  def _initialize_simple_sum_topology(self):
    '''
    Initialize controllers and nodes forming a simple topology for an network
    in which input nodes generate increments, and output nodes aggregated the sum
    of increments.
    '''
    # Create node controllers (each simulates the behavior of a separate machine.
    self.machine_a_controller = self.new_machine_controller()
    self.machine_b_controller = self.new_machine_controller()
    self.machine_c_controller = self.new_machine_controller()

    # Configure the starting network topology
    self.root_input_node = self.machine_a_controller.start_node(messages.input_node_config(str(uuid.uuid4())))
    self.root_output_node = self.machine_a_controller.start_node(
        messages.output_node_config(str(uuid.uuid4()), initial_state=0))
    self.sum_node = self.machine_a_controller.start_node(
        messages.sum_node_config(
            str(uuid.uuid4()),
            senders=[self.root_input_node.handle(), self.root_output_node.handle()],
            receivers=[],
        ))
    self.root_input_node.start_sending_to(
        self.sum_node.handle(), transport=self.sum_node.new_transport_for(self.root_input_node.handle()))
    self.root_output_node.receive_from(
        self.sum_node.handle(), transport=self.sum_node.new_transport_for(self.root_output_node.handle()))

  def test_sum_of_two(self):
    self._initialize_simple_sum_topology()
    # Run the simulation
    self.simulated_hardware.start()
    self.simulated_hardware.run_for(ms=30)

    user_b_input_config = self.root_input_node.create_kid_config('input_b', self.machine_b_controller.handle())
    user_b_input = self.machine_b_controller.start_node(user_b_input_config)

    user_c_input_config = self.root_input_node.create_kid_config('input_c', self.machine_c_controller.handle())
    user_c_input = self.machine_c_controller.start_node(user_c_input_config)

    user_b_output = self.machine_b_controller.start_node(
        self.root_output_node.create_kid_config('output_b', self.machine_b_controller.handle()))
    user_c_output = self.machine_c_controller.start_node(
        self.root_output_node.create_kid_config('output_c', self.machine_c_controller.handle()))

    self.simulated_hardware.run_for(ms=30)

    self.machine_b_controller.send(user_b_input.handle(), messages.increment(2))
    self.simulated_hardware.run_for(ms=30)
    self.machine_b_controller.send(user_b_input.handle(), messages.increment(1))
    self.simulated_hardware.run_for(ms=50)
    self.machine_c_controller.send(user_c_input.handle(), messages.increment(1))

    self.simulated_hardware.run_for(ms=500)

    self.assertIsNotNone(user_b_output)
    self.assertIsNotNone(user_c_output)

    self.assertEqual(4, self.machine_b_controller.get_output_state(user_b_output.handle()['id']))
    self.assertEqual(4, self.machine_c_controller.get_output_state(user_c_output.handle()['id']))
