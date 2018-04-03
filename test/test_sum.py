import unittest
import time

from nose.plugins.attrib import attr

from dist_zero import messages, errors, runners
from dist_zero.node.sum import SumNode
from dist_zero.node.io import InputNode, OutputNode
from dist_zero.runners.simulator import SimulatedHardware
from dist_zero.runners.docker import DockerSimulatedHardware
from dist_zero.recorded import RecordedUser

@attr(mode=runners.MODE_VIRTUAL)
class VirtualizedSumTest(unittest.TestCase):
  def setUp(self):
    self.virtual_hardware = DockerSimulatedHardware()

  def tearDown(self):
    if self.virtual_hardware.started:
      self.virtual_hardware.clean_all()

  def test_sum_one_virtual(self):
    self.virtual_hardware.start()
    container_a_handle = self.virtual_hardware.new_container()
    self.assertEqual(container_a_handle['type'], 'OsMachineController')

    time.sleep(0.4)  # Any value less than 0.3 has been observed to be too short for broken nodes to actually fail.
    self.assertEqual(1, len(self.virtual_hardware.get_running_containers()))
    self.assertEqual(1, len(self.virtual_hardware.all_spawned_containers()))


@attr(mode=runners.MODE_SIMULATED)
class SimulatedSumTest(unittest.TestCase):
  def setUp(self):
    self.simulated_hardware = SimulatedHardware()
    self.nodes = 0

  def new_machine_controller(self):
    result = self.simulated_hardware.new_simulated_machine_controller(
        name='Node {}'.format(self.nodes))
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
    self.simulated_hardware.run_for(ms=30)

    # Create kid nodes with pre-recorded users.
    user_b_input_handle = self.root_input_node.add_kid(
        self.machine_b_controller.handle(),
        recorded_user=RecordedUser('user b', [
          (30, messages.increment(2)),
          (60, messages.increment(1)),
        ]))
    user_c_input_handle = self.root_input_node.add_kid(
        self.machine_c_controller.handle(),
        recorded_user=RecordedUser('user c', [
          (33, messages.increment(1)),
          (43, messages.increment(1)),
          (73, messages.increment(1)),
        ]))

    user_b_input = self.machine_b_controller.get_node(user_b_input_handle)
    user_c_input = self.machine_c_controller.get_node(user_c_input_handle)

    user_b_output_handle = self.root_output_node.add_kid(self.machine_b_controller.handle())
    user_c_output_handle = self.root_output_node.add_kid(self.machine_c_controller.handle())

    self.simulated_hardware.run_for(ms=30)

    user_b_output = self.machine_b_controller.get_node(user_b_output_handle)
    user_c_output = self.machine_c_controller.get_node(user_c_output_handle)

    self.assertEqual(0, user_b_output.get_state())
    self.assertEqual(0, user_c_output.get_state())

    self.simulated_hardware.run_for(ms=500)

    self.assertEqual(6, user_b_output.get_state())
    self.assertEqual(6, user_c_output.get_state())


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
    self.root_input_node = self.machine_a_controller.start_node(messages.input_node_config())
    self.root_output_node = self.machine_a_controller.start_node(messages.output_node_config())
    self.sum_node = self.machine_a_controller.start_node(messages.sum_node_config(
      senders=[self.root_input_node.handle(), self.root_output_node.handle()],
      receivers=[],
    ))
    self.root_input_node.send_to(self.sum_node.handle())
    self.root_output_node.receive_from(self.sum_node.handle())

  def test_sum_of_two(self):
    self._initialize_simple_sum_topology()
    # Run the simulation
    self.simulated_hardware.start()
    self.simulated_hardware.run_for(ms=30)

    user_b_input_handle = self.root_input_node.add_kid(self.machine_b_controller.handle())
    user_c_input_handle = self.root_input_node.add_kid(self.machine_c_controller.handle())

    user_b_input = self.machine_b_controller.get_node(user_b_input_handle)
    user_c_input = self.machine_c_controller.get_node(user_c_input_handle)

    user_b_output_handle = self.root_output_node.add_kid(self.machine_b_controller.handle())
    user_c_output_handle = self.root_output_node.add_kid(self.machine_c_controller.handle())

    self.simulated_hardware.run_for(ms=30)

    self.machine_b_controller.send(user_b_input.handle(), messages.increment(2))
    self.simulated_hardware.run_for(ms=30)
    self.machine_b_controller.send(user_b_input.handle(), messages.increment(1))
    self.simulated_hardware.run_for(ms=50)
    self.machine_c_controller.send(user_c_input.handle(), messages.increment(1))

    self.simulated_hardware.run_for(ms=500)

    user_b_output = self.machine_b_controller.get_node(user_b_output_handle)
    user_c_output = self.machine_c_controller.get_node(user_c_output_handle)


    self.assertIsNotNone(user_b_output)
    self.assertIsNotNone(user_c_output)

    self.assertEqual(4, user_b_output.get_state())
    self.assertEqual(4, user_c_output.get_state())

