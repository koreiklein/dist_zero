import unittest

from dist_zero import messages
from dist_zero.node.sum import SumNode
from dist_zero.node.io import InputNode, OutputNode
from .simulator import SimulatedHardware

class SumTest(unittest.TestCase):
  def setUp(self):
    self.simulated_hardware = SimulatedHardware()
    self.nodes = 0

  def new_machine_controller(self):
    result = self.simulated_hardware.new_simulated_machine_controller(
        name='Node {}'.format(self.nodes))
    self.nodes += 1
    return result

  def test_sum_of_two(self):
    # Create node controllers (each simulates the behavior of a separate machine.
    machine_a_controller = self.new_machine_controller()
    machine_b_controller = self.new_machine_controller()
    machine_c_controller = self.new_machine_controller()

    # Configure the starting network topology
    root_input_node = machine_a_controller.start_node(messages.input_node_config())
    root_output_node = machine_a_controller.start_node(messages.output_node_config())
    sum_node = machine_a_controller.start_node(messages.sum_node_config(
      senders=[root_input_node.handle()],
      receivers=[root_output_node.handle()],
    ))

    # Run the simulation
    self.simulated_hardware.start()
    self.simulated_hardware.run_for(ms=30)

    user_b_input_handle = root_input_node.add_kid(machine_b_controller.handle())
    user_c_input_handle = root_input_node.add_kid(machine_c_controller.handle())

    user_b_input = machine_b_controller.get_node(user_b_input_handle)
    user_c_input = machine_c_controller.get_node(user_c_input_handle)

    user_b_output_handle = root_output_node.add_kid(machine_b_controller.handle())
    user_c_output_handle = root_output_node.add_kid(machine_c_controller.handle())

    self.simulated_hardware.run_for(ms=30)

    machine_b_controller.send(user_b_input.handle(), messages.increment(2))
    self.simulated_hardware.run_for(ms=30)
    machine_b_controller.send(user_b_input.handle(), messages.increment(1))
    self.simulated_hardware.run_for(ms=50)
    machine_c_controller.send(user_c_input.handle(), messages.increment(1))

    self.simulated_hardware.run_for(ms=500)

    user_b_output = machine_b_controller.get_node(user_b_output_handle)
    user_c_output = machine_c_controller.get_node(user_c_output_handle)

    self.assertIsNotNone(user_b_output)
    self.assertIsNotNone(user_c_output)

    self.assertEqual(4, user_b_output.get_state())
    self.assertEqual(4, user_c_output.get_state())

