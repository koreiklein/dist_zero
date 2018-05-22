import time
import logging

import pytest

import dist_zero.ids
from dist_zero import messages, errors, spawners
from dist_zero.node.sum import SumNode
from dist_zero.node.io import InputNode, OutputNode
from dist_zero.recorded import RecordedUser

logger = logging.getLogger(__name__)


@pytest.mark.simulated
def test_times_in_order():
  RecordedUser('user b', [
      (60, messages.sum.increment(1)),
      (80, messages.sum.increment(2)),
  ])

  with pytest.raises(errors.InternalError):
    RecordedUser('user b', [
        (80, messages.sum.increment(2)),
        (60, messages.sum.increment(1)),
    ])


@pytest.mark.simulated
def test_send_no_transport(simulated_system):
  system = simulated_system
  machine_handle = system.create_machine(
      messages.machine.machine_config(machine_name='test machine', machine_controller_id=dist_zero.ids.new_id()))

  machine = system.spawner.get_machine_controller(machine_handle)

  node_a = machine.start_node(messages.io.input_node_config(dist_zero.ids.new_id()))
  node_b = machine.start_node(messages.io.output_node_config(dist_zero.ids.new_id(), initial_state=0))
  with pytest.raises(errors.NoTransportError):
    machine.send(node_handle=node_b.handle(), message=messages.sum.increment(3), sending_node_handle=node_a.handle())


def test_sum_two_nodes_on_three_machines(demo):
  # Create node controllers (each simulates the behavior of a separate machine.
  machine_a_handle, machine_b_handle, machine_c_handle = demo.new_machine_controllers(3)

  demo.run_for(ms=200)

  sum_node_handle = demo.system.spawn_node(
      on_machine=machine_a_handle,
      node_config=messages.sum.sum_node_config(
          node_id=dist_zero.ids.new_id(),
          senders=[],
          sender_transports=[],
          receivers=[],
          receiver_transports=[],
      ))

  demo.run_for(ms=200)

  # Configure the starting network topology
  root_input_node_handle = demo.system.spawn_node(
      on_machine=machine_a_handle, node_config=messages.io.input_node_config(dist_zero.ids.new_id()))
  root_output_node_handle = demo.system.spawn_node(
      on_machine=machine_a_handle, node_config=messages.io.output_node_config(dist_zero.ids.new_id(), initial_state=0))

  demo.run_for(ms=1000)

  demo.system.send_to_node(sum_node_handle,
                           messages.sum.set_input(root_input_node_handle,
                                                  demo.system.create_transport_for(sum_node_handle,
                                                                                   root_input_node_handle)))
  demo.system.send_to_node(sum_node_handle,
                           messages.sum.set_output(root_output_node_handle,
                                                   demo.system.create_transport_for(sum_node_handle,
                                                                                    root_output_node_handle)))

  demo.run_for(ms=200)

  user_b_output_handle = demo.system.create_kid(
      parent_node=root_output_node_handle, new_node_name='output_b', machine_controller_handle=machine_b_handle)
  user_c_output_handle = demo.system.create_kid(
      parent_node=root_output_node_handle, new_node_name='output_c', machine_controller_handle=machine_c_handle)

  # Wait for the output nodes to start up
  demo.run_for(ms=200)

  user_b_input_handle = demo.system.create_kid(
      parent_node=root_input_node_handle,
      new_node_name='input_b',
      machine_controller_handle=machine_b_handle,
      recorded_user=RecordedUser('user b', [
          (2030, messages.sum.increment(2)),
          (2060, messages.sum.increment(1)),
      ]))
  user_c_input_handle = demo.system.create_kid(
      parent_node=root_input_node_handle,
      new_node_name='input_c',
      machine_controller_handle=machine_c_handle,
      recorded_user=RecordedUser('user c', [
          (2033, messages.sum.increment(1)),
          (2043, messages.sum.increment(1)),
          (2073, messages.sum.increment(1)),
      ]))

  demo.run_for(ms=3000)

  user_b_state = demo.system.get_output_state(user_b_output_handle)
  user_c_state = demo.system.get_output_state(user_c_output_handle)
  assert 6 == user_b_state
  assert 6 == user_c_state
