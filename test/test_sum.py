import time
import logging

import pytest

import dist_zero.ids
from dist_zero import messages, errors, spawners
from dist_zero.node.sum import SumNode
from dist_zero.node.io import InternalNode
from dist_zero.recorded import RecordedUser

logger = logging.getLogger(__name__)


@pytest.mark.simulated
def test_times_in_order():
  RecordedUser('user b', [
      (60, messages.io.input_action(1)),
      (80, messages.io.input_action(2)),
  ])

  with pytest.raises(errors.InternalError):
    RecordedUser('user b', [
        (80, messages.io.input_action(2)),
        (60, messages.io.input_action(1)),
    ])


@pytest.mark.parametrize('drop_rate,network_error_type,seed', [
    (0.0, 'drop', 'a'),
    (0.02, 'drop', 'a'),
    (0.02, 'drop', 'b'),
    (0.02, 'drop', 'c'),
    (0.02, 'drop', 'd'),
    (0.02, 'drop', 'e'),
    (0.02, 'drop', 'f'),
    (0.02, 'drop', 'g'),
    (0.27, 'duplicate', 'a'),
    (0.27, 'reorder', 'a'),
])
def test_sum_two_nodes_on_three_machines(demo, drop_rate, network_error_type, seed):
  # Create node controllers (each simulates the behavior of a separate machine.
  network_errors_config = messages.machine.std_simulated_network_errors_config()
  network_errors_config['outgoing'][network_error_type]['rate'] = drop_rate
  network_errors_config['outgoing'][network_error_type]['regexp'] = '.*increment.*'

  machine_a_handle, machine_b_handle, machine_c_handle = demo.new_machine_controllers(
      3,
      base_config={'network_errors_config': network_errors_config},
      random_seed=seed,
  )

  demo.run_for(ms=200)

  sum_node_id = demo.system.spawn_node(
      on_machine=machine_a_handle,
      node_config=messages.sum.sum_node_config(
          node_id=dist_zero.ids.new_id('SumNode'),
          senders=[],
          receivers=[],
      ))

  demo.run_for(ms=200)

  # Configure the starting network topology
  root_input_node_id = dist_zero.ids.new_id('InternalNode')
  demo.system.spawn_node(
      on_machine=machine_a_handle,
      node_config=messages.io.internal_node_config(
          root_input_node_id,
          variant='input',
          adjacent=demo.system.generate_new_handle(new_node_id=root_input_node_id, existing_node_id=sum_node_id)))

  root_output_node_id = dist_zero.ids.new_id('InternalNode')
  demo.system.spawn_node(
      on_machine=machine_a_handle,
      node_config=messages.io.internal_node_config(
          root_output_node_id,
          variant='output',
          adjacent=demo.system.generate_new_handle(new_node_id=root_output_node_id, existing_node_id=sum_node_id),
          initial_state=0))

  demo.run_for(ms=1000)

  user_b_output_id = demo.system.create_kid(
      parent_node_id=root_output_node_id, new_node_name='output_b', machine_controller_handle=machine_b_handle)
  user_c_output_id = demo.system.create_kid(
      parent_node_id=root_output_node_id, new_node_name='output_c', machine_controller_handle=machine_c_handle)

  # Wait for the output nodes to start up
  demo.run_for(ms=200)

  user_b_input_id = demo.system.create_kid(
      parent_node_id=root_input_node_id,
      new_node_name='input_b',
      machine_controller_handle=machine_b_handle,
      recorded_user=RecordedUser('user b', [
          (2030, messages.io.input_action(2)),
          (2060, messages.io.input_action(1)),
      ]))
  user_c_input_id = demo.system.create_kid(
      parent_node_id=root_input_node_id,
      new_node_name='input_c',
      machine_controller_handle=machine_c_handle,
      recorded_user=RecordedUser('user c', [
          (2033, messages.io.input_action(1)),
          (2043, messages.io.input_action(1)),
          (2073, messages.io.input_action(1)),
      ]))

  demo.run_for(ms=5000)

  user_b_state = demo.system.get_output_state(user_b_output_id)
  user_c_state = demo.system.get_output_state(user_c_output_id)
  assert 6 == user_b_state
  assert 6 == user_c_state
