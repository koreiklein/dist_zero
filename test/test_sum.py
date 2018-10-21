import time
import logging

import pytest

import dist_zero.ids
from dist_zero import messages, errors
from dist_zero.node.io import DataNode
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


@pytest.mark.asyncio
@pytest.mark.parametrize('error_regexp,drop_rate,network_error_type,seed', [
    ('.*increment.*', 0.0, 'drop', 'a'),
    ('.*increment.*', 0.02, 'drop', 'a'),
    ('.*increment.*', 0.02, 'drop', 'b'),
    ('.*increment.*', 0.02, 'drop', 'c'),
    ('.*increment.*', 0.02, 'drop', 'd'),
    ('.*increment.*', 0.02, 'drop', 'e'),
    ('.*increment.*', 0.02, 'drop', 'f'),
    ('.*increment.*', 0.02, 'drop', 'g'),
    ('.*increment.*', 0.27, 'duplicate', 'a'),
    ('.*increment.*', 0.27, 'reorder', 'a'),
    ('.*input_action.*', 0.4, 'drop', 'h'),
    ('.*output_action.*', 0.4, 'drop', 'h'),
])
async def test_sum_two_nodes_on_three_machines(demo, drop_rate, network_error_type, seed, error_regexp):
  # Create node controllers (each simulates the behavior of a separate machine.
  network_errors_config = messages.machine.std_simulated_network_errors_config()
  network_errors_config['outgoing'][network_error_type]['rate'] = drop_rate
  network_errors_config['outgoing'][network_error_type]['regexp'] = error_regexp
  system_config = messages.machine.std_system_config()
  system_config['DATA_NODE_KIDS_LIMIT'] = 30
  system_config['TOTAL_KID_CAPACITY_TRIGGER'] = 0
  system_config['SUM_NODE_SENDER_LIMIT'] = 30
  system_config['SUM_NODE_RECEIVER_LIMIT'] = 30

  machine_a, machine_b, machine_c = await demo.new_machine_controllers(
      3,
      base_config={
          'system_config': system_config,
          'network_errors_config': network_errors_config,
      },
      random_seed=seed,
  )

  await demo.run_for(ms=200)

  # Configure the starting network topology
  root_input_node_id = dist_zero.ids.new_id('DataNode_input')
  demo.system.spawn_node(
      on_machine=machine_a,
      node_config=messages.io.data_node_config(
          root_input_node_id, parent=None, height=1, state_updater='sum', variant='input'))

  root_output_node_id = dist_zero.ids.new_id('DataNode_output')
  demo.system.spawn_node(
      on_machine=machine_c,
      node_config=messages.io.data_node_config(
          root_output_node_id, parent=None, height=1, variant='output', state_updater='sum', initial_state=0))

  await demo.run_for(ms=200)

  # Set up the sum computation with a migration:
  root_computation_node_id = demo.connect_trees_with_sum_network(root_input_node_id, root_output_node_id, machine_b)

  await demo.run_for(ms=2000)

  assert root_computation_node_id == demo.system.get_adjacent(root_input_node_id)
  assert root_computation_node_id == demo.system.get_adjacent(root_output_node_id)
  input_kids, output_kids, computation_kids = [
      demo.system.get_kids(nid) for nid in [root_input_node_id, root_output_node_id, root_computation_node_id]
  ]
  assert 1 == len(input_kids)
  assert 1 == len(output_kids)
  assert 2 == len(computation_kids)

  input_kid, output_kid = input_kids[0], output_kids[0]
  computation_kid_a, computation_kid_b = computation_kids
  if input_kid not in demo.system.get_senders(computation_kid_a):
    computation_kid_a, computation_kid_b = (computation_kid_b, computation_kid_a)

  assert input_kid in demo.system.get_senders(computation_kid_a)
  assert computation_kid_a in demo.system.get_senders(computation_kid_b)
  assert computation_kid_b == demo.system.get_adjacent(output_kid)
  comp_a_kids, comp_b_kids = [demo.system.get_kids(nid) for nid in [computation_kid_a, computation_kid_b]]
  assert 1 == len(comp_a_kids)
  assert 1 == len(comp_b_kids)
  sum_kid_a, sum_kid_b = comp_a_kids[0], comp_b_kids[0]

  a_sends_to, b_sends_to = [demo.system.get_receivers(nid) for nid in [sum_kid_a, sum_kid_b]]
  assert 1 == len(a_sends_to)
  assert 0 == len(b_sends_to)
  assert a_sends_to[0] == sum_kid_b

  a_receives_from, b_receives_from = [demo.system.get_senders(nid) for nid in [sum_kid_a, sum_kid_b]]
  assert 0 == len(a_receives_from)
  assert 1 == len(b_receives_from)
  assert b_receives_from[0] == sum_kid_a

  output_kid = demo.system.get_kids(root_output_node_id)[0]
  input_kid = demo.system.get_kids(root_input_node_id)[0]

  user_b_output_id = demo.system.create_kid(parent_node_id=output_kid, new_node_name='output_b', machine_id=machine_b)
  user_c_output_id = demo.system.create_kid(parent_node_id=output_kid, new_node_name='output_c', machine_id=machine_c)
  demo.run_for(ms=2000)

  user_b_input_id = demo.system.create_kid(
      parent_node_id=input_kid,
      new_node_name='input_b',
      machine_id=machine_b,
      recorded_user=RecordedUser('user b', [
          (2030, messages.io.input_action(2)),
          (2060, messages.io.input_action(1)),
      ]))
  user_c_input_id = demo.system.create_kid(
      parent_node_id=input_kid,
      new_node_name='input_c',
      machine_id=machine_c,
      recorded_user=RecordedUser('user c', [
          (2033, messages.io.input_action(1)),
          (2043, messages.io.input_action(1)),
          (2073, messages.io.input_action(1)),
      ]))

  await demo.run_for(ms=5000)

  # Smoke test that at least one message was acknowledged by sum node in the middle.
  if network_error_type == 'duplicate':
    all_sum_kids = []

    def _explore(node):
      kids = demo.system.get_kids(node)
      if len(kids) == 0:
        all_sum_kids.append(node)
      else:
        for kid in kids:
          _explore(kid)

    _explore(root_computation_node_id)
    assert any(demo.system.get_stats(sum_kid)['n_duplicates'] > 0 for sum_kid in all_sum_kids)

  # Check that the output nodes receive the correct sum
  user_b_state = demo.system.get_output_state(user_b_output_id)
  user_c_state = demo.system.get_output_state(user_c_output_id)

  assert 6 == user_b_state
  assert 6 == user_c_state

  # At this point, the nodes should be done sending meaningful messages, run for some more time
  # and assert that certain stats have not changed.
  sum_node_stats = demo.system.get_stats(sum_kid_a)
  demo.run_for(ms=2000)
  later_sum_node_stats = demo.system.get_stats(sum_kid_a)
  for stat in ['n_retransmissions', 'n_reorders', 'n_duplicates', 'sent_messages', 'acknowledged_messages']:
    assert (sum_node_stats[stat] == later_sum_node_stats[stat]), 'Values were unequal for the stat "{}"'.format(stat)

  b_input_stats = demo.system.get_stats(user_b_input_id)
  c_input_stats = demo.system.get_stats(user_c_input_id)

  assert 2 == b_input_stats['sent_messages']
  assert 3 == c_input_stats['sent_messages']

  assert 2 == b_input_stats['acknowledged_messages']
  assert 3 == c_input_stats['acknowledged_messages']
