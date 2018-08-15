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
def test_sum_two_nodes_on_three_machines(demo, drop_rate, network_error_type, seed, error_regexp):
  # Create node controllers (each simulates the behavior of a separate machine.
  network_errors_config = messages.machine.std_simulated_network_errors_config()
  network_errors_config['outgoing'][network_error_type]['rate'] = drop_rate
  network_errors_config['outgoing'][network_error_type]['regexp'] = error_regexp

  machine_a, machine_b, machine_c = demo.new_machine_controllers(
      3,
      base_config={'network_errors_config': network_errors_config},
      random_seed=seed,
  )

  demo.run_for(ms=200)

  # Configure the starting network topology
  root_input_node_id = dist_zero.ids.new_id('InternalNode_input')
  demo.system.spawn_node(
      on_machine=machine_a,
      node_config=messages.io.internal_node_config(root_input_node_id, parent=None, depth=0, variant='input'))

  root_output_node_id = dist_zero.ids.new_id('InternalNode_output')
  demo.system.spawn_node(
      on_machine=machine_c,
      node_config=messages.io.internal_node_config(
          root_output_node_id, parent=None, depth=0, variant='output', initial_state=0))

  demo.run_for(ms=200)

  # Set up the sum computation with a migration:
  node_id = dist_zero.ids.new_id('MigrationNode_add_sum_computation')
  sum_node_id = dist_zero.ids.new_id('ComputationNode_root')
  input_handle_for_migration = demo.system.generate_new_handle(new_node_id=node_id, existing_node_id=root_input_node_id)
  output_handle_for_migration = demo.system.generate_new_handle(
      new_node_id=node_id, existing_node_id=root_output_node_id)
  demo.system.spawn_node(
      on_machine=machine_b,
      node_config=messages.migration.migration_node_config(
          node_id=node_id,
          source_nodes=[(input_handle_for_migration, messages.migration.source_migrator_config(will_sync=False, ))],
          sink_nodes=[(output_handle_for_migration,
                       messages.migration.sink_migrator_config(
                           new_flow_senders=[sum_node_id],
                           old_flow_sender_ids=[],
                           will_sync=False,
                       ))],
          removal_nodes=[],
          sync_pairs=[],
          insertion_node_configs=[
              messages.computation.computation_node_config(
                  node_id=sum_node_id,
                  depth=1,
                  parent=None,
                  senders=[input_handle_for_migration],
                  receivers=[output_handle_for_migration],
                  migrator=messages.migration.insertion_migrator_config(
                      configure_right_places={input_handle_for_migration['id']: (0, 1)},
                      senders=[input_handle_for_migration],
                      receivers=[output_handle_for_migration],
                  ),
              )
          ]))

  demo.run_for(ms=1000)
  print(demo.system._spawner.node_by_id().keys())
  node_by_id = demo.system._spawner.node_by_id()
  import ipdb
  ipdb.set_trace()

  user_b_output_id = demo.system.create_kid(
      parent_node_id=root_output_node_id, new_node_name='output_b', machine_id=machine_b)
  user_c_output_id = demo.system.create_kid(
      parent_node_id=root_output_node_id, new_node_name='output_c', machine_id=machine_c)

  # Wait for the output nodes to start up
  demo.run_for(ms=200)

  user_b_input_id = demo.system.create_kid(
      parent_node_id=root_input_node_id,
      new_node_name='input_b',
      machine_id=machine_b,
      recorded_user=RecordedUser('user b', [
          (2030, messages.io.input_action(2)),
          (2060, messages.io.input_action(1)),
      ]))
  user_c_input_id = demo.system.create_kid(
      parent_node_id=root_input_node_id,
      new_node_name='input_c',
      machine_id=machine_c,
      recorded_user=RecordedUser('user c', [
          (2033, messages.io.input_action(1)),
          (2043, messages.io.input_action(1)),
          (2073, messages.io.input_action(1)),
      ]))

  demo.run_for(ms=5000)

  # Smoke test that at least one message was acknowledged by the middle sum node.
  sum_node_stats = demo.system.get_stats(sum_node_id)
  assert sum_node_stats['acknowledged_messages'] > 0
  if network_error_type == 'duplicate':
    assert sum_node_stats['n_duplicates'] > 0

  # Check that the output nodes receive the correct sum
  user_b_state = demo.system.get_output_state(user_b_output_id)
  user_c_state = demo.system.get_output_state(user_c_output_id)
  assert 6 == user_b_state
  assert 6 == user_c_state

  # At this point, the nodes should be done sending meaningful messages, run for some more time
  # and assert that certain stats have not changed.
  demo.run_for(ms=2000)
  later_sum_node_stats = demo.system.get_stats(sum_node_id)
  for stat in ['n_retransmissions', 'n_reorders', 'n_duplicates', 'sent_messages', 'acknowledged_messages']:
    assert (sum_node_stats[stat] == later_sum_node_stats[stat]), 'Values were unequal for the stat "{}"'.format(stat)

  b_input_stats = demo.system.get_stats(user_b_input_id)
  c_input_stats = demo.system.get_stats(user_c_input_id)

  assert 2 == b_input_stats['sent_messages']
  assert 3 == c_input_stats['sent_messages']

  assert 2 == b_input_stats['acknowledged_messages']
  assert 3 == c_input_stats['acknowledged_messages']
