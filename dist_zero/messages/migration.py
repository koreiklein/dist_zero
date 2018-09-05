'''
Messages to be received by migration nodes.
'''


def migration_node_config(
    node_id,
    source_nodes,
    sink_nodes,
    removal_nodes,
    insertion_node_configs,
    sync_pairs,
):
  '''
  Node config message for a `MigrationNode`

  :param str node_id: The node id of this migration.

  :param list source_nodes: A list of pairs (source_node_handle, migrator_config)
  :param list sink_nodes: A list of pairs (sink_node_handle, migrator_config)
  :param list removal_nodes: A list of pairs (removal_node_handle, migrator_config)

  :param list insertion_node_configs: A list of node configs for insertion nodes.

  :param list sync_pairs: A list of pairs (node_a_handle, target_node_ids) where node_a should sync data
    to each node in target_node_ids in the syncing stage of the migration.
  '''
  return {
      'type': 'MigrationNode',
      'id': node_id,
      'source_nodes': source_nodes,
      'sink_nodes': sink_nodes,
      'removal_nodes': removal_nodes,
      'insertion_node_configs': insertion_node_configs,
      'sync_pairs': sync_pairs,
  }


def source_migrator_config(will_sync, migration=None):
  '''
  Migrator configuration for a `SourceMigrator`

  :param bool will_sync: True iff the migrator will need to sync its data during a syncing stage.
  :param migration: The :ref:`handle` of the relevant migration, or `None` if it has not yet been spawned.
  :param migration: :ref:`handle`
  '''
  return {
      'type': 'source_migrator',
      'will_sync': will_sync,
      'migration': migration,
  }


def sink_migrator_config(new_flow_senders, old_flow_sender_ids, will_sync, migration=None):
  '''
  :param list new_flow_senders: The list of of the nodes that send to this sink in the new flow.
    When send to the migration node it contains ids, when sent to the migrator it will contain handles.
  :param list[str] old_flow_sender_ids: The list of ids of the nodes that send to this sink in the old flow.
  :param bool will_sync: True iff the migrator will need to sync its data during a syncing stage.
  :param object migration: A migration config, or `None` if the migration is not yet spawned.
  '''
  return {
      'type': 'sink_migrator',
      'new_flow_senders': new_flow_senders,
      'old_flow_sender_ids': old_flow_sender_ids,
      'will_sync': will_sync,
      'migration': migration,
  }


def removal_migrator_config(sender_ids, receiver_ids, will_sync):
  '''
  Configuration for a removal migrator.

  :param list[str] sender_ids: The ids of the senders that will stop sending.
  :param list[str] receiver_ids: The ids of the receivers that will stop receiving.
  :param bool will_sync: True iff the migrator will need to sync its data during a syncing stage.
  '''
  return {'type': 'removal_migrator', 'sender_ids': sender_ids, 'receiver_ids': receiver_ids, 'will_sync': will_sync}


def insertion_migrator_config(configure_right_parent_ids, senders, receivers, right_configurations=None,
                              migration=None):
  '''
  :param list[str] configure_right_parent_ids: The ids of the nodes that will send 'configure_right_parent' to this
    insertion node.
  :param list senders: A list of :ref:`handle` of the `Node` s that will send to self by the end of the migration.
  :param dict[str, object] right_configurations: In same cases, an insertion node can be preconfigured with right_configurations
    given from the parent node that spawned it.   In that case, ``right_configurations`` is a dictionary mapping right node ids
    to the prexisting right_configuration.  Otherwise, ``right_configurations`` is `None`
  :param list receivers: A list of :ref:`handle` of the `Node` s that will receive from self by the end of the migration.
  :param migration: If the insertion node will communicate directly with the migration node, this is a handle for it.
    Otherwise, it is `None`
  :type migration: :ref:`handle` or `None`
  '''
  return {
      'type': 'insertion_migrator',
      'configure_right_parent_ids': configure_right_parent_ids,
      'senders': senders,
      'right_configurations': right_configurations,
      'receivers': receivers,
      'migration': migration
  }


def attach_migrator(migrator_config):
  '''
  Message to inform an existing `Node` instance that it should attach a new `Migrator` instance
  configured from ``migrator_config``

  :param object migrator_config: The migrator configuration for the new `Migrator` instance.
  '''
  return {'type': 'attach_migrator', 'migrator_config': migrator_config}


def attached_migrator(migration_id, insertion_node_handle=None):
  '''
  Informs the receiving `MigrationNode` that the sending node has attached the requested `Migrator` subclass.

  :param str migration_id: The id of the relevant migration.
  :param insertion_node_handle: The :ref:`handle` of the insertion node that just attached, or `None` if the attaching
    `Node` did not have an `InsertionMigrator` migrator.
  :type insertion_node_handle: :ref:`handle`
  '''
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'attached_migrator',
          'insertion_node_handle': insertion_node_handle
      }
  }


def start_flow(migration_id):
  '''
  A migration message indicating to a `SourceMigrator` that it should start the new flow while preserving
  the use of the old flow.

  :param str migration_id: The id of the relevant migration.
  '''
  return {'type': 'migration', 'migration_id': migration_id, 'message': {'type': 'start_flow'}}


def completed_flow(sequence_number):
  '''
  Sent by a sink node to indicate that it has received all the requisite started_flow and replacing_flow
  messages along the new and old flows.

  :param int sequence_number: The first sequence number for which the sink node will be receiving the new flow.
  '''
  return {'type': 'completed_flow', 'sequence_number': sequence_number}


def started_flow(migration_id):
  '''
  Send up the tree of sink nodes to indicate that they have started to receive the new flow.

  :param str migration_id: The id of the relevant migration.
  '''
  return {'type': 'migration', 'migration_id': migration_id, 'message': {'type': 'started_flow'}}


def replacing_flow(migration_id, sequence_number):
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'replacing_flow',
          'sequence_number': sequence_number
      }
  }


def migration_message(migration_id, message):
  '''Wrap a message in a migration message.'''
  return {'type': 'migration', 'migration_id': migration_id, 'message': message}


def start_syncing(migration_id, receivers):
  '''
  During the syncing state of a migration, the migration node will send this message
  to nodes that need to sync their data somewhere.

  :param str migration_id: The id of the relevant migration.
  :param receiver: A list of :ref:`handle` of the `Node` s to which this message's recipient should sync.
  :type receiver: list[:ref:`handle`]
  '''
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'start_syncing',
          'migration_id': migration_id,
          'receivers': receivers
      }
  }


def set_sum_total(migration_id, from_node, total):
  '''
  For sum nodes that are the middle nodes in a migration and are currently migrating but not synced up,
  the message informs them of their total.

  :param str migration_id: The id of the relevant migration.
  :param from_node: The :ref:`handle` of sum node that sent this sum.
  :type from_node: :ref:`handle`
  :param int total: The total to start with.
  '''
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'set_sum_total',
          'from_node': from_node,
          'total': total
      }
  }


def sum_total_set(migration_id):
  '''
  Once a node has set its sum total (during a migration sync) it will send this message
  to the node that sent it its new sum.

  :param str migration_id: The id of the relevant migration.
  '''
  return {'type': 'migration', 'migration_id': migration_id, 'message': {'type': 'sum_total_set'}}


def syncer_is_synced():
  '''
  Indicates to a migrator node that the sender has finished syncing data.
  '''
  return {'type': 'syncer_is_synced'}


def prepare_for_switch(migration_id):
  '''
  Sent from a migration node to insertion, removal, and sink nodes to get them into a deltas_only state
  where they are prepared for the switch.

  :param str migration_id: The id of the relevant migration.
  '''
  return {'type': 'migration', 'migration_id': migration_id, 'message': {'type': 'prepare_for_switch'}}


def prepared_for_switch(migration_id):
  '''
  Sent from insertion, removal, and sink nodes back to the migration node to indicate
  that they have prepared for a switch.

  :param str migration_id: The id of the relevant migration.
  '''
  return {'type': 'migration', 'migration_id': migration_id, 'message': {'type': 'prepared_for_switch'}}


def switch_flows(migration_id):
  '''
  Sent from a migrator node to a source node to indicate that it should now switch to the new flow.

  :param str migration_id: The id of the relevant migration.
  '''
  return {'type': 'migration', 'migration_id': migration_id, 'message': {'type': 'switch_flows'}}


def switched_flows(migration_id):
  '''
  Informs the `MigrationNode` that a sink node has swapped to the new flow.
  '''
  return {'type': 'migration', 'migration_id': migration_id, 'message': {'type': 'switched_flows'}}


def swapped_to_duplicate(migration_id, first_live_sequence_number):
  '''
  These messages are sent along the new flow to indicate that the flows have been swapped.

  :param str migration_id: The id of the relevant migration.
  :param int first_live_sequence_number: The first sequence number that the sender will new use after the swap.
  '''
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'swapped_to_duplicate',
          'first_live_sequence_number': first_live_sequence_number,
      },
  }


def swapped_from_duplicate(migration_id, first_live_sequence_number):
  '''
  These messages are sent along the old flow to indicate that the flows have been swapped.

  :param str migration_id: The id of the relevant migration.
  :param int first_live_sequence_number: The first sequence number that the sender will never send along this old flow.
  '''
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'swapped_from_duplicate',
          'first_live_sequence_number': first_live_sequence_number,
      },
  }


def terminate_migrator(migration_id):
  '''
  Sent from the migration node to migrators to indicate that the migration is over.

  :param str migration_id: The id of the relevant migration.
  '''
  return {'type': 'migration', 'migration_id': migration_id, 'message': {'type': 'terminate_migrator'}}


def migrator_terminated(migration_id):
  '''
  Sent from the migrator nodes to the migration node to indicate that they have been terminated.

  :param str migration_id: The id of the relevant migration.
  '''
  return {'type': 'migration', 'migration_id': migration_id, 'message': {'type': 'migrator_terminated'}}


def substitute_left_configuration(migration_id, new_node_id):
  '''
  Informs a node that the left config is was expecting should actually come from a different node.

  Useful when spawning kids that have a gap on the left side.

  :param str migration_id: The id of the relevant migration.
  :param str new_node_id: The id of the node that will send a left_configuration instead.
  '''
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'substitute_left_configuration',
          'new_node_id': new_node_id
      }
  }


def substitute_right_parent(migration_id, new_parent_id):
  '''
  Informs a node that the right configs it was expecting from one parent should actually come from another parent.

  Useful when spawning kids that have a gap on the left side.

  :param str migration_id: The id of the relevant migration.
  :param str new_parent_id: The id of the parent to listen for instead.
  '''
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'substitute_right_parent',
          'new_parent_id': new_parent_id
      }
  }


def configure_right_parent(migration_id, kid_ids):
  '''
  A node will receive this message from the parents of its eventual receivers to indicate
  which nodes will eventually receive from it.

  Those same nodes that will eventually receive from it should each be expected to send it a right_configuration.

  :param str migration_id: The id of the relevant migration.
  :param list[str] kid_ids: The ids of the `Node` instances that will receive from the node getting this message.
  '''
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'configure_right_parent',
          'kid_ids': kid_ids
      }
  }


def configure_new_flow_right(migration_id, right_configurations):
  '''
  Transmit 'right' configurations, sent while starting a new flow.

  Either `InsertionMigrator` or `SourceMigrator` can receive this message.
  :param str migration_id: The id of the relevant migration.
  :param list[object] right_configurations: The `right_configuration` messages.
  '''
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'configure_new_flow_right',
          'right_configurations': right_configurations,
      }
  }


def right_configuration(parent_handle, is_data, height, n_kids, connection_limit):
  '''

  :param parent_handle: The :ref:`handle` of the sender. A sibling node to the right of the node receiving the message.
  :param int height: The height of the sending node in its tree.  0 for a leaf node, 1 for a parent of a leaf, > 1 for other.
  :param bool is_data: True iff the sending node is a data node.  False iff a computation node.
  :param n_kids: If the right node is a data node with a set number of kids, n_kids will give that number.
    Otherwise, n_kids will be `None`
  :type n_kids: int or None
  :param int connection_limit: The maximum number of connections the receiving node is allowed to add to all kids of its
    right parent.
  '''
  return {
      'parent_handle': parent_handle,
      'is_data': is_data,
      'height': height,
      'n_kids': n_kids,
      'connection_limit': connection_limit
  }


def set_source_right_parents(migration_id, configure_right_parent_ids):
  '''
  Sent from a source parent to each of its kids to let them know which right parents to wait for.

  :param str migration_id: The id of the relevant migration.
  :param list[str] configure_right_parent_ids: The ids of the nodes that will send 'configure_right_parent' to this
    source node.
  '''
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'set_source_right_parents',
          'configure_right_parent_ids': configure_right_parent_ids
      }
  }


def configure_new_flow_left(migration_id, left_configurations):
  '''
  The 'left' configuration, sent while starting a new flow.

  Either `InsertionMigrator` or `SinkMigrator` can receive this message.

  :param str migration_id: The id of the relevant migration.
  :param list[object] left_configurations: The list of `left_configuration` messages.
  '''
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'configure_new_flow_left',
          'left_configurations': left_configurations,
      }
  }


def left_configuration(height, is_data, node, kids):
  '''
  :param int height: The height of the sending node in its tree.  0 for a leaf node, 1 for a parent of a leaf, > 1 for other.
  :param bool is_data: True iff the sending node is a data node.  False iff a computation node.
  :param node: The :ref:`handle` of the sending node.
  :type node: :ref:`handle`
  :param list kids: A list of dictionaries each with the following keys:
     'handle': A :ref:`handle` for a kid
     'connection_limit': The maximum number of outgoing nodes the next node is allowed to add to that kid
  '''
  return {
      'height': height,
      'is_data': is_data,
      'node': node,
      'kids': kids,
  }


def set_new_flow_adjacent(migration_id, adjacent):
  '''
  :param str migration_id: The id of the relevant migration.
  :param adjacent: The :ref:`handle` of a new adjacent node.
  :type adjacent: :ref:`handle`
  '''
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'set_new_flow_adjacent',
          'adjacent': adjacent,
      }
  }
