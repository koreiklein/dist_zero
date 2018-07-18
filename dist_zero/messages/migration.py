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


def source_migrator_config(exporter_swaps):
  '''
  Migrator configuration for a `SourceMigrator`

  :param list exporter_swaps: A list of pairs (receiver_id, new_receivers) giving for each existing
    receiver, the set of new receivers that it should duplicate to.
    In the config used to spawn a source node, each receiver will be identified by a handle.
    In the configs sent to migration nodes, each receiver will be identified by id as the `MigrationNode`
    often must first spawn new receiver nodes in order to have handles with which to identify them.
  '''
  return {'type': 'source_migrator', 'exporter_swaps': exporter_swaps}


def sink_migrator_config(new_flow_sender_ids, old_flow_sender_ids):
  '''
  :param list[str] new_flow_sender_ids: The list of ids of the nodes that sent to this sink in the new flow.
  :param list[str] old_flow_sender_ids: The list of ids of the nodes that sent to this sink in the old flow.
  '''
  return {
      'type': 'sink_migrator',
      'new_flow_sender_ids': new_flow_sender_ids,
      'old_flow_sender_ids': old_flow_sender_ids,
  }


def removal_migrator_config():
  return {'type': 'removal_migrator'}


def insertion_migrator_config(senders, receivers):
  '''
  :param list senders: A list of :ref:`handle` of the `Node` s that will send to self by the end of the migration.
  :param list receivers: A list of :ref:`handle` of the `Node` s that will receive from self by the end of the migration.
  '''
  return {'type': 'insertion_migrator', 'senders': senders, 'receivers': receivers}


def connect_node(node, direction):
  '''
  Inform a node that it is now linked to a new node either
  as a sender or a receiver.

  :param node: The handle of the new node
  :type node: :ref:`handle`
  :param str direction: 'sender' or 'receiver' depending respectively on whether the newly added node will
    send to or receive from the node getting this message.
  '''
  return {'type': 'connect_node', 'node': node, 'direction': direction}


def attach_migrator(migrator_config):
  '''
  Message to inform an existing `Node` instance that it should attach a new `Migrator` instance
  configured from ``migrator_config``

  :param object migrator_config: The migrator configuration for the new `Migrator` instance.
  '''
  return {'type': 'attach_migrator', 'migrator_config': migrator_config}


def attached_migrator(insertion_node_handle=None):
  '''
  Informs the receiving `MigrationNode` that the sending node has attached the requested `Migrator` subclass.

  :param insertion_node_handle: The :ref:`handle` of the insertion node that just attached, or `None` if the attaching
    `Node` did not have an `InsertionMigrator` migrator.
  :type insertion_node_handle: :ref:`handle`
  '''
  return {'type': 'attached_migrator', 'insertion_node_handle': insertion_node_handle}


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


def started_flow(migration_id, sequence_number, sender):
  '''
  Informs a `Node` on the path of a new flow for a migration that the preceeding `Node`
  is now sending messages of the new flow through it.

  :param str migration_id: The id of the relevant migration.
  :param int sequence_number: The sequence number of the first message the receiver will receive
    as part of the new flow.
  :param sender: The :ref:`handle` of the `Node` that will now be sending messages in the new flow.
  :type sender: :ref:`handle`
  '''
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'started_flow',
          'sequence_number': sequence_number,
          'sender': sender
      }
  }


def replacing_flow(migration_id, sequence_number):
  return {
      'type': 'migration',
      'migration_id': migration_id,
      'message': {
          'type': 'replacing_flow',
          'sequence_number': sequence_number
      }
  }


def new_flow_sequence_message(migration_id, sequence_message):
  '''Wrap a sequence message in a migration message.'''
  return {'type': 'migration', 'migration_id': migration_id, 'message': sequence_message}


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


def prepared_for_switch():
  '''
  Sent from insertion, removal, and sink nodes back to the migration node to indicate
  that they have prepared for a switch.

  :param str migration_id: The id of the relevant migration.
  '''
  return {'type': 'prepared_for_switch'}


def switch_flows(migration_id):
  '''
  Sent from a migrator node to a source node to indicate that it should now switch to the new flow.

  :param str migration_id: The id of the relevant migration.
  '''
  return {'type': 'migration', 'migration_id': migration_id, 'message': {'type': 'switch_flows'}}


def switched_flows():
  '''
  Informs the `MigrationNode` that a sink node has swapped to the new flow.
  '''
  return {'type': 'switched_flows'}


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
  :param int first_live_sequence_number: The first sequence number that the sender will new use after the swap.
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
  '''
  return {'type': 'migration', 'migration_id': migration_id, 'message': {'type': 'terminate_migrator'}}


def migrator_terminated():
  '''
  Sent from the migrator nodes to the migration node to indicate that they have been terminated.
  '''
  return {'type': 'migrator_terminated'}
