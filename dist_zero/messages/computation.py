def computation_node_config(node_id,
                            left_is_data,
                            right_is_data,
                            left_ids,
                            configure_right_parent_ids,
                            height,
                            parent,
                            senders,
                            receiver_ids,
                            migrator,
                            is_mid_node=False,
                            connector=None):
  '''
  A node config for creating an internal `ComputationNode` in a network of computation nodes.

  :param str node_id: The id to use for the new node.
  :param bool left_is_data: True iff the node just to the left is a data node.
  :param bool right_is_data: True iff the node just to the right is a data node.
  :param list[str] configure_right_parent_ids: The ids of the nodes that will send 'configure_right_parent' to this
    insertion node.
  :param parent: The :ref:`handle` of this node's parent.
  :type parent: :ref:`handle`
  :param int height: The height of the new computation node in its tree.
  :param list senders: A list of :ref:`handle` s of sending nodes.
  :param list[str] left_ids: A list of the ids of nodes for which the computation node should expect a left configuration.
  :param bool is_mid_node: True iff this node is functioning as the mid node in an hourglass operation.
  :param list[str] receiver_ids: A list of ids of the nodes that should receive from self, or `None` if that list should
    be determined based on the right_configurations received by the node as it starts up.
  :param migrator: The migrator config for the new node if it is being started as part of a migration.
  :param object connector: Serializable json object representing the `Connector` instance of the newly spawned
    `ComputationNode`.
  '''
  if node_id == 'ComputationNode_adjacent_dvW3HJ7BCOlT':
    import ipdb
    ipdb.set_trace()
  return {
      'type': 'ComputationNode',
      'id': node_id,
      'parent': parent,
      'left_ids': left_ids,
      'height': height,
      'senders': senders,
      'left_is_data': left_is_data,
      'right_is_data': right_is_data,
      'is_mid_node': is_mid_node,
      'configure_right_parent_ids': configure_right_parent_ids,
      'receiver_ids': receiver_ids,
      'migrator': migrator,
      'connector': connector,
  }
