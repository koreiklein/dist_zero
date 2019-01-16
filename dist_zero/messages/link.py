def link_node_config(node_id,
                     left_is_data,
                     right_is_data,
                     left_ids,
                     configure_right_parent_ids,
                     height,
                     parent,
                     senders,
                     receiver_ids,
                     migrator,
                     connector_type,
                     leaf_config,
                     is_mid_node=False,
                     connector=None):
  '''
  A node config for creating an internal `LinkNode` in a network of link nodes.

  :param str node_id: The id to use for the new node.
  :param bool left_is_data: True iff the node just to the left is a data node.
  :param bool right_is_data: True iff the node just to the right is a data node.
  :param list[str] configure_right_parent_ids: The ids of the nodes that will send 'configure_right_parent' to this
      insertion node.
  :param parent: The :ref:`handle` of this node's parent.
  :type parent: :ref:`handle`
  :param int height: The height of the new link node in its tree.
  :param list senders: A list of :ref:`handle` s of sending nodes.
  :param list[str] left_ids: A list of the ids of nodes for which the link node should expect a left configuration.
  :param bool is_mid_node: True iff this node is functioning as the mid node in an hourglass operation.
  :param list[str] receiver_ids: A list of ids of the nodes that should receive from self, or `None` if that list should
      be determined based on the right_configurations received by the node as it starts up.
  :param object leaf_config: Configuration information for how this `LinkNode` should run its leaves.
  :param migrator: The migrator config for the new node if it is being started as part of a migration.
  :param object connector_type: One of the connector_type messages, defining which type of connector to use.
  :param object connector: Serializable json object representing the `Connector` instance of the newly spawned
      `LinkNode`.
  '''
  return {
      'type': 'LinkNode',
      'id': node_id,
      'parent': parent,
      'left_ids': left_ids,
      'height': height,
      'senders': senders,
      'left_is_data': left_is_data,
      'right_is_data': right_is_data,
      'is_mid_node': is_mid_node,
      'configure_right_parent_ids': configure_right_parent_ids,
      'leaf_config': leaf_config,
      'receiver_ids': receiver_ids,
      'migrator': migrator,
      'connector_type': connector_type,
      'connector': connector,
  }


def all_to_all_connector_type():
  return {'type': 'all_to_all_connector'}


def all_to_one_available_connector_type():
  return {'type': 'all_to_one_available_connector'}


def sum_leaf():
  return {'type': 'sum_link_leaf'}


def forward_to_any_leaf():
  return {'type': 'forward_to_any_link_leaf'}
