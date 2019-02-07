def new_link_node_config(node_id, left_is_data, right_is_data, leaf_config, height=None):
  '''A config for a `LinkNode`.'''
  return {
      'type': 'Link',
      'id': node_id,
      'height': height,
      'left_is_data': left_is_data,
      'right_is_data': right_is_data,
      'leaf_config': leaf_config,
  }


def load(messages_per_second):
  '''
  Return a load object to describe the expected load along a subscription.

  :param float messages_per_second: An estimate of the total messages per second through the subscription.
  '''
  return {'messages_per_second': messages_per_second}


def start_subscription(subscriber, load, kids=None):
  '''
  Request to start a subscription between the sender and the receiver.
  Only the node to the left (the node sending the data) should send start_subscription,
  and only the node to the right (the node receiving the data) should receive start_subscription.

  :param object subscriber: The role handle of the node to the left that would like to subscribe.
  :param load: Describes the total load the sender anticipates will be sent over this subscription.
  :param list kid_ids: If provided, gives the exact list of kid node ids of the sender.
  '''
  return {'type': 'start_subscription', 'subscriber': subscriber, 'load': load, 'kid_ids': kid_ids}


def subscription_started(leftmost_kids):
  '''
  Sent in response to start_subscription by the node to the right to indicate that
  the subscription from the node to the left has started.

  :param list leftmost_kids: A list of role handles of the kids of this node.
  '''
  return {'type': 'subscription_started', 'leftmost_kids': leftmost_kids}


def subscription_edges(edges):
  '''
  Sent by the left node of a subscription to the right node to indicate which of the left node's
  rightmost kids will be subscribing to which of the right node's leftmost kids.

  :param dict[str,list] edges: An edge map.  For each kid in the 'leftmost_kids' argument to the
    preceeding subscription_started message, it should map that kid's node_id to the list of
    role handles the sender has assigned to send to that kid.
  '''
  return {'type': 'subscription_edges', 'edges': edges}


def subscribe_to(target, height):
  '''
  Indicates to the SendStartSubscription role which target node it should send the start_subscription message.

  :param object target: The role handle of the node to subscribe to.
  :param int height: The height of the ``target`` node.
  '''
  return {'type': 'subscribe_to', 'target': target}


def set_link_neighbors(left_roles, right_roles):
  '''
  Sent by a `LinkNode` instance's parent to inform the link which nodes will be to its left and right.

  :param list left_roles: The list of handles of the roles of nodes to the immediate left of this one.
  :param list right_roles: The list of handles of the roles of nodes to the immediate right of this one.
  '''
  return {'type': 'set_link_neighbors', 'left_roles': left_roles, 'right_roles': right_roles}


def hello_link_parent(kid):
  '''
  Sent by link nodes to their parents immediately after starting to indicate that they are up.

  :param kid: The handle for the role of this kid.
  '''
  return {'type': 'hello_link_parent', 'kid': kid}


def link_started():
  '''
  Sent by a link to its parent once it's finished spawning and connecting its entire graph.
  This message marks the end of a child's role in a `CreateLink` transaction.
  '''
  return {'type': 'link_started'}


# ============================================================================
# ============================================================================
# ============================================================================
# ============================================================================
# FIXME(KK): Old Code Below!  Please remove it!!
# ============================================================================
# ============================================================================
# ============================================================================
# ============================================================================


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
