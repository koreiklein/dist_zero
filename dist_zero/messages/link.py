def link_node_config(node_id, left_is_data, right_is_data, link_key, height=None):
  '''A config for a `LinkNode`.'''
  return {
      'type': 'LinkNode',
      'id': node_id,
      'height': height,
      'left_is_data': left_is_data,
      'right_is_data': right_is_data,
      'link_key': link_key,
  }


def load(messages_per_second):
  '''
  Return a load object to describe the expected load along a subscription.

  :param float messages_per_second: An estimate of the total messages per second through the subscription.
  '''
  return {'messages_per_second': messages_per_second}


def start_subscription(subscriber, link_key, load, height, source_interval, kid_intervals=None):
  '''
  Request to start a subscription between the sender and the receiver.
  Only the node to the left (the node sending the data) should send start_subscription,
  and only the node to the right (the node receiving the data) should receive start_subscription.

  :param object subscriber: The role handle of the node to the left that would like to subscribe.
  :param str link_key: The key identifying the link this subscription is a part of
  :param height: The height of the subscriber.
  :param load: Describes the total load the sender anticipates will be sent over this subscription.
  :param tuple source_interval: A pair of keys giving the interval that the subscriber will send from.
  :param list kid_intervals: If provided, gives the exact list of intervals managed by each kid of the sender.
  '''
  if kid_intervals:
    for x in kid_intervals:
      if x[0] is None or x[1] is None:
        import ipdb
        ipdb.set_trace()
  return {
      'type': 'start_subscription',
      'subscriber': subscriber,
      'link_key': link_key,
      'height': height,
      'load': load,
      'source_interval': source_interval,
      'kid_intervals': kid_intervals
  }


def subscription_started(leftmost_kids, link_key, target_intervals, source_intervals=None):
  '''
  Sent in response to start_subscription by the node to the right to indicate that
  the subscription from the node to the left has started.

  :param list leftmost_kids: A list of role handles of the kids of this node.
  :param str link_key: The key identifying the link this subscription is a part of
  :param dict[str, tuple] target_intervals: A dictionary mapping each kid_id in ``leftmost_kids`` to its target interval
  :param dict[str, tuple] source_intervals: When sent to a data node, this should by a dictionary mapping
    each kid_id in the ``leftmost_kids`` to its source interval.  Otherwise, it can be `None`.
  '''
  return {
      'type': 'subscription_started',
      'leftmost_kids': leftmost_kids,
      'link_key': link_key,
      'target_intervals': target_intervals,
      'source_intervals': source_intervals
  }


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
  return {'type': 'subscribe_to', 'target': target, 'height': height}


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
