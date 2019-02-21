from collections import defaultdict

from dist_zero import transaction, messages, errors, ids, intervals

from dist_zero.node.io.transactions.send_start_subscription import SendStartSubscription
from dist_zero.node.io.transactions.receive_start_subscription import ReceiveStartSubscription

from dist_zero.node.link import manager


class CreateLink(transaction.ParticipantRole):
  '''
  Used to start up the root `LinkNode` of a link (sub)tree.
  Connects that link to source and target root nodes.
  '''

  def __init__(self, src, tgt):
    '''
    :param src: The :ref:`handle` of the root `DataNode` of the source data tree.
    :type src: :ref:`handle`
    :param tgt: The :ref:`handle` of the root `DataNode` of the target data tree.
    :type tgt: :ref:`handle`
    '''
    self._src = src
    self._tgt = tgt

    # A dict mapping the ids of the sources and targets of self to their hello_parent messages.
    self._hello_parent = None

  async def run(self, controller: 'TransactionRoleController'):
    controller.enlist(self._src, SendStartSubscription, dict(parent=controller.new_handle(self._src['id'])))
    controller.enlist(self._tgt, ReceiveStartSubscription, dict(requester=controller.new_handle(self._tgt['id'])))

    self._hello_parent = {}
    for i in range(2):
      hello_parent, kid_id = await controller.listen(type='hello_parent')
      self._hello_parent[kid_id] = hello_parent

    if set(self._hello_parent.keys()) != set([self._src['id'], self._tgt['id']]):
      raise errors.InternalError("Should have received hello_parent messages from exactly the src and tgt nodes.")

    controller.node._height = max(hello_parent['kid_summary']['height'] for hello_parent in self._hello_parent.values())

    src_role = self._hello_parent[self._src['id']]['kid']
    tgt_role = self._hello_parent[self._tgt['id']]['kid']
    controller.send(
        src_role,
        messages.link.subscribe_to(target=controller.new_handle(src_role['id']), height=controller.node._height))

    left_neighbors = [src_role]
    right_neighbors = [tgt_role]
    await StartLinkNode(
        parent=None,
        source_interval=intervals.interval_json([intervals.Min, intervals.Max]),
        target_interval=intervals.interval_json([intervals.Min, intervals.Max]),
        neighbors=(left_neighbors, right_neighbors),
    ).run(controller)


class StartLinkNode(transaction.ParticipantRole):
  MAX_MESSAGE_RATE_PER_NODE_HZ = 200
  '''The highest message rate we would like to allow for a leaf link node (in hertz)'''

  def __init__(self, parent, source_interval, target_interval, neighbors=None):
    '''
    :param parent: If provided, this node's parent in the transaction.
    :param neighbors: If provided, a pair of lists (left_roles, right_roles) giving the lists of roles of the nodes to
      this node's immediate left and right.
    '''
    self._parent = parent
    self._neighbors = neighbors

    self._controller = None

    self._source_interval = intervals.parse_interval(source_interval)
    self._target_interval = intervals.parse_interval(target_interval)

    self._rectangle = {} # Map each kid id to a pair (source_interval, target_interval)

    # list of pairs (source_interval, start_subscription) ordered by source_interval[0]
    # They should form a partition of self._node._source_interval
    self._start_subscription_columns = None
    # Map each node to the immediate right to its subscription_started message
    self._subscription_started = None
    # Map each node to the immediate left to its subscription_edges message
    self._subscription_edges = None
    # Map each leftmost kid to the list of roles that will be sending to it
    self._left_kid_senders = None

    # Maps blocks in self._manager to their associated node_id
    self._block_to_node_id = None

    self._kids = {} # Map node id to the role handle for all the kids of self

    # Will be set to the total estimated message rate (in hertz) of this node's descendants.
    self._total_messages_per_second = None

    # Sometimes, the leftmost kids of the graph are spawned early.
    # If that happens, self._leftmost_kids will be a list of their roles.
    self._leftmost_kids = None

  @property
  def _manager(self):
    return self._node._manager

  @_manager.setter
  def _manager(self, value):
    self._node._manager = value

  async def run(self, controller: 'TransactionRoleController'):
    self._controller = controller
    self._node._source_interval = self._source_interval
    self._node._target_interval = self._target_interval

    # Each of the stages of the process of creating a link is factored into
    # one of the below method calls for readability, better debugging and improved stack traces.

    await self._send_hello_to_parent()
    await self._receive_neighbors()
    await self._receive_all_start_subscriptions()
    await self._spawn_leftmost_kids()
    await self._send_subscription_started_to_left_neighbors()
    await self._subscribe_to_right_neighbors()
    await self._receive_subscription_edges()
    await self._spawn_and_connect_graph()
    await self._send_subscription_edges()
    await self._receive_link_started_from_all_kids()
    await self._send_link_started_to_parent()

    self._node.maybe_start_leaf()

  async def _send_hello_to_parent(self):
    if self._parent is not None:
      self._controller.node._parent = self._controller.role_handle_to_node_handle(self._parent)
      self._controller.send(self._parent,
                            messages.link.hello_link_parent(self._controller.new_handle(self._parent['id'])))

  async def _receive_neighbors(self):
    if self._neighbors is None:
      set_link_neighbors, _sender_id = await self._controller.listen(type='set_link_neighbors')
      self._neighbors = (set_link_neighbors['left_roles'], set_link_neighbors['right_roles'])

  async def _receive_all_start_subscriptions(self):
    start_subscription_by_subscriber_id = {}
    while len(self._start_subscription) < len(self._left_neighbors):
      start_subscription, subscriber_id = await self._controller.listen('start_subscription')
      start_subscription_by_subscriber_id[subscriber_id] = start_subscription

    self._start_subscription_columns = list(
        sorted(((intervals.parse_interval(start_subscription['source_interval']), start_subscription)
                for start_subscription in start_subscription_by_subscriber_id.items()),
               key=lambda pair: pair[0][0]))

    self._validate_start_subscription_columns()

    self._total_messages_per_second = sum(start_subscription['load']['messages_per_second']
                                          for start_subscription in start_subscription_by_subscriber_id.values())

  async def _spawn_leftmost_kids(self):
    self._leftmost_kids = []

    if self._node._height == 0:
      pass
    elif self._node._left_is_data:
      await self._spawn_leftmost_kids_for_data_node()
    else:
      await self._spawn_leftmost_kids_by_load()

  async def _send_subscription_started_to_left_neighbors(self):
    for source_interval, start_subscription in self._start_subscription_columns:
      subscriber = start_subscription['subscriber']
      target_intervals = {}
      leftmost_kids = []
      for kid in self._leftmost_kids:
        kid_id = kid['id']
        kid_source_interval, kid_target_interval = self._rectangle[kid_id]
        if intervals.is_subinterval(kid_source_interval, source_interval):
          leftmost_kids.append(self._controller.transfer_handle(kid, subscriber['id']))
          target_intervals[kid_id] = intervals.interval_json(kid_target_interval)

      self._controller.send(
          subscriber, messages.link.subscription_started(
              leftmost_kids=leftmost_kids, target_intervals=target_intervals))

  async def _subscribe_to_right_neighbors(self):
    load_per_right_neighbor = messages.link.load(
        messages_per_second=self._total_messages_per_second / len(self._right_neighbors))
    for right_neighbor in self._right_neighbors:
      self._controller.send(
          right_neighbor,
          messages.link.start_subscription(
              subscriber=self._controller.new_handle(right_neighbor['id']),
              load=load_per_right_neighbor,
              source_interval=intervals.interval_json(self._source_interval),
              kid_intervals=None, # link nodes do not send their kids in the start_subscription message
          ))

    self._subscription_started = {}
    while len(self._subscription_started) < len(self._right_neighbors):
      subscription_started, right_id = await self._controller.listen('subscription_started')
      self._subscription_started[right_id] = subscription_started

  async def _send_subscription_edges(self):
    for right in self._right_neighbors:
      subscription_started = self._subscription_started[right['id']]
      senders = lambda kid_id: (self._kids[self._block_to_node_id[below]] for below in self._manager.target_block(kid_id).below)
      self._controller.send(
          right,
          messages.link.subscription_edges(
              edges={
                  right_kid['id']:
                  [self._controller.transfer_handle(sender, right_kid['id']) for sender in senders(right_kid['id'])]
                  for right_kid in subscription_started['leftmost_kids']
              }))

  async def _receive_link_started_from_all_kids(self):
    missing_link_started_ids = set(self._kids.keys())
    while missing_link_started_ids:
      _msg, kid_id = await self._controller.listen(type='link_started')
      missing_link_started_ids.remove(kid_id)

  async def _send_link_started_to_parent(self):
    if self._parent is not None:
      self._controller.send(self._parent, messages.link.link_started())

  async def _receive_subscription_edges(self):
    self._subscription_edges = {}
    while len(self._subscription_edges) < len(self._left_neighbors):
      subscription_edges, sender_id = await self._controller.listen(type='subscription_edges')
      self._subscription_edges[sender_id] = subscription_edges

    self._left_kid_senders = defaultdict(list)
    for subscription_edges in self._subscription_edges.values():
      for left_kid_id, senders in subscription_edges['edges'].items():
        self._left_kid_senders[left_kid_id].extend(senders)

  async def _spawn_and_connect_graph(self):
    '''
    First, determine the entire graph of kids (note that the leftmost kids have already been determined and spawned)
    Next, spawn all the nodes in the graph that have not yet been spawned.
    Finally, inform all the nodes in the graph of their neighbors.
    '''
    if self._node._height == 0:
      return

    left_ids = [kid['id'] for kid in self._leftmost_kids]
    target_object_intervals = [(kid['id'], intervals.parse_interval(target_intervals[kid['id']]))
                               for subscription_started in self._subscription_started.values()
                               for target_intervals in [subscription_started['target_intervals']]
                               for kid in subscription_started['leftmost_kids']]
    right_ids = [kid_id for kid_id, _interval in target_object_intervals]

    self._manager = manager.LinkGraphManager(
        source_object_intervals=[(kid_id, self._rectangle[kid_id][0]) for kid_id in left_ids],
        target_object_intervals=target_object_intervals,
        constraints=manager.Constraints(
            max_above=self._system_config['LINK_NODE_MAX_RECEIVERS'],
            max_below=self._system_config['LINK_NODE_MAX_SENDERS'],
        ))

    self._block_to_node_id = {}
    for kid_id in left_ids:
      self._block_to_node_id[self._manager.source_block[kid_id]] = kid_id
    for kid in right_ids:
      self._block_to_node_id[self._manager.target_block[kid_id]] = kid_id

    kid_args = []
    for block in self._manager.internal_blocks():
      source_interval, target_interval = LinkGraphManager.block_rectangle(block)

      node_id = ids.new_id('LinkNode_internal')

      self._block_to_node_id[block] = node_id
      kid_args.append(
          dict(
              node_id=node_id,
              rightmost=block.is_target,
              source_interval=source_interval,
              target_interval=target_interval))

    await self._spawn_and_await_kids(kid_args)

    # Inform the leftmost kids of their neighbors
    for kid in self._leftmost_kids:
      kid_id = kid['id']
      receivers = (self._kids[self._block_to_node_id[block]] for block in self._manager.source_block(kid_id).above)
      senders = self._left_kid_senders[kid_id]
      self._controller.send(
          kid,
          messages.link.set_link_neighbors(
              left_roles=[self._controller.transfer_handle(sender, kid_id) for sender in senders],
              right_roles=[self._controller.transfer_handle(receiver, kid_id) for receiver in receivers]))

    # Inform the internal kids of their neighbors
    for block in self._manager.internal_blocks():
      node_id = self._block_to_node_id[block]
      senders = (self._kids[self._block_to_node_id[below]] for below in block.below)
      receivers = (self._kids[self._block_to_node_id[above]] for above in block.above)
      self._controller.send(
          self._kids[node_id],
          messages.link.set_link_neighbors(
              left_roles=[self._controller.transfer_handle(sender, node_id) for sender in senders],
              right_roles=[self._controller.transfer_handle(receiver, node_id) for receiver in receivers],
          ))

    # Do not inform the manager target nodes of their neighbors, as they have a separate parent.
    # Their linkage information will be sent to their parent via the subscription_edges message
    # and forwarded to them by their parent in its own set_link_neighbors message.

  @property
  def _system_config(self):
    return self._controller.node.system_config

  @property
  def _node(self):
    return self._controller.node

  async def _spawn_and_await_kids(self, spawn_kid_args_list):
    node_ids = set()
    for args in spawn_kid_args_list:
      node_ids.add(self._spawn_kid(**args))

    result = []
    while node_ids:
      hello_link_parent, kid_id = await self._controller.listen(type='hello_link_parent')
      kid = hello_link_parent['kid']
      self._kids[kid['id']] = kid
      result.append(kid)

    return result

  def _spawn_kid(self, source_interval, target_interval, node_id, leftmost=False, rightmost=False):
    node_config = messages.link.new_link_node_config(
        node_id=node_id,
        left_is_data=leftmost and self._node._left_is_data,
        right_is_data=rightmost and self._node._right_is_data,
        leaf_config=self._node._leaf_config,
        height=self._node._height - 1)

    self._rectangle[node_id] = (source_interval, target_interval)

    self._controller.spawn_enlist(
        node_config, StartLinkNode,
        dict(
            parent=self._controller.new_handle(node_id),
            source_interval=intervals.interval_json(source_interval),
            target_interval=intervals.interval_json(target_interval),
        ))

    return node_config['id']

  async def _spawn_leftmost_kids_for_data_node(self):
    if len(self._start_subscription_columns) != 1:
      raise errors.InternalError(
          "Leftmost kids can be spawned by exact match only when there is a unique left adjacent node."
          f" Got {len(self._start_subscription_columns)}.")

    _interval, start_subscription = self._start_subscription_columns[0]

    target_interval = self._node._target_interval
    kids = await self._spawn_and_await_kids(
        dict(
            source_interval=intervals.parse_interval(source_interval_json),
            target_interval=target_interval,
            leftmost=True,
            node_id=ids.new_id('LinkNode_data_leftmost'),
        ) for source_interval_json in start_subscription['kid_intervals'])
    self._leftmost_kids.extend(kids)

  def _max_message_rate_per_kid(self):
    kid_height = self._node._height - 1
    max_descendant_width = self._system_config['LINK_NODE_KIDS_WIDTH_LIMIT']**kid_height
    return StartLinkNode.MAX_MESSAGE_RATE_PER_NODE_HZ * max_descendant_width

  def _partition_interval(self, interval, n_parts):
    start, stop = interval

  def _validate_start_subscription_columns(self):
    cur_source_key = self._node._source_interval[0]
    for (source_start, source_stop), start_subscription in self._start_subscription_columns:
      if source_start != cur_source_key:
        raise errors.InternalError("start_subscription messages to StartLinkNode did not form a valid partition")
      cur_source_key = source_stop

    if cur_source_key != self._node._source_interval[1]:
      raise errors.InternalError(
          "start_subscription messages to StartLinkNode did not form a valid partition at the parent's right endpoint")

  async def _spawn_leftmost_kids_by_load():
    '''
    Spawn the leftmost layer of kids.
    Fewer nodes leads to a more compact network.
    More nodes leads to less flux/load through each node.

    Generally, a good algorithm to pick this layer should try to minimize
    the number of kids while ensuring no kid will be overloaded.
    '''

    kids = await self._spawn_and_await_kids(
        dict(
            leftmost=True,
            node_id=ids.new_id('LinkNode_internal_leftmost'),
            source_interval=source_interval,
            target_interval=self._target_interval,
        ) for source_interval, start_subscription in self._start_subscription_columns)

    self._controller.logger.info(
        "Starting link node with {n_leftmost_kids} leftmost kids.", extra={'n_leftmost_kids': len(kids)})

    self._leftmost_kids.extend(kids)

  @property
  def _left_neighbors(self):
    return self._neighbors[0]

  @property
  def _right_neighbors(self):
    return self._neighbors[1]


def _midway(left, right):
  '''
  Calculate some key between left and right.
  The choice of key is rather arbitrary when either argument is an infinity.
  '''
  if left == intervals.Min:
    if right == intervals.Max:
      return 0.0
    else:
      return right - 1.0
  elif right == intervals.Max:
    return left + 1.0
  else:
    return (left + right) / 2.0
