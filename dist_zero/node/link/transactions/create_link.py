from collections import defaultdict

from dist_zero import transaction, messages, errors, ids
from dist_zero.network_graph import NetworkGraph
from dist_zero.topology_picker import TopologyPicker

from dist_zero.node.io.transactions.send_start_subscription import SendStartSubscription
from dist_zero.node.io.transactions.receive_start_subscription import ReceiveStartSubscription


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
    controller.enlist(self._tgt, ReceiveStartSubscription, {})

    self._hello_parent = {}
    for i in range(2):
      hello_parent, kid_id = await controller.listen(type='hello_parent')
      self._hello_parent[kid_id] = hello_parent

    if set(self._hello_parent.keys()) != set(self._src['id'], self._tgt['id']):
      raise errors.InternalError("Should have received hello_parent messages from exactly the src and tgt nodes.")

    controller.node._height = max(hello_parent['kid_summary']['height'] for hello_parent in self._hello_parent.values())

    src_role = self._hello_parent[self._src['id']]['kid']
    tgt_role = self._hello_parent[self._tgt['id']]['kid']
    controller.send(
        src_role,
        messages.link.subscribe_to(target=controller.new_handle(src_role['id']), height=controller.node._height))

    left_neighbors = [src_role]
    right_neighbors = [tgt_role]
    StartLinkNode(parent=None, neighbors=(left_neighbors, right_neighbors)).run(controller)


class StartLinkNode(transaction.ParticipantRole):
  MAX_MESSAGE_RATE_PER_NODE_HZ = 200
  '''The highest message rate we would like to allow for a leaf link node (in hertz)'''

  def __init__(self, parent=None, neighbors=None):
    '''
    :param parent: If provided, this node's parent in the transaction.
    :param neighbors: If provided, a pair of lists (left_roles, right_roles) giving the lists of roles of the nodes to
      this node's immediate left and right.
    '''
    self._parent = parent
    self._neighbors = neighbors

    self._controller = None

    # Map each node to the immediate left to its start_subscription message
    self._start_subscription = None
    # Map each node to the immediate right to its subscription_started message
    self._subscription_started = None
    # Map each node to the immediate left to its subscription_edges message
    self._subscription_edges = None
    # Map each leftmost kid to the list of roles that will be sending to it
    self._left_kid_senders = None

    self._kids = {} # Map node id to the role handle for all the kids of self

    # Will be set to the total estimated message rate (in hertz) of this node's descendants.
    self._total_messages_per_second = None

    # Sometimes, the leftmost kids of the graph are spawned early.
    # If that happens, self._leftmost_kids will be a list of their roles.
    self._leftmost_kids = None

    # The NetworkGraph and TopologyPicker instances describing how the kids are arranged
    # Note that the TopologyPicker itself does not determine the first or last layers of the graph
    # and that while all but the last layer contain kids of this node, the last layer contains kids of this node's
    # right neighbors
    self._graph = None
    self._picker = None

  async def run(self, controller: 'TransactionRoleController'):
    self._controller = controller

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
    self._start_subscription = {}
    while len(self._start_subscription) < len(self._left_neighbors):
      start_subscription, subscriber_id = await self._controller.listen('start_subscription')
      self._start_subscription[subscriber_id] = start_subscription

    self._total_messages_per_second = sum(
        start_subscription['load']['messages_per_second'] for start_subscription in self._start_subscription.values())

  async def _spawn_leftmost_kids(self):
    self._leftmost_kids = []

    if self._node._height == 0:
      pass
    elif self._node._left_is_data:
      await self._spawn_leftmost_kids_for_data_node()
    else:
      await self._spawn_leftmost_kids_by_load()

  async def _send_subscription_started_to_left_neighbors(self):
    for start_subscription in self._start_subscription.values():
      subscriber = start_subscription['subscriber']
      self._controller.send(
          subscriber,
          messages.link.subscription_started(
              leftmost_kids=[self._controller.transfer_handle(kid, subscriber['id']) for kid in self._leftmost_kids]))

  async def _subscribe_to_right_neighbors(self):
    load_per_right_neighbor = messages.link.load(
        messages_per_second=self._total_messages_per_second / len(self._right_neighbors))
    for right_neighbor in self._right_neighbors:
      self._controller.send(
          right_neighbor,
          messages.link.start_subscription(
              subscriber=self._controller.new_handle(right_neighbor['id']),
              load=load_per_right_neighbor,
              kid_ids=None, # link nodes do not send their kids in the start_subscription message
          ))

    self._subscription_started = {}
    while len(self._subscription_started) < len(self._right_neighbors):
      subscription_started, right_id = await self._controller.listen('subscription_started')
      self._subscription_started[right_id] = subscription_started

  async def _send_subscription_edges(self):
    for right in self._right_neighbors:
      subscription_started = self._subscription_started[right['id']]
      self._controller.send(
          right,
          messages.link.subscription_edges(
              edges={
                  right_kid['id']: [
                      self._controller.transfer_handle(self._kids[sender_id], right_kid['id'])
                      for sender_id in self._graph.node_senders(right_kid['id'])
                  ]
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

    lefts = list(self._leftmost_kids)
    rights = [
        kid for subscription_started in self._subscription_started.values()
        for kid in subscription_started['leftmost_kids']
    ]

    self._graph = NetworkGraph()
    self._picker = TopologyPicker(
        graph=self._graph,
        lefts=[kid['id'] for kid in lefts],
        rights=[kid['id'] for kid in rights],
        max_outputs=self._system_config['LINK_NODE_MAX_RECEIVERS'],
        max_inputs=self._system_config['LINK_NODE_MAX_SENDERS'],
        name_prefix="Link")

    if len(self._picker.layers) < 2:
      raise errors.InternalError("We should have at least 2 layers")

    # Spawn kids that have not yet been created
    await self._spawn_and_await_kids(
        dict(
            node_id=node_id,
            leftmost=False,
            rightmost=(i == len(self._picker.layers) - 2),
        ) for i in range(1,
                         len(self._picker.layers) - 1) for node_id in self._picker.layers[i])

    # Inform the leftmost kids of their neighbors
    for kid in self._leftmost_kids:
      self._controller.send(
          kid,
          messages.link.set_link_neighbors(
              left_roles=self._left_kid_senders[kid['id']],
              right_roles=[
                  self._controller.transfer_handle(self._kids[receiver_id], kid['id'])
                  for reciever_id in self._graph.node_receivers(kid['id'])
              ],
          ))

    # Inform non-leftmost kids of their neighbors
    for layer in self._picker.layers[1:-1]:
      for node_id in layer:
        self._controller.send(
            self._kids[node_id],
            messages.link.set_link_neighbors(
                left_roles=[
                    self._controller.transfer_handle(self._kids[sender_id], node_id)
                    for sender_id in self._graph.node_senders(node_id)
                ],
                right_roles=[
                    self._controller.transfer_handle(self._kids[receiver_id], node_id)
                    for reciever_id in self._graph.node_receivers(node_id)
                ],
            ))

    # Since the last layer of nodes in the graph are not our kids, we do not send
    # them set_link_neighbors messages.  Their linkage information will be sent to their
    # parent via the subscription_edges message.

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

  def _spawn_kid(self, node_id=None, leftmost=False, rightmost=False):
    node_config = messages.link.new_link_node_config(
        node_id=ids.new_id('Link') if node_id is None else node_id,
        left_is_data=leftmost and self._node._left_is_data,
        right_is_data=rightmost and self._node._right_is_data,
        leaf_config=self._node._leaf_config,
        height=self._node._height - 1)

    self._controller.spawn_enlist(node_config, StartLinkNode,
                                  dict(parent=self._controller.new_handle(node_config['id'])))

    return node_config['id']

  async def _spawn_leftmost_kids_for_data_node(self):
    if len(self._start_subscription) != 1:
      raise errors.InternalError(
          "Leftmost kids can be spawned by exact match only when there is a unique left adjacent node."
          f" Got {len(self._start_subscription)}.")

    start_subscription = next(iter(self._start_subscription))

    kids = await self._spawn_and_await_kids(dict(leftmost=True) for kid_id in start_subscription['kid_ids'])
    self._leftmost_kids.extend(kids)

  def _max_message_rate_per_kid(self):
    kid_height = self._node._height - 1
    max_descendant_width = self._system_config['LINK_NODE_KIDS_WIDTH_LIMIT']**kid_height
    return StartLinkNode.MAX_MESSAGE_RATE_PER_NODE_HZ * max_descendant_width

  async def _spawn_leftmost_kids_by_load():
    '''
    Spawn the leftmost layer of kids.
    Fewer nodes leads to a more compact network.
    More nodes leads to less flux/load through each node.

    Generally, a good algorithm to pick this layer should try to minimize
    the number of kids while ensuring no kid will be overloaded.
    '''

    n_kids = int(self._total_messages_per_second // self._max_message_rate_per_kid())
    self._controller.logger.info(
        "Starting link node with {n_leftmost_kids} leftmost kids.", extra={'n_leftmost_kids': n_kids})

    kids = await self._spawn_and_await_kids(dict(leftmost=True) for i in range(n_kids))
    self._leftmost_kids.extend(kids)

  @property
  def _left_neighbors(self):
    return self._neighbors[0]

  @property
  def _right_neighbors(self):
    return self._neighbors[1]
