from collections import defaultdict

from dist_zero import transaction, messages, errors, intervals


class SendStartSubscription(transaction.ParticipantRole):
  '''
  Transaction role to have a (sub)tree of data nodes to send start_subscription messages.
  '''

  def __init__(self, parent, link_key):
    self._parent = parent
    self._link_key = link_key

    # List of role handles of the nodes we will subscribe to.  There is often just one,
    # except in cases where the connected link is of a greater height than this node.
    self._targets = None
    self._target_height = None # The height of self._targets[-1]
    self._kid_roles = None
    self._controller = None

  @property
  def _target(self):
    return self._targets[-1]

  async def run(self, controller: 'TransactionRoleController'):
    self._controller = controller
    self._controller.send(
        self._parent,
        messages.io.hello_parent(
            kid=self._controller.new_handle(self._parent['id']), kid_summary=self._node._kid_summary_message()))

    subscribe_to, _sender_id = await self._controller.listen(type='subscribe_to')
    self._targets = [subscribe_to['target']]
    self._target_height = subscribe_to['height']
    while self._target_height > self._node._height:
      await self._subscribe_to_greater_height_target()

    await self._subscribe_to_same_height_target()

  async def _subscribe_to_greater_height_target(self):
    self._controller.logger.info(
        "Data node starting subscription to overly high target {target_id}", extra={'target_id': self._targets['id']})
    interval = intervals.interval_json(self._node._source_interval)
    self._controller.send(
        self._target,
        messages.link.start_subscription(
            subscriber=self._controller.new_handle(self._target['id']),
            link_key=self._link_key,
            load=messages.link.load(messages_per_second=self._node._estimated_messages_per_second()),
            height=self._node._height,
            source_interval=interval,
            kid_intervals=[interval], # We use this node as the unique kid of itself to even out mismatched heights.
        ))

    subscription_started, _sender_id = await self._controller.listen(type='subscription_started')
    self._validate_subscription_started(subscription_started)

    proxies = subscription_started['leftmost_kids']
    if len(proxies) != 1:
      raise errors.InternalError(
          "A sending data node subscribed to a link of greater height, but did not get a unique proxy to pair with.")
    proxy = proxies[0]

    # Inform the target to connect its proxy to this node (not one of its kids)
    # so as to balance out the mismatched heights.
    self._controller.send(
        self._target, messages.link.subscription_edges(edges={
            proxy['id']: [self.new_handle(self._target['id'])],
        }))

    self._targets.append(proxy)
    self._target_height -= 1

  def _make_matches(self, subscription_started):
    '''
    yield pairs (my_kid_id, other_kid_handle)
    that form a bijection between the kids of this node and the kids in ``subscription_started``.
    Kids should be matched by the start point.
    '''
    other_target_interval = subscription_started['target_intervals']
    my_kid_id_by_start = {self._node._kids.left_endpoint(kid_id): kid_id for kid_id in self._node._kids}
    for other_kid in subscription_started['leftmost_kids']:
      other_kid_start = other_target_interval[other_kid['id']][0]
      my_kid_id = my_kid_id_by_start.pop(other_kid_start, None)
      if my_kid_id is None:
        raise errors.InternalError("Mismatched adjacent leftmost kids: "
                                   "Could not find the left endpoint in my_kid_id_by_start")
      yield my_kid_id, other_kid

    if my_kid_id_by_start:
      raise errors.InternalError("Mismatched adjacent leftmost kids: "
                                 "Extra kids remained unmatched in my_kid_id_by_start")

  async def _enlist_kids_and_await_hellos(self):
    for kid_id in self._node._kids:
      self._controller.enlist(self._node._kids[kid_id], SendStartSubscription,
                              dict(parent=self._controller.new_handle(kid_id), link_key=self._link_key))

    self._kid_roles = {}
    while len(self._kid_roles) < len(self._node._kids):
      hello_parent, kid_id = await self._controller.listen(type='hello_parent')
      self._kid_roles[kid_id] = hello_parent['kid']

  @property
  def _node(self):
    return self._controller.node

  def _validate_subscription_started(self, subscription_started):
    if subscription_started['link_key'] != self._link_key:
      raise errors.InternalError("Mismatched link keys")

  async def _subscribe_to_same_height_target(self):
    await self._enlist_kids_and_await_hellos()

    self._controller.logger.info(
        "Data node starting subscription to {target_id}", extra={'target_id': self._target['id']})
    self._controller.send(
        self._target,
        messages.link.start_subscription(
            subscriber=self._controller.new_handle(self._target['id']),
            link_key=self._link_key,
            load=messages.link.load(messages_per_second=self._node._estimated_messages_per_second()),
            height=self._node._height,
            source_interval=self._node._interval_json(),
            kid_intervals=[
                intervals.interval_json(self._node._kids.kid_interval(kid_id)) for kid_id in self._node._kids
            ]))

    subscription_started, _sender_id = await self._controller.listen(type='subscription_started')
    self._validate_subscription_started(subscription_started)

    edges = defaultdict(list)

    for my_kid_id, other_kid in self._make_matches(subscription_started):
      my_kid = self._kid_roles[my_kid_id]
      edges[other_kid['id']].append(self._controller.transfer_handle(my_kid, self._target['id']))
      self._controller.send(
          my_kid,
          messages.link.subscribe_to(
              target=self._controller.transfer_handle(other_kid, my_kid_id), height=self._node._height - 1))

    self._controller.send(self._target, messages.link.subscription_edges(edges=edges))
