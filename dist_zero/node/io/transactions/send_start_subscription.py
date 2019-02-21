from collections import defaultdict

from dist_zero import transaction, messages, errors, intervals


class SendStartSubscription(transaction.ParticipantRole):
  '''
  Transaction role to have a (sub)tree of data nodes to send start_subscription messages.
  '''

  def __init__(self, parent):
    self._parent = parent

    # List of role handles of the nodes we will subscribe to.  There is often just one,
    # except in cases where the connected link is of a greater height than this node.
    self._targets = None
    self._target_height = None # The height of self._targets[-1]
    self._kid_roles = None

  @property
  def _target(self):
    return self._targets[-1]

  async def run(self, controller: 'TransactionRoleController'):
    controller.send(
        self._parent,
        messages.io.hello_parent(
            kid=controller.new_handle(self._parent['id']), kid_summary=controller.node._kid_summary_message()))

    subscribe_to, _sender_id = await controller.listen(type='subscribe_to')
    self._targets = [subscribe_to['target']]
    self._target_height = subscribe_to['height']
    while self._target_height > controller.node._height:
      await self._subscribe_to_greater_height_target(controller)

    await self._subscribe_to_same_height_target(controller)

  async def _subscribe_to_greater_height_target(self, controller):
    controller.logger.info(
        "Data node starting subscription to overly high target {target_id}", extra={'target_id': self._targets['id']})
    interval = intervals.interval_json(controller.node._source_interval)
    controller.send(
        self._target,
        messages.link.start_subscription(
            subscriber=controller.new_handle(self._target['id']),
            load=messages.link.load(messages_per_second=controller.node._estimated_messages_per_second()),
            source_interval=interval,
            # We use this node as the unique kid of itself to even out mismatched heights.
            kid_intervals=[interval]))

    subscription_started, _sender_id = await controller.listen(type='subscription_started')
    proxies = subscription_started['leftmost_kids']
    if len(proxies) != 1:
      raise errors.InternalError(
          "A sending data node subscribed to a link of greater height, but did not get a unique proxy to pair with.")
    proxy = proxies[0]

    # Inform the target to connect its proxy to this node (not one of its kids)
    # so as to balance out the mismatched heights.
    controller.send(self._target,
                    messages.link.subscription_edges(edges={
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
    my_kid_id_by_start = {controller.node._kids.left_endpoint(kid_id): kid_id for kid_id in controller.node._kids}
    for other_kid in subscription_started['leftmost_kids']:
      other_kid_start = other_target_interval[other_kid['id']][0]
      my_kid_id = my_kid_id_by_start.pop(other_kid_start, None)
      if my_kid_id is None:
        raise errors.InternalError("Mismatched adjacent leftmost kids: "
                                   "Could not find the left endpoint in my_kid_id_by_start")
      yield my_kid_id, other_kid

    if kid_id_by_start:
      raise errors.InternalError("Mismatched adjacent leftmost kids: "
                                 "Extra kids remained unmatched in kid_id_by_start")

  async def _enlist_kids_and_await_hellos(self, controller):
    for kid_id in controller.node._kids:
      controller.enlist(controller.node._kids[kid_id], SendStartSubscription,
                        dict(parent=controller.new_handle(kid_id)))

    self._kid_roles = {}
    while len(self._kid_roles) < len(controller.node._kids):
      hello_parent, kid_id = await controller.listen(type='hello_parent')
      self._kid_roles[kid_id] = hello_parent['kid']

  async def _subscribe_to_same_height_target(self, controller):
    await self._enlist_kids_and_await_hellos(controller)

    controller.logger.info("Data node starting subscription to {target_id}", extra={'target_id': self._target['id']})
    controller.send(
        self._target,
        messages.link.start_subscription(
            subscriber=controller.new_handle(self._target['id']),
            load=messages.link.load(messages_per_second=controller.node._estimated_messages_per_second()),
            source_interval=intervals.interval_json(controller.node._source_interval),
            kid_intervals=[
                intervals.interval_json(controller.node._kids.kid_interval(kid_id)) for kid_id in controller.node._kids
            ]))

    subscription_started, _sender_id = await controller.listen(type='subscription_started')

    edges = defaultdict(list)

    for my_kid_id, other_kid in self._make_matches(subscription_started):
      my_kid = self._kid_roles[my_kid_id]
      edges[other_kid['id']].append(controller.transfer_handle(my_kid, self._target['id']))
      controller.send(
          my_kid,
          messages.link.subscribe_to(
              target=controller.transfer_handle(other_kid, my_kid_id), height=controller.node._height - 1))

    controller.send(self._target, messages.link.subscription_edges(edges=edges))
