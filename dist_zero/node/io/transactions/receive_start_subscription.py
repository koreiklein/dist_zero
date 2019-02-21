from dist_zero import transaction, messages, errors


class ReceiveStartSubscription(transaction.ParticipantRole):
  '''
  Transaction role to prepare a (sub)tree of data nodes to receive start_subscription messages.
  '''

  def __init__(self, requester):
    self._requester = requester
    self._controller = None

  @property
  def _node(self):
    return self._controller.node

  def _subscription_started(self, leftmost_kids):
    return messages.link.subscription_started(
        leftmost_kids=leftmost_kids,
        target_intervals={kid['id']: self._node._kids.interval_json()
                          for kid in leftmost_kids})

  async def _receive_from_greater_height_source(self, subscriber):
    self._controller.send(subscriber, self._subscription_started([self._controller.new_handle(subscriber['id'])]))
    edges, sender_id = await self._controller.listen(type='subscription_edges')
    proxies = edges[self._node.id]
    if len(proxies) != 1:
      raise errors.InternalError(
          "A receiving data node subscribed to a link of greater height, but did not get a unique proxy to pair with.")
    start_subscription, _sender_id = await self._controller.listen(type='start_subscription') # It will come from proxy
    return start_subscription

  async def run(self, controller: 'TransactionRoleController'):
    self._controller = controller

    self._controller.send(
        self._requester,
        messages.io.hello_parent(
            kid=self._controller.new_handle(self._requester['id']), kid_summary=self._node._kid_summary_message()))

    start_subscription, _sender_id = await self._controller.listen(type='start_subscription')
    while start_subscription['height'] > self._node._height:
      start_subscription = await self._receive_from_greater_height_source(start_subscription['subscriber'])

    subscriber = start_subscription['subscriber']
    leftmost_kids = await self._enlist_kids_and_await_hellos()
    self._controller.send(
        subscriber,
        self._subscription_started(
            leftmost_kids=[self._controller.transfer_handle(kid, subscriber['id']) for kid in leftmost_kids]))

    # We don't actually need to do anything with these edges and only listen for this
    # message so that it isn't lost.
    await self._controller.listen(type='subscription_edges')

  async def _enlist_kids_and_await_hellos(self):
    kid_ids = set(self._node._kids)
    for kid_id in kid_ids:
      kid = self._node._kids[kid_id]
      self._controller.enlist(kid, ReceiveStartSubscription, dict(requester=self._controller.new_handle(kid_id)))

    leftmost_kids = []
    while kid_ids:
      hello_parent, kid_id = await self._controller.listen(type='hello_parent')
      kid_ids.remove(kid_id)
      leftmost_kids.append(hello_parent['kid'])

    return leftmost_kids
