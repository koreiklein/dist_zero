from dist_zero import transaction, messages, errors


class ReceiveStartSubscription(transaction.ParticipantRole):
  '''
  Transaction role to prepare a (sub)tree of data nodes to receive start_subscription messages.
  '''

  def __init__(self, requester):
    self._requester = requester

  async def run(self, controller: 'TransactionRoleController'):
    controller.send(
        self._requester,
        messages.io.hello_parent(
            kid=controller.new_handle(self._requester['id']), kid_summary=controller.node._kid_summary_message()))

    await ReceiveStartSubscriptionHelper().run(controller)


class ReceiveStartSubscriptionHelper(transaction.ParticipantRole):
  async def run(self, controller: 'TransactionRoleController'):
    start_subscription, _sender_id = await controller.listen(type='start_subscription')
    subscriber = start_subscription['subscriber']
    while start_subscription['height'] > controller.node._height:
      # The subscriber has too great a height, tell it to have its kids subscribe to this node again.
      controller.send(subscriber,
                      messages.link.subscription_started(leftmost_kids=[controller.new_handle(subscriber['id'])]))
      edges, sender_id = await controller.listen(type='subscription_edges')
      proxies = edges[controller.node.id]
      if len(proxies) != 1:
        raise errors.InternalError(
            "A receiving data node subscribed to a link of greater height, but did not get a unique proxy to pair with."
        )
      proxy = proxies[0]
      start_subscription, _sender_id = await controller.listen(type='start_subscription') # It will come from proxy
      subscriber = start_subscription['subscriber']

    kids = list(controller.node._kids.values())
    for kid in kids:
      controller.enlist(kid, ReceiveStartSubscriptionHelper, {})

    controller.send(subscriber, messages.link.subscription_started(leftmost_kids=kids))

    await controller.listen(type='subscription_edges')
    # We don't actually need to do anything with these edges and only listen for this
    # message that it isn't lost.
