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

    kid_ids = set(controller.node._kids)
    for kid_id in kid_ids:
      kid = controller.node._kids[kid_id]
      controller.enlist(kid, ReceiveStartSubscription, {})

    leftmost_kids = []
    while kid_ids:
      hello_parent, kid_id = await controller.listen(type='hello_parent')
      kid_ids.remove(kid_id)
      leftmost_kids.append(controller.transfer_handle(hello_parent['kid'], subscriber['id']))

    controller.send(subscriber, messages.link.subscription_started(leftmost_kids=leftmost_kids))

    await controller.listen(type='subscription_edges')
    # We don't actually need to do anything with these edges and only listen for this
    # message that it isn't lost.
