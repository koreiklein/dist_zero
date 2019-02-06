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

    ReceiveStartSubscriptionHelper().run(controller)


class ReceiveStartSubscriptionHelper(transaction.ParticipantRole):
  async def run(self, controller: 'TransactionRoleController'):
    start_subscription, _sender_id = await controller.listen(type='start_subscription')
    subscriber = start_subscription['subscriber']

    kids = list(controller.node._kids.values())
    for kid in kids:
      controller.enlist(kid, ReceiveStartSubscriptionHelper, {})

    controller.send(subscriber, messages.link.subscription_started(leftmost_kids=kids))
