from dist_zero import transaction, messages, errors


class SendStartSubscription(transaction.ParticipantRole):
  '''
  Transaction role to have a (sub)tree of data nodes to send start_subscription messages.
  '''

  def __init__(self, parent):
    self._parent = parent

    self._target = None # Role handle of the node we will subscribe to
    self._kid_ids = None # The kids of our kids at the 'time' we try to subscribe.
    self._kid_roles = None

  async def run(self, controller: 'TransactionRoleController'):
    controller.send(
        messages.io.hello_parent(
            kid=controller.new_handle(self._parent['id']), kid_summary=controller.node._kid_summary_message()))

    subscribe_to, _sender_id = await controller.listen(type='subscribe_to')
    self._target = subscribe_to['target']

    self._kid_ids = list(controller.node._kids.keys())
    for kid in controller.node._kids.values():
      controller.enlist(kid, SendStartSubscription, dict(parent=controller.new_handle(kid['id'])))

    remaining_kid_ids = set(self._kid_ids)
    self._kid_roles = {}
    while remaining_kid_ids:
      hello_parent, kid_id = await controller.listen(type='hello_parent')
      remaining_kid_ids.remove(kid_id)
      self._kid_roles[kid_id] = hello_parent['kid']

    controller.logger.info("Data node starting subscription to {target_id}", extra={'target_id': self._target['id']})
    controller.send(
        self._target,
        messages.link.start_subscription(
            subscriber=controller.new_handle(self._target['id']),
            load=messages.link.load(messages_per_second=controller.node._estimated_messages_per_second()),
            kids=list(self._kid_ids)))

    subscription_started, _sender_id = controller.listen(type='subscription_started')
    self._kids_to_match = subscription_started['leftmost_kids']

    if len(self._kids_to_match) != len(self._kid_ids):
      raise errors.InternalError(
          "Data node started a subscription but was not given the proper number of kids to connect to.",
          extra={
              'n_my_kids': len(self._kid_ids),
              'n_other_kids': len(self._kids_to_match)
          })

      for kid_id, matched_kid_role_handle in zip(self._kid_ids, self._kids_to_match):
        kid = self._kid_roles[kid_id]
        controller.send(kid, messages.link.subscribe_to(controller.transfer_handle(matched_kid_role_handle, kid_id)))
