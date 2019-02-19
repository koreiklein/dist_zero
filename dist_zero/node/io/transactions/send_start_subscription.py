from collections import defaultdict

from dist_zero import transaction, messages, errors


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
    self._kid_ids = None # The kids of our kids at the 'time' we try to subscribe.
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
    controller.send(
        self._target,
        messages.link.start_subscription(
            subscriber=controller.new_handle(self._target['id']),
            load=messages.link.load(messages_per_second=controller.node._estimated_messages_per_second()),
            # We use this node as the unique kid of itself to even out mismatched heights.
            kid_ids=[controller.node.id]))

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

  async def _subscribe_to_same_height_target(self, controller):
    self._kid_ids = list(controller.node._kids)
    for kid_id in self._kid_ids:
      controller.enlist(controller.node._kids[kid_id], SendStartSubscription,
                        dict(parent=controller.new_handle(kid_id)))

    self._kid_roles = {}
    while len(self._kid_roles) < len(self._kid_ids):
      hello_parent, kid_id = await controller.listen(type='hello_parent')
      self._kid_roles[kid_id] = hello_parent['kid']

    controller.logger.info("Data node starting subscription to {target_id}", extra={'target_id': self._target['id']})
    controller.send(
        self._target,
        messages.link.start_subscription(
            subscriber=controller.new_handle(self._target['id']),
            load=messages.link.load(messages_per_second=controller.node._estimated_messages_per_second()),
            kid_ids=list(self._kid_ids)))

    subscription_started, _sender_id = await controller.listen(type='subscription_started')
    self._kids_to_match = subscription_started['leftmost_kids']

    if len(self._kids_to_match) != len(self._kid_ids):
      raise errors.InternalError(
          "Data node started a subscription but was not given the proper number of kids to connect to.",
          extra={
              'n_my_kids': len(self._kid_ids),
              'n_other_kids': len(self._kids_to_match)
          })

    edges = defaultdict(list)
    for kid_id, matched_kid in zip(self._kid_ids, self._kids_to_match):
      kid = self._kid_roles[kid_id]
      edges[matched_kid['id']].append(controller.transfer_handle(kid, self._target['id']))
      controller.send(
          kid,
          messages.link.subscribe_to(
              target=controller.transfer_handle(matched_kid, kid_id), height=controller.node._height - 1))

    controller.send(self._target, messages.link.subscription_edges(edges=edges))
