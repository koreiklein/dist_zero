from dist_zero import transaction, messages, ids, errors, infinity

from . import helpers


class SplitKid(transaction.OriginatorRole):
  def __init__(self, kid_id):
    self._kid_id = kid_id

    self._kid = None

  async def run(self, controller: 'TransactionRoleController'):
    if controller.node._height == 0:
      raise errors.InternalError("height 0 DataNode instances can not split their kids")

    self._kid = controller.node._kids.get(self._kid_id, None)
    if self._kid is None:
      controller.logger.info(
          "Canceling SplitKid transaction because the kid was not present when the transaction started.")
      return

    old_kid_stop = controller.node._kids.right_endpoint(self._kid_id)

    new_id = ids.new_id('DataNode')
    node_config = messages.io.data_node_config(
        node_id=new_id,
        parent=controller.node.new_handle(new_id),
        variant=controller.node._variant,
        leaf_config=controller.node._leaf_config,
        height=controller.node._height - 1)
    controller.spawn_enlist(
        node_config,
        helpers.NewAbsorber,
        dict(
            parent=controller.new_handle(new_id),
            # Start with an empty interval, the left side will grow to the left
            interval=infinity.interval_json([old_kid_stop, old_kid_stop])))
    hello_parent, _sender_id = await controller.listen(type='hello_parent')
    new = hello_parent['kid']

    controller.enlist(
        self._kid, SplitNode,
        dict(
            absorber=controller.transfer_handle(new, self._kid_id),
            parent=controller.new_handle(self._kid_id),
        ))

    finished_absorbing, sender_id = await controller.listen(type='finished_absorbing')
    start, stop = infinity.parse_interval(finished_absorbing['new_interval'])
    controller.node._kids.set_summary(sender_id, finished_absorbing['summary'])

    finished_splitting, sender_id = await controller.listen(type='finished_splitting')
    controller.node._kids.split(
        kid_id=self._kid_id,
        mid=start,
        new_kid=controller.role_handle_to_node_handle(new),
        new_summary=finished_splitting['summary'])


class SplitNode(transaction.ParticipantRole):
  def __init__(self, parent, absorber):
    self._parent = parent
    self._absorber = absorber

  async def run(self, controller: 'TransactionRoleController'):
    if controller.node._height == 0 and controller.node._leaf_config['interval_type'] != 'interval':
      raise errors.InternalError(f"Unable to split a height 0 Node with interval_type \"{interval_type}\" != interval")
    mid, leaving_kids = controller.node._kids.shrink_right()
    leaving_kid_ids = [kid['id'] for kid in leaving_kids]

    controller.send(self._absorber,
                    messages.io.absorb_these_kids(kid_ids=leaving_kid_ids, left_endpoint=infinity.key_to_json(mid)))

    for kid in leaving_kids:
      controller.enlist(
          kid, helpers.FosterChild,
          dict(
              old_parent=controller.new_handle(kid['id']),
              new_parent=controller.transfer_handle(self._absorber, kid['id'])))

    kid_ids = set(leaving_kid_ids)
    while kid_ids:
      _goodbye_parent, kid_id = await controller.listen(type='goodbye_parent')
      kid_ids.remove(kid_id)

    controller.send(self._parent, messages.io.finished_splitting(summary=controller.node._kid_summary_message()))
