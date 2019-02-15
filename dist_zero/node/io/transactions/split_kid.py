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

    old_kid_start, old_kid_stop = controller.node._kid_to_interval[self._kid['id']]

    new_id = ids.new_id('DataNode')
    node_config = messages.io.data_node_config(
        node_id=new_id,
        parent=controller.node.new_handle(new_id),
        variant=controller.node._variant,
        leaf_config=controller.node._leaf_config,
        height=controller.node._height - 1)
    controller.spawn_enlist(
        node_config,
        helpers.Absorber,
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
    controller.node._kid_summaries[sender_id] = finished_absorbing['summary']

    a, b = controller.node._kid_to_interval[self._kid_id]
    kid_index = controller.node._kid_intervals.index([a, b, self._kid_id])

    controller.node._kid_intervals[kid_index][1] = start
    controller.node._kid_to_interval[self._kid_id][1] = start
    controller.node._kid_intervals.add([start, stop, new_id])
    controller.node._kid_to_interval[new_id] = [start, stop]

    controller.node._kids[new_id] = controller.role_handle_to_node_handle(new)

    finished_splitting, sender_id = await controller.listen(type='finished_splitting')
    controller.node._kid_summaries[sender_id] = finished_splitting['summary']


class SplitNode(transaction.ParticipantRole):
  def __init__(self, parent, absorber):
    self._parent = parent
    self._absorber = absorber

  async def run(self, controller: 'TransactionRoleController'):
    if controller.node._height == 0:
      await self._zero_height_split(controller)
    else:
      await self._nonzero_height_split(controller)

    controller.send(self._parent, messages.io.finished_splitting(summary=controller.node._kid_summary_message()))

  async def _nonzero_height_split(self, controller: 'TransactionRoleController'):
    # FIXME(KK): Figure out what to do about this case.
    raise RuntimeError("Not Yet Implemented")

  async def _nonzero_height_split(self, controller: 'TransactionRoleController'):
    n_kids = len(controller.node._kid_intervals)
    n_to_keep = n_kids // 2
    leaving_kid_ids = [kid_id for start, stop, kid_id in controller.node._kid_intervals[n_to_keep:]]
    controller.send(self._absorber, messages.io.absorb_these_kids(leaving_kid_ids))

    for kid_id in leaving_kid_ids:
      controller.enlist(
          controller.node._kids[kid_id], helpers.FosterChild,
          dict(old_parent=controller.new_handle(kid_id), new_parent=controller.transfer_handle(self._absorber, kid_id)))

    kid_ids = set(leaving_kid_ids)
    while kid_ids:
      _goodbye_parent, kid_id = await controller.listen(type='goodbye_parent')
      kid_ids.remove(kid_id)

    controller.node._interval[1] = controller.node._kid_intervals[n_to_keep][0]
    for kid_id in leaving_kid_ids:
      controller.node._kid_summaries.pop(kid_id, None)
      controller.node._kids.pop(kid_id)
      start, stop = controller.node._kid_to_interval.pop(kid_id)
      controller.node._kid_intervals.remove([start, stop, kid_id])
