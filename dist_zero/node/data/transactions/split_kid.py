from dist_zero import transaction, messages, ids, errors, intervals

from . import helpers


class SplitKid(transaction.OriginatorRole):
  '''Split a specific kid of this node.'''

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
    controller.logger.info("Splitting node {new_id} from {cur_node_id}", extra={'new_id': new_id})
    node_config = messages.data.data_node_config(
        node_id=new_id,
        parent=controller.node.new_handle(new_id),
        dataset_program_config=controller.node._dataset_program_config,
        height=controller.node._height - 1)
    controller.spawn_enlist(
        node_config,
        helpers.NewAbsorber,
        dict(
            parent=controller.new_handle(new_id),
            # Start with an empty interval, the left side will grow to the left
            interval=intervals.interval_json([old_kid_stop, old_kid_stop])))
    hello_parent, _sender_id = await controller.listen(type='hello_parent')
    new = hello_parent['kid']
    controller.logger.debug("Got hello from new kid")

    controller.enlist(
        self._kid, SplitNode,
        dict(
            absorber=controller.transfer_handle(new, self._kid_id),
            parent=controller.new_handle(self._kid_id),
        ))

    finished_absorbing, sender_id = await controller.listen(type='finished_absorbing')
    controller.logger.debug("Got finished_absorbing")
    start, stop = intervals.parse_interval(finished_absorbing['new_interval'])

    finished_splitting, sender_id = await controller.listen(type='finished_splitting')
    controller.logger.debug("Got finished_splitting")
    controller.node._kids.split(
        kid_id=self._kid_id,
        mid=start,
        new_kid=controller.role_handle_to_node_handle(new),
        kid_summary=finished_splitting['summary'],
        new_kid_summary=finished_absorbing['summary'])
    controller.node.check_limits()


class SplitNode(transaction.ParticipantRole):
  '''
  As part of a `SplitKid` transaction originated by the parent,
  split ``controller.node._kids`` into two pieces, and send the right half to ``self._absorber``
  '''

  def __init__(self, parent, absorber):
    self._parent = parent
    self._absorber = absorber

  async def run(self, controller: 'TransactionRoleController'):
    if controller.node._height == 0 and controller.node._dataset_program_config['interval_type'] != 'interval':
      raise errors.InternalError(f"Unable to split a height 0 Node with interval_type \"{interval_type}\" != interval")
    mid, leaving_kids = controller.node._kids.shrink_right()
    controller.logger.info(
        "Splitting at midpoint {midpoint} [{kids_before_midpoint}]-midpoint-[{kids_after_midpoint}]",
        extra={
            'midpoint': mid,
            'kids_after_midpoint': len(leaving_kids),
            'kids_before_midpoint': len(controller.node._kids),
        })
    leaving_kid_ids = [kid['id'] for kid in leaving_kids]

    controller.send(self._absorber,
                    messages.data.absorb_these_kids(kid_ids=leaving_kid_ids, left_endpoint=intervals.key_to_json(mid)))

    for kid in leaving_kids:
      controller.enlist(
          kid, helpers.FosterChild,
          dict(
              old_parent=controller.new_handle(kid['id']),
              new_parent=controller.transfer_handle(self._absorber, kid['id'])))

    controller.logger.debug("Waiting for goodbyes from kids")
    kid_ids = set(leaving_kid_ids)
    while kid_ids:
      _goodbye_parent, kid_id = await controller.listen(type='goodbye_parent')
      kid_ids.remove(kid_id)
    controller.logger.debug("Got goodbyes from kids")

    controller.send(self._parent, messages.data.finished_splitting(summary=controller.node._kid_summary_message()))
    controller.node.check_limits()
