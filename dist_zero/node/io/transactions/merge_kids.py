from dist_zero import transaction, messages, errors

from . import helpers


class MergeKids(transaction.OriginatorRole):
  '''
  Originate a transaction to merge two nodes by terminating the left node and having the right node adopt its kids.
  '''

  def __init__(self, left_kid_id, right_kid_id):
    self._absorbee_id = left_kid_id
    self._absorber_id = right_kid_id

  async def run(self, controller: 'TransactionRoleController'):
    # By the time this transaction starts to run, the kids may no longer be mergeable
    if not controller.node._kids_are_mergeable(self._absorbee_id, self._absorber_id):
      return

    controller.logger.info(
        "Starting MergeKids transaction merging '{absorbee_id}' into '{absorber_id}'",
        extra={
            'absorbee_id': self._absorbee_id,
            'absorber_id': self._absorber_id,
        })

    controller.enlist(controller.node._data_node_kids[self._absorber_id], helpers.GrowAbsorber,
                      dict(parent=controller.new_handle(self._absorber_id)))

    hello_parent, _sender_id = await controller.listen(type='hello_parent')
    self._absorber = hello_parent['kid']

    controller.enlist(
        controller.node._data_node_kids[self._absorbee_id], helpers.Absorbee,
        dict(
            parent=controller.new_handle(self._absorbee_id),
            absorber=controller.transfer_handle(self._absorber, self._absorbee_id)))

    await controller.listen(type='goodbye_parent')
    finished_absorbing, _sender_id = await controller.listen(type='finished_absorbing')
    controller.node._data_node_kids.set_summary(self._absorbee_id, finished_absorbing['summary'])
    controller.node._data_node_kids.merge_right(self._absorbee_id)

    controller.node._send_kid_summary()
    controller.logger.info("Finished MergeKids transaction.")
