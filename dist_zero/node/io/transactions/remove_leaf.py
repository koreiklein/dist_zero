from dist_zero import transaction, errors

from . import helpers


class RemoveLeaf(transaction.OriginatorRole):
  def __init__(self, kid_id):
    self._kid_id = kid_id

  async def run(self, controller: 'TransactionRoleController'):
    controller.node._updated_summary = True
    if self._kid_id in controller.node._kids:
      controller.node._kids.pop(self._kid_id)
      start, stop = controller.node._kid_to_interval.pop(self._kid_id)
      controller.node._kid_intervals.remove([start, stop, self._kid_id])

    if self._kid_id in controller.node._kid_summaries:
      controller.node._kid_summaries.pop(self._kid_id)

    controller.node._send_kid_summary()
