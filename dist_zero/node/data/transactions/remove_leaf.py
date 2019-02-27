from dist_zero import transaction, errors

from . import helpers


class RemoveLeaf(transaction.OriginatorRole):
  def __init__(self, kid_id):
    self._kid_id = kid_id

  async def run(self, controller: 'TransactionRoleController'):
    controller.node._updated_summary = True
    if self._kid_id in controller.node._kids:
      controller.logger.info("Removing kid")
      controller.node._kids.remove_kid(self._kid_id)
    else:
      controller.logger.info("Kid to remove was not found")

    controller.node._send_kid_summary()
    controller.node.check_limits()
