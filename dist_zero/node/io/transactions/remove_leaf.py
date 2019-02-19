from dist_zero import transaction, errors

from . import helpers


class RemoveLeaf(transaction.OriginatorRole):
  def __init__(self, kid_id):
    self._kid_id = kid_id

  async def run(self, controller: 'TransactionRoleController'):
    controller.node._updated_summary = True
    if self._kid_id in controller.node._data_node_kids:
      controller.node._data_node_kids.remove_kid(self._kid_id)

    controller.node._send_kid_summary()
