from dist_zero import transaction, errors, intervals, messages
from dist_zero.node.data.kids import DataNodeKids

from . import helpers, spawn_kid


class NewDataset(transaction.ParticipantRole):
  def __init__(self, requester=None):
    self._requester = requester

  async def run(self, controller: 'TransactionRoleController'):
    if controller.node._kids:
      raise errors.InternalError("Can't start a new dataset on a node that already has kids.")
    if controller.node._parent:
      raise errors.InternalError("Can't start a new dataset on a node with a parent.")

    controller.node._kids = DataNodeKids(intervals.Min, intervals.Max, controller=controller.node._controller)

    if controller.node._height > 1:
      controller.logger.info("NewDataset is dispatching to SpawnKid")
      await spawn_kid.SpawnKid().run(controller)
    else:
      controller.logger.info(
          "NewDataset is not spawning any kids, as the height is too low. {height}",
          extra={'height': controller.node._height})

    if self._requester is not None:
      controller.send(self._requester, messages.data.started_dataset(controller.new_handle(self._requester['id'])))
