from dist_zero import transaction, errors, infinity

from . import helpers, spawn_kid


class NewDataset(transaction.ParticipantRole):
  async def run(self, controller: 'TransactionRoleController'):
    if controller.node._kids:
      raise errors.InternalError("Can't start a new dataset on a node that already has kids.")
    if controller.node._parent:
      raise errors.InternalError("Can't start a new dataset on a node with a parent.")

    controller.node._interval = [infinity.Min, infinity.Max]

    if controller.node._height > 1:
      await spawn_kid.SpawnKid().run(controller)
