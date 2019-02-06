from dist_zero import transaction, errors

from . import helpers, spawn_kid


class NewDataset(transaction.ParticipantRole):
  async def run(self, controller: 'TransactionRoleController'):
    if controller.node._parent is None and controller.node._height > 1 and len(controller.node._kids) == 0:
      await spawn_kid.SpawnKid().run(controller)
