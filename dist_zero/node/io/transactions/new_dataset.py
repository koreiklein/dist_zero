from dist_zero import transaction, errors, infinity
from dist_zero.node.io.kids import DataNodeKids

from . import helpers, spawn_kid


class NewDataset(transaction.ParticipantRole):
  async def run(self, controller: 'TransactionRoleController'):
    if controller.node._data_node_kids:
      raise errors.InternalError("Can't start a new dataset on a node that already has kids.")
    if controller.node._parent:
      raise errors.InternalError("Can't start a new dataset on a node with a parent.")

    controller.node._data_node_kids = DataNodeKids(infinity.Min, infinity.Max, controller=controller.node._controller)

    if controller.node._height > 1:
      await spawn_kid.SpawnKid().run(controller)
