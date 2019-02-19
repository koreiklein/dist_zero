from dist_zero import transaction, messages, errors, infinity
from dist_zero.node.io.kids import DataNodeKids

from .spawn_kid import SpawnKid


class StartDataNode(transaction.ParticipantRole):
  def __init__(self, parent, interval):
    self._parent = parent
    self._interval_json = interval

  async def run(self, controller: 'TransactionRoleController'):
    controller.node._parent = controller.role_handle_to_node_handle(self._parent)
    controller.node._data_node_kids = DataNodeKids(
        *infinity.parse_interval(self._interval_json), controller=controller.node._controller)

    if controller.node._height > 1:
      # In this case, this node should start with at least one kid.
      # Include the logic of a SpawnKid transaction as part of this transaction.
      controller.logger.info("Starting a new data node with a single kid.")
      await SpawnKid(send_summary=False).run(controller)
    else:
      controller.logger.info("Starting a new data node without additional kids.")

    controller.send(
        self._parent,
        messages.io.hello_parent(
            controller.new_handle(self._parent['id']),
            kid_summary=controller.node._kid_summary_message(),
            interval=self._interval_json))


class NewAbsorber(transaction.ParticipantRole):
  '''Like `GrowAbsorber`, but for starting up a new node.'''

  def __init__(self, parent, interval=None):
    self.parent = parent
    self.interval = interval

  async def run(self, controller: 'TransactionRoleController'):
    controller.node._data_node_kids = DataNodeKids(
        *infinity.parse_interval(self.interval), controller=controller.node._controller)
    await GrowAbsorber(parent=self.parent).run(controller)


class GrowAbsorber(transaction.ParticipantRole):
  '''Adopt all the kids from another role.'''

  def __init__(self, parent):
    self.parent = parent

  async def run(self, controller: 'TransactionRoleController'):
    controller.send(
        self.parent,
        messages.io.hello_parent(
            controller.new_handle(self.parent['id']), kid_summary=controller.node._kid_summary_message()))

    absorb_these_kids, _sender_id = await controller.listen(type='absorb_these_kids')
    controller.node._data_node_kids.grow_left(infinity.json_to_key(absorb_these_kids['left_endpoint']))
    kid_ids = set(absorb_these_kids['kid_ids'])

    while kid_ids:
      hello_parent, kid_id = await controller.listen(type='hello_parent')
      kid_ids.remove(kid_id)
      controller.node._data_node_kids.add_kid(
          kid=controller.role_handle_to_node_handle(hello_parent['kid']),
          interval=infinity.parse_interval(hello_parent['interval']),
          summary=hello_parent['kid_summary'])

    controller.send(
        self.parent,
        messages.io.finished_absorbing(
            controller.node._kid_summary_message(),
            new_interval=controller.node._interval_json(),
        ))

    controller.node._send_kid_summary()


class Absorbee(transaction.ParticipantRole):
  '''Transfer all of a node's kids to an `Absorber`'''

  def __init__(self, parent, absorber):
    self.parent = parent
    self.absorber = absorber

  async def run(self, controller: 'TransactionRoleController'):
    kid_ids = set()
    controller.send(
        self.absorber,
        messages.io.absorb_these_kids(
            kid_ids=list(controller.node._data_node_kids), left_endpoint=controller.node._interval_json()[0]))
    for kid_id in controller.node._data_node_kids:
      kid_ids.add(kid_id)
      controller.enlist(
          controller.node._data_node_kids[kid_id], FosterChild,
          dict(old_parent=controller.new_handle(kid_id), new_parent=controller.transfer_handle(self.absorber, kid_id)))

    while kid_ids:
      _goodbye_parent, kid_id = await controller.listen(type='goodbye_parent')
      kid_ids.remove(kid_id)

    controller.send(self.parent, messages.io.goodbye_parent())
    controller.node._terminate()


class FosterChild(transaction.ParticipantRole):
  '''Switch the parent of a node.'''

  def __init__(self, old_parent, new_parent):
    self.old_parent = old_parent
    self.new_parent = new_parent

  async def run(self, controller: 'TransactionRoleController'):
    controller.send(self.old_parent, messages.io.goodbye_parent())
    controller.send(
        self.new_parent,
        messages.io.hello_parent(
            controller.new_handle(self.new_parent['id']),
            kid_summary=controller.node._kid_summary_message(),
            interval=controller.node._interval_json(),
        ))
    controller.node._parent = controller.role_handle_to_node_handle(self.new_parent)
    controller.node._send_kid_summary()
