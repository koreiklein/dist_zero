from dist_zero import transaction, messages, errors

from .spawn_kid import SpawnKid


class StartDataNode(transaction.ParticipantRole):
  def __init__(self, parent):
    self._parent = parent

  async def run(self, controller: 'TransactionRoleController'):
    controller.node._parent = controller.role_handle_to_node_handle(self._parent)

    if controller.node._height > 1 and len(controller.node._kids) == 0:
      # In this case, this node should start with at least one kid.
      # Include the logic of a SpawnKid transaction as part of this transaction.
      controller.logger.info("Starting a new data node with a single kid.")
      await SpawnKid(send_summary=False).run(controller)
    else:
      controller.logger.info("Starting a new data node without additional kids.")

    controller.send(
        self._parent,
        messages.io.hello_parent(
            controller.new_handle(self._parent['id']), kid_summary=controller.node._kid_summary_message()))


class Absorber(transaction.ParticipantRole):
  '''Adopt all the kids from another role.'''

  def __init__(self, parent):
    self.parent = parent

  async def run(self, controller: 'TransactionRoleController'):
    controller.send(
        self.parent,
        messages.io.hello_parent(
            controller.new_handle(self.parent['id']), kid_summary=controller.node._kid_summary_message()))

    absorb_these_kids, _sender_id = await controller.listen(type='absorb_these_kids')
    kid_ids = set(absorb_these_kids['kid_ids'])

    while kid_ids:
      hello_parent, kid_id = await controller.listen(type='hello_parent')
      kid_ids.remove(kid_id)
      controller.node._kids[kid_id] = controller.role_handle_to_node_handle(hello_parent['kid'])
      if hello_parent['kid_summary']:
        controller.node._kid_summaries[kid_id] = hello_parent['kid_summary']

    controller.send(self.parent, messages.io.finished_absorbing(controller.node._kid_summary_message()))

    controller.node._send_kid_summary()


class Absorbee(transaction.ParticipantRole):
  '''Transfer all of a node's kids to an `Absorber`'''

  def __init__(self, parent, absorber):
    self.parent = parent
    self.absorber = absorber

  async def run(self, controller: 'TransactionRoleController'):
    kid_ids = set()
    controller.send(self.absorber, messages.io.absorb_these_kids(list(controller.node._kids.keys())))
    for kid in controller.node._kids.values():
      kid_ids.add(kid['id'])
      controller.enlist(
          kid, FosterChild,
          dict(
              old_parent=controller.new_handle(kid['id']),
              new_parent=controller.transfer_handle(self.absorber, kid['id'])))

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
        ))
    controller.node._parent = controller.role_handle_to_node_handle(self.new_parent)
    controller.node._send_kid_summary()
