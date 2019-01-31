from dist_zero import transaction, messages


class MergeKids(transaction.OriginatorRole):
  '''
  Originate a transaction to merge two nodes by terminating the left node and having the right node adopt its kids.
  '''

  def __init__(self, left_kid_id, right_kid_id):
    self._absorbee_id = left_kid_id
    self._absorber_id = right_kid_id

  async def run(self, controller: 'TransactionController'):
    controller.enlist(controller.node._kids[self._absorber_id], 'Absorber',
                      dict(parent=controller.new_handle(self._absorber_id)))

    hello_parent, _sender_id = await controller.listen(type='hello_parent', node_id=self._absorber_id)
    self._absorber = hello_parent['kid']

    controller.enlist(
        controller.node._kids[self._absorbee_id], 'Absorbee',
        dict(
            parent=controller.new_handle(self._absorbee_id),
            absorber=controller.transfer_handle(self._absorber, self._absorbee_id)))

    await controller.listen(type='goodbye_parent', node_id=self._absorbee_id)
    await controller.listen(type='finished_absorbing', node_id=self._absorber_id)
    self._node._kids.pop(self._absorbee_id)


class Absorber(transaction.ParticipantRole):
  '''Adopt all the kids from another role.'''

  def __init__(self, parent):
    self.parent = parent

  async def run(self, controller: 'TransactionController'):
    controller.send(self.parent, messages.io.hello_parent(controller.new_handle(self.parent['id'])))

    kid_ids_list = await controller.listen(type='absorb_these_kids')
    kid_ids = set(kid_ids_list)

    while kid_ids:
      kid, kid_id = await controller.listen(type='hello_parent')
      kid_ids.remove(kid_id)
      controller.node._kids[kid_id] = kid

    controller.send(self.parent, messages.io.finished_absorbing())


class Absorbee(transaction.ParticipantRole):
  '''Transfer all of a node's kids to an `Absorber`'''

  def __init__(self, parent, absorber):
    self.parent = parent
    self.absorber = absorber

  async def run(self, controller: 'TransactionController'):
    kid_ids = set()
    controller.send(self.absorber, messages.io.absorb_these_kids(list(controller.node._kids.keys())))
    for kid in controller.node._kids.values():
      kid_ids.add(kid['id'])
      controller.enlist(
          kid, 'FosterChild',
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

  async def run(self, controller: 'TransactionController'):
    controller.send(self.old_parent, messages.io.goodbye_parent())
    controller.send(self.new_parent, messages.io.hello_parent(controller.new_handle(self.new_parent['id'])))
    controller.node._parent = controller.role_handle_to_node_handle(self.new_parent)
