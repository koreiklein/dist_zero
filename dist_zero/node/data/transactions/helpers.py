from dist_zero import transaction, messages, errors, intervals
from dist_zero.node.data.kids import DataNodeKids

from .spawn_kid import SpawnKid


class StartDataNode(transaction.ParticipantRole):
  def __init__(self, parent, interval):
    self._parent = parent
    self._interval_json = interval

  async def run(self, controller: 'TransactionRoleController'):
    controller.node._parent = controller.role_handle_to_node_handle(self._parent)
    controller.node._kids = DataNodeKids(
        *intervals.parse_interval(self._interval_json), controller=controller.node._controller)
    if controller.node._kids.left == controller.node._kids.right:
      import ipdb
      ipdb.set_trace()

    if controller.node._height > 1:
      # In this case, this node should start with at least one kid.
      # Include the logic of a SpawnKid transaction as part of this transaction.
      controller.logger.info("Starting a new data node with a single kid.")
      await SpawnKid(send_summary=False).run(controller)
    else:
      controller.logger.info("Starting a new data node without additional kids.")

    controller.send(
        self._parent,
        messages.data.hello_parent(
            controller.new_handle(self._parent['id']),
            kid_summary=controller.node._kid_summary_message(),
            interval=self._interval_json))
    controller.node.check_limits()


class NewAbsorber(transaction.ParticipantRole):
  '''Like `GrowAbsorber`, but for starting up a new node.'''

  def __init__(self, parent, interval=None):
    self.parent = parent
    self.interval = interval

  async def run(self, controller: 'TransactionRoleController'):
    controller.node._kids = DataNodeKids(
        *intervals.parse_interval(self.interval), controller=controller.node._controller)
    controller.logger.info("Dispatching to GrowAbsorber")
    await GrowAbsorber(parent=self.parent).run(controller)
    if controller.node._kids.left == controller.node._kids.right:
      import ipdb
      ipdb.set_trace()


class GrowAbsorber(transaction.ParticipantRole):
  '''Adopt all the kids from another role.'''

  def __init__(self, parent):
    self.parent = parent

  async def run(self, controller: 'TransactionRoleController'):
    controller.send(
        self.parent,
        messages.data.hello_parent(
            controller.new_handle(self.parent['id']), kid_summary=controller.node._kid_summary_message()))

    absorb_these_kids, _sender_id = await controller.listen(type='absorb_these_kids')
    controller.node._kids.grow_left(intervals.json_to_key(absorb_these_kids['left_endpoint']))
    kid_ids = set(absorb_these_kids['kid_ids'])

    controller.logger.debug("waiting for hellos from kids")

    while kid_ids:
      hello_parent, kid_id = await controller.listen(type='hello_parent')
      kid_ids.remove(kid_id)
      controller.node._kids.add_kid(
          kid=controller.role_handle_to_node_handle(hello_parent['kid']),
          interval=intervals.parse_interval(hello_parent['interval']),
          summary=hello_parent['kid_summary'])

    controller.logger.debug("got hellos from kids")

    controller.send(
        self.parent,
        messages.data.finished_absorbing(
            controller.node._kid_summary_message(),
            new_interval=controller.node._interval_json(),
        ))

    controller.node._send_kid_summary()
    controller.node.check_limits()


class Absorbee(transaction.ParticipantRole):
  '''Transfer all of a node's kids to a `GrowAbsorber`'''

  def __init__(self, parent, absorber):
    self.parent = parent
    self.absorber = absorber

  async def run(self, controller: 'TransactionRoleController'):
    controller.logger.info("Being absorbed by {absorber_id}", extra={'absorber_id': self.absorber['id']})

    kid_ids = set()
    controller.send(
        self.absorber,
        messages.data.absorb_these_kids(
            kid_ids=list(controller.node._kids), left_endpoint=controller.node._interval_json()[0]))
    for kid_id in controller.node._kids:
      kid_ids.add(kid_id)
      controller.enlist(
          controller.node._kids[kid_id], FosterChild,
          dict(old_parent=controller.new_handle(kid_id), new_parent=controller.transfer_handle(self.absorber, kid_id)))

    controller.logger.debug("Waiting for goodbyes from kids")
    while kid_ids:
      _goodbye_parent, kid_id = await controller.listen(type='goodbye_parent')
      kid_ids.remove(kid_id)

    controller.logger.debug("Got goodbyes from kids")

    controller.send(self.parent, messages.data.goodbye_parent())
    controller.node._terminate()
    controller.node.check_limits()


class FosterChild(transaction.ParticipantRole):
  '''Switch the parent of a node.'''

  def __init__(self, old_parent, new_parent):
    self.old_parent = old_parent
    self.new_parent = new_parent

  async def run(self, controller: 'TransactionRoleController'):
    controller.logger.info(
        "Leaving old parent {old_parent_id} for new parent {new_parent_id}",
        extra={
            'old_parent_id': self.old_parent['id'],
            'new_parent_id': self.new_parent['id'],
        })
    controller.send(self.old_parent, messages.data.goodbye_parent())
    controller.send(
        self.new_parent,
        messages.data.hello_parent(
            controller.new_handle(self.new_parent['id']),
            kid_summary=controller.node._kid_summary_message(),
            interval=controller.node._interval_json(),
        ))
    controller.node._parent = controller.role_handle_to_node_handle(self.new_parent)
    controller.node._send_kid_summary()
    controller.node.check_limits()
