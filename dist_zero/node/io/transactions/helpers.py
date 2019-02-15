from dist_zero import transaction, messages, errors, infinity

from .spawn_kid import SpawnKid


class StartDataNode(transaction.ParticipantRole):
  def __init__(self, parent, interval):
    self._parent = parent
    self._interval_json = interval

  async def run(self, controller: 'TransactionRoleController'):
    controller.node._parent = controller.role_handle_to_node_handle(self._parent)
    controller.node._set_interval(self._interval_json)

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
            controller.new_handle(self._parent['id']),
            kid_summary=controller.node._kid_summary_message(),
            interval=self._interval_json))


class Absorber(transaction.ParticipantRole):
  '''Adopt all the kids from another role.'''

  def __init__(self, parent, interval=None):
    self.parent = parent
    self.interval = interval

  async def run(self, controller: 'TransactionRoleController'):
    if self.interval is not None:
      controller.node._interval = infinity.parse_interval(self.interval)

    controller.send(
        self.parent,
        messages.io.hello_parent(
            controller.new_handle(self.parent['id']), kid_summary=controller.node._kid_summary_message()))

    absorb_these_kids, _sender_id = await controller.listen(type='absorb_these_kids')
    kid_ids = set(absorb_these_kids['kid_ids'])

    while kid_ids:
      hello_parent, kid_id = await controller.listen(type='hello_parent')
      if 'interval' not in hello_parent:
        raise errors.InternalError("Each new kid must send its interval to Absorber.")

      kid_ids.remove(kid_id)
      controller.node._kids[kid_id] = controller.role_handle_to_node_handle(hello_parent['kid'])
      interval = infinity.parse_interval(hello_parent['interval'])
      controller.node._kid_to_interval[kid_id] = interval
      controller.node._kid_intervals.add([interval[0], interval[1], kid_id])
      if hello_parent['kid_summary']:
        controller.node._kid_summaries[kid_id] = hello_parent['kid_summary']

    if controller.node._kid_intervals and controller.node._interval[0] > controller.node._kid_intervals[0][0]:
      controller.node._interval[0] = controller.node._kid_intervals[0][0]

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
            interval=controller.node._interval_json(),
        ))
    controller.node._parent = controller.role_handle_to_node_handle(self.new_parent)
    controller.node._send_kid_summary()
