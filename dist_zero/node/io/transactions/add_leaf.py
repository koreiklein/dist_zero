from dist_zero import transaction, errors, messages

from . import helpers


class AddLeaf(transaction.ParticipantRole):
  '''Transaction on leaf node to add itself to a dataset.'''

  def __init__(self, parent):
    self._parent = parent

  async def run(self, controller: 'TransactionRoleController'):
    controller.logger.info(
        'Starting AddLeaf transaction adding {kid_id} to {parent_id}',
        extra={
            'kid_id': controller.node.id,
            'parent_id': self._parent['id'],
        })

    controller.enlist(
        self._parent, AddLeafParent,
        dict(kid=controller.new_handle(self._parent['id']), kid_summary=controller.node._kid_summary_message()))

    leaf_key, _sender_id = await controller.listen(type='set_leaf_key')
    # Leaves have None as their interval's stop coordinate
    controller.node._interval = [leaf_key['key'], None]


class AddLeafParent(transaction.ParticipantRole):
  '''Transaction on the immediate parent of a leaf node to add the leaf to the dataset as one of its kids.'''

  def __init__(self, kid, kid_summary):
    self._kid = kid
    self._kid_summary = kid_summary

  async def run(self, controller: 'TransactionRoleController'):
    controller.logger.info(
        'Starting AddLeafParent transaction adding {kid_id} to {parent_id}',
        extra={
            'kid_id': self._kid['id'],
            'parent_id': controller.node.id,
        })

    kid = controller.role_handle_to_node_handle(self._kid)
    controller.node._updated_summary = True

    controller.node._kids[kid['id']] = kid
    controller.node._kid_summaries[kid['id']] = self._kid_summary
    key = controller.node._next_leaf_key()
    interval = [key, None] # Leaves have None as their interval's stop coordinate
    controller.node._kid_to_interval[kid['id']] = interval
    controller.node._kid_intervals.add([key, None, kid['id']])
    controller.send(self._kid, messages.io.set_leaf_key(key=key))

    if controller.node._exporter is not None:
      controller.node.send(
          controller.node._exporter.receiver,
          messages.migration.update_left_configuration(
              parent_id=controller.node.id,
              new_kids=[{
                  'connection_limit':
                  controller.node.system_config['SUM_NODE_SENDER_LIMIT'],
                  'handle':
                  controller.node.transfer_handle(handle=kid, for_node_id=controller.node._exporter.receiver_id)
              }],
              new_height=controller.node._height))

    if controller.node._importer is not None:
      controller.node.send(
          controller.node._importer.sender,
          messages.migration.update_right_configuration(
              parent_id=controller.node.id,
              new_kids=[controller.node.transfer_handle(kid, controller.node._importer.sender_id)],
              new_height=controller.node._height))

    if controller.node._monitor.out_of_capacity():
      controller.node._send_kid_summary()
