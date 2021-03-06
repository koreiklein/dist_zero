import blist

from dist_zero import transaction, messages, ids, errors

from . import helpers


class BumpHeight(transaction.OriginatorRole):
  async def run(self, controller: 'TransactionRoleController'):
    if controller.node._parent is not None:
      raise errors.InternalError("Only the root node may bump its height.")

    self.proxy_id = ids.new_id('DataNode_proxy')

    controller.logger.debug("Spawning proxy {proxy_id}.", extra={'proxy_id': self.proxy_id})

    proxy_config = messages.data.data_node_config(
        node_id=self.proxy_id,
        parent=controller.node.new_handle(self.proxy_id),
        dataset_program_config=controller.node._dataset_program_config,
        height=controller.node._height)

    controller.spawn_enlist(
        proxy_config, helpers.NewAbsorber,
        dict(parent=controller.new_handle(self.proxy_id), interval=controller.node._interval_json()))
    hello_parent, _sender_id = await controller.listen(type='hello_parent')
    proxy = hello_parent['kid']

    controller.logger.debug("Received hello from proxy", extra={'proxy_id': self.proxy_id})

    kids_to_absorb = list(controller.node._kids)
    controller.send(
        proxy, messages.data.absorb_these_kids(
            kid_ids=kids_to_absorb, left_endpoint=controller.node._interval_json()[0]))

    kid_ids = set(kids_to_absorb)
    controller.logger.debug("Sending children to leave for the proxy", extra={'n_kids': len(kid_ids)})
    for kid_id in kids_to_absorb:
      controller.enlist(
          controller.node._kids[kid_id], helpers.FosterChild,
          dict(old_parent=controller.new_handle(kid_id), new_parent=controller.transfer_handle(proxy, kid_id)))

    while kid_ids:
      _goodbye_parent, kid_id = await controller.listen(type='goodbye_parent')
      kid_ids.remove(kid_id)

    controller.logger.debug("All kids have left")

    finished_absorbing, _sender_id = await controller.listen(type='finished_absorbing')

    proxy_node = controller.role_handle_to_node_handle(proxy)

    # Restore node state

    controller.node._height += 1
    interval = controller.node._interval()
    controller.node._kids.clear()
    controller.node._kids.add_kid(kid=proxy_node, interval=interval, summary=finished_absorbing['summary'])

    # After bumping the height, we will certainly need a new kid
    from .split_kid import SplitKid
    await SplitKid(kid_id=proxy['id']).run(controller)

    controller.logger.info("Finish BumpHeight")
