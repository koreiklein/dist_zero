import blist

from dist_zero import transaction, messages, ids, errors, infinity
from dist_zero.network_graph import NetworkGraph

from . import helpers


class BumpHeight(transaction.OriginatorRole):
  async def run(self, controller: 'TransactionRoleController'):
    if controller.node._parent is not None:
      raise errors.InternalError("Only the root node may bump its height.")

    controller.logger.info("Starting a BumpHeight transaction on the root node.")

    self.proxy_id = ids.new_id('DataNode_proxy')

    proxy_config = messages.io.data_node_config(
        node_id=self.proxy_id,
        parent=controller.node.new_handle(self.proxy_id),
        variant=controller.node._variant,
        leaf_config=controller.node._leaf_config,
        height=controller.node._height)

    controller.spawn_enlist(
        proxy_config, helpers.Absorber,
        dict(parent=controller.new_handle(self.proxy_id), interval=infinity.interval_json(controller.node._interval)))
    hello_parent, _sender_id = await controller.listen(type='hello_parent')
    proxy = hello_parent['kid']

    kids_to_absorb = list(controller.node._kids.keys())
    controller.send(proxy, messages.io.absorb_these_kids(kids_to_absorb))

    kid_ids = set(kids_to_absorb)
    for kid_id in kids_to_absorb:
      controller.enlist(
          controller.node._kids[kid_id], helpers.FosterChild,
          dict(old_parent=controller.new_handle(kid_id), new_parent=controller.transfer_handle(proxy, kid_id)))

    while kid_ids:
      _goodbye_parent, kid_id = await controller.listen(type='goodbye_parent')
      kid_ids.remove(kid_id)

    finished_absorbing, _sender_id = await controller.listen(type='finished_absorbing')

    proxy_node = controller.role_handle_to_node_handle(proxy)

    # Restore node state

    controller.node._height += 1
    controller.node._kids = {proxy['id']: proxy_node}
    controller.node._kid_summaries = {}
    controller.node._kid_summaries[proxy['id']] = finished_absorbing['summary']
    proxy_interval = list(controller.node._interval)
    controller.node._kid_to_interval = {proxy['id']: proxy_interval}
    controller.node._initialize_kid_intervals()
    controller.node._kid_intervals.add([proxy_interval[0], proxy_interval[1], proxy['id']])
    controller.node._graph = NetworkGraph()
    controller.node._graph.add_node(proxy['id'])
    if controller.node._adjacent is not None:
      controller.node.send(
          controller.node._adjacent,
          messages.io.bumped_height(
              proxy=controller.node.transfer_handle(proxy_node, controller.node._adjacent['id']),
              kid_ids=kids_to_absorb,
              variant=controller.node._variant))

    # After bumping the height, we will certainly need a new kid
    from .split_kid import SplitKid
    await SplitKid(kid_id=proxy['id']).run(controller)

    controller.logger.info("Finish BumpHeight")
