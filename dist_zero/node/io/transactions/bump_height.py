from dist_zero import transaction, messages, ids, errors
from dist_zero.network_graph import NetworkGraph
'''
Old bump height transaction:
  - parent sets _root_proxy_id
  - parent spawns a proxy node using adopter_node_config with its kids as adoptees
    - that config contains within it the data_node_config
  - when the adopter starts, it sends adopt messages to all of the old root's kids
  - each kid says goodbye to the root, and hello to the proxy
  - the proxy says hello to the root
  - the root receives that hello, and calls _finish_bumping_height which sets up a lot of important state

New plan for the BumpHeight transaction:
  - BumpHeight starts on the root
  - the root starts a proxy with spawn_enlist to run an Absorber role.
  - The absorber says hello to the root
  - The root gets the hello, and
    - enlists all of its kids into FosterChild roles.
    - sends absorb_these_kids to the proxy
  - The proxy absorbs all the kids, then sends finished_absorbing to the root and terminates
  - The root waits for all of the goodbyes from its old kids, and the finished_absorbing from the proxy
  - The root finishes by setting up all the same state that was required before

TODOS:
  - maybe remove any of the following
    - the adopt message
    - the AdopterNode class and module
    - def change_node

'''


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

    controller.spawn_enlist(proxy_config, 'Absorber', dict(parent=controller.new_handle(self.proxy_id)))
    hello_parent, _sender_id = await controller.listen(type='hello_parent')
    proxy = hello_parent['kid']
    proxy_summary = hello_parent['kid_summary']

    kids_to_absorb = list(controller.node._kids.keys())
    controller.send(proxy, messages.io.absorb_these_kids(kids_to_absorb))

    kid_ids = set(kids_to_absorb)
    for kid in controller.node._kids.values():
      controller.enlist(
          kid, 'FosterChild',
          dict(old_parent=controller.new_handle(kid['id']), new_parent=controller.transfer_handle(proxy, kid['id'])))

    while kid_ids:
      _goodbye_parent, kid_id = await controller.listen(type='goodbye_parent')
      kid_ids.remove(kid_id)

    await controller.listen(type='finished_absorbing')

    proxy_node = controller.role_handle_to_node_handle(proxy)

    controller.node._height += 1
    controller.node._kid_summaries = {}
    controller.node._kids = {proxy['id']: proxy_node}
    controller.node._graph = NetworkGraph()
    controller.node._graph.add_node(proxy['id'])
    if controller.node._adjacent is not None:
      controller.node.send(
          controller.node._adjacent,
          messages.io.bumped_height(
              proxy=self.transfer_handle(proxy_node, controller.node._adjacent['id']),
              kid_ids=kids_to_absorb,
              variant=controller.node._variant))
