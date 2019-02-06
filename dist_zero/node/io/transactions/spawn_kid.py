from dist_zero import transaction, messages, ids, errors

from . import helpers


class SpawnKid(transaction.OriginatorRole):
  def __init__(self, send_summary=True, force=False):
    self._send_summary = send_summary
    self._force = force

  async def run(self, controller: 'TransactionRoleController'):
    if controller.node._height == 0:
      raise errors.InternalError("height 0 DataNode instances can not spawn kids")
    if not self._force and not controller.node._monitor.out_of_capacity():
      controller.logger.info("Canceling SpawnKid transaction because the spawning node is not out of capacity.")
      return

    controller.logger.info("Starting a SpawnKid transaction.")

    node_id = ids.new_id("DataNode_kid")
    node_config = messages.io.data_node_config(
        node_id=node_id,
        parent=None,
        variant=controller.node._variant,
        leaf_config=controller.node._leaf_config,
        height=controller.node._height - 1)

    controller.spawn_enlist(node_config, helpers.StartDataNode, dict(parent=controller.new_handle(node_id), ))

    hello_parent, _sender_id = await controller.listen(type='hello_parent')
    if hello_parent['kid_summary']:
      controller.node._kid_summaries[node_id] = hello_parent['kid_summary']
    else:
      controller.node._kid_summaries[node_id] = messages.io.kid_summary(
          size=0, n_kids=0, availability=controller.node._leaf_availability * controller.node._kid_capacity_limit)
    controller.node._kids[node_id] = controller.role_handle_to_node_handle(hello_parent['kid'])
    if self._send_summary:
      controller.node._send_kid_summary()

    controller.logger.info("Finishing a SpawnKid transaction.")