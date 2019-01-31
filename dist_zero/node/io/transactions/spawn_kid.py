'''
How it works now:
  - parent spawns a kid node with the proper config
  - kid sends hello_parent
  - parent gets info from kid

  ! Also, if the kid starts up and decides it can't be childless,
    then it waits until it has spawned its own kid before sending its hello_parent.
    - This behavior involves the startup_kid parameter.
'''

from dist_zero import transaction, messages, ids, errors


class SpawnKid(transaction.OriginatorRole):
  def __init__(self, send_summary=True):
    self._send_summary = send_summary

  async def run(self, controller: 'TransactionRoleController'):
    if controller.node._height == 0:
      raise errors.InternalError("height 0 DataNode instances can not spawn kids")

    controller.logger.info("Starting a SpawnKid transaction.")

    node_id = ids.new_id("DataNode_kid")
    node_config = messages.io.data_node_config(
        node_id=node_id,
        parent=None,
        variant=controller.node._variant,
        leaf_config=controller.node._leaf_config,
        height=controller.node._height - 1)

    controller.spawn_enlist(node_config, 'StartDataNode', dict(parent=controller.new_handle(node_id), ))

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


class StartDataNode(transaction.ParticipantRole):
  def __init__(self, parent):
    self._parent = parent

  async def run(self, controller: 'TransactionRoleController'):
    controller.node._parent = controller.role_handle_to_node_handle(self._parent)
    controller.logger.info("Starting a new data node.")

    if controller.node._height > 1 and len(controller.node._kids) == 0:
      # In this case, this node should start with at least one kid.
      # Include the logic of a SpawnKid transaction as part of this transaction.
      await SpawnKid(False).run(controller)

    controller.send(
        self._parent,
        messages.io.hello_parent(
            controller.new_handle(self._parent['id']), kid_summary=controller.node._kid_summary_message()))
