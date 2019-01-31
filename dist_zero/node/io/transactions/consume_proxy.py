from dist_zero import transaction, messages
from .merge_kids import Absorbee


class ConsumeProxy(transaction.OriginatorRole):
  '''A transaction that absorbs the unique child of a root node into the root node itself.'''

  async def run(self, controller: 'TransactionRoleController'):
    proxy = controller.node._get_proxy()
    if proxy is None:
      controller.logger.info("Aborting a scheduled ConsumeProxy transaction as the root does not current have a proxy.")
      return

    controller.logger.info(
        "Starting ConsumeProxy transaction to consume '{proxy_id}'.", extra={
            'proxy_id': proxy['id'],
        })

    my_handle = controller.new_handle(proxy['id'])
    controller.enlist(proxy, 'Absorbee', dict(parent=my_handle, absorber=my_handle))

    absorb_these_kids, _sender_id = await controller.listen(type='absorb_these_kids')
    kid_ids = set(absorb_these_kids['kid_ids'])

    while kid_ids:
      hello_parent, kid_id = await controller.listen(type='hello_parent')
      kid_ids.remove(kid_id)
      controller.node._kids[kid_id] = hello_parent['kid']

    await controller.listen(type='goodbye_parent')

    controller.node._height -= 1
    controller.node._remove_kid(proxy['id'])
    controller.logger.info(
        "Finished ConsumeProxy transaction.", extra={
            'proxy_id': proxy['id'],
        })
