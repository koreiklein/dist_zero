from dist_zero import transaction, messages
from .merge_kids import Absorbee
'''
Old structure:
  root  ---> proxy --> kid0
                   --> kid1
                   --> kid2
                   --> kid3
                   ...

Old way of doing it:
  - root sets root._root_consuming_proxy_id
  - root sends merge_with to proxy
  - proxy has its kids switch their parent to the root and terminates sending goodbye_parent
  - root gets goodbye_parent, decrements its height and goes back to the old state

New way of doing it:
  - root starts a transaction ConsumeProxy
  - root enlists its proxy as an Absorbee
  - proxy enlists kids as FosterChild
'''


class ConsumeProxy(transaction.OriginatorRole):
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
