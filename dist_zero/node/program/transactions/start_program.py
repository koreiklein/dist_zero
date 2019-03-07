from dist_zero import transaction, messages, errors

from dist_zero.node.data.transactions import new_dataset
from dist_zero.node.link.transactions import create_link
from dist_zero.node.data.transactions.send_start_subscription import SendStartSubscription
from dist_zero.node.data.transactions.receive_start_subscription import ReceiveStartSubscription


class StartProgram(transaction.ParticipantRole):
  '''
  Begin an entire distributed program.
  '''

  def __init__(self, dataset_configs, link_configs):
    '''
    :param list dataset_configs: The list of `dataset_config` messages.
    :param list link_configs: The list of `link_config` messages.
    '''
    self._dataset_configs = dataset_configs
    self._link_configs = link_configs

    self._controller = None

  async def run(self, controller: 'TransactionRoleController'):
    self._controller = controller

    # Spawn all the datasets at once
    for dataset_config in self._dataset_configs:
      self._spawn_dataset(dataset_config)

    # Wait for all the datasets to be up and running
    self._controller.node._datasets = {}
    while len(self._controller.node._datasets) < len(self._dataset_configs):
      started_dataset, ds_id = await self._controller.listen(type='started_dataset')
      self._controller.node._datasets[ds_id] = self._controller.role_handle_to_node_handle(started_dataset['root'])

    self._controller.node._spy_key_to_ds_id = {}
    for ds_config in self._dataset_configs:
      for spy_key in self._get_spy_keys(ds_config):
        self._controller.node._spy_key_to_ds_id[spy_key] = ds_config['id']

    self._controller.node._links = {}
    # Spawn the links one at a time to prevent deadlocks
    for link_config in self._link_configs:
      await StartLink(
          link_config=link_config,
          src=self._controller.node._datasets[link_config['src_dataset_id']],
          tgt=self._controller.node._datasets[link_config['tgt_dataset_id']]).run(self._controller)

  def _get_spy_keys(self, dataset_config):
    program_config = dataset_config['dataset_program_config']
    if program_config['type'] == 'reactive_dataset_program_config':
      return [spy_key for expr_json in program_config['concrete_exprs'] for spy_key in expr_json['spy_keys']]
    else:
      return []

  def _spawn_dataset(self, dataset_config):
    '''Spawn a dataset.  Do not wait for it to start.'''
    self._controller.spawn_enlist(
        node_config=messages.data.data_node_config(
            node_id=dataset_config['id'],
            parent=None,
            height=0 if dataset_config['singleton'] else 2,
            dataset_program_config=dataset_config['dataset_program_config']),
        participant=new_dataset.NewDataset,
        args=dict(requester=self._controller.new_handle(dataset_config['id'])))


class StartLink(transaction.ParticipantRole):
  def __init__(self, link_config, src, tgt):
    self._link_config = link_config
    self._src = src
    self._tgt = tgt
    self._controller = None

  async def run(self, controller: 'TransactionRoleController'):
    '''Spawn a link and wait for it to start.'''
    self._controller = controller

    link_key = self._link_config['link_key']

    self._controller.enlist(self._src, SendStartSubscription,
                            dict(parent=self._controller.new_handle(self._src['id']), link_key=link_key))
    self._controller.enlist(self._tgt, ReceiveStartSubscription,
                            dict(requester=self._controller.new_handle(self._tgt['id']), link_key=link_key))

    ds_hello_parent = {}
    for i in range(2):
      hello_parent, kid_id = await self._controller.listen(type='hello_parent')
      ds_hello_parent[kid_id] = hello_parent

    self._controller.spawn_enlist(
        node_config=messages.link.link_node_config(
            node_id=self._link_config['id'],
            left_is_data=True,
            right_is_data=True,
            height=max(hello_parent['kid_summary']['height'] for hello_parent in ds_hello_parent.values()),
            link_key=self._link_config['link_key']),
        participant=create_link.CreateLink,
        args=dict(
            requester=self._controller.new_handle(self._link_config['id']),
            src=self._controller.transfer_handle(ds_hello_parent[self._src['id']]['kid'], self._link_config['id']),
            tgt=self._controller.transfer_handle(ds_hello_parent[self._tgt['id']]['kid'], self._link_config['id']),
        ))

    link_started, link_id = await self._controller.listen(type='link_started')
    self._controller.node._links[link_id] = self._controller.role_handle_to_node_handle(link_started['link'])
