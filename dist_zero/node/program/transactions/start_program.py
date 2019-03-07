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
    self._dataset_configs = dataset_configs
    self._link_configs = link_configs

    self._ds_id_to_handle = None # Map from dataset root id to its role handle
    self._link_id_to_handle = None # Map from link root id to its role handle

    self._controller = None

  async def run(self, controller: 'TransactionRoleController'):
    self._controller = controller

    # Spawn all the datasets at once
    for dataset_config in self._dataset_configs:
      self._spawn_dataset(dataset_config)

    # Wait for all the datasets to be up and running
    self._ds_id_to_handle = {}
    while len(self._ds_id_to_handle) < len(self._dataset_configs):
      started_dataset, ds_id = await self._controller.listen(type='started_dataset')
      self._ds_id_to_handle[ds_id] = started_dataset['root']

    self._link_id_to_handle = {}
    # Spawn the links one at a time to prevent deadlocks
    for link_config in self._link_configs:
      await self._spawn_link(link_config)

    self._controller.node._spy_key_to_ds_id = {}
    for ds_config in self._dataset_configs:
      for spy_key in self._get_spy_keys(ds_config):
        self._controller.node._spy_key_to_ds_id[spy_key] = ds_config['id']

    self._controller.node._datasets = {
        key: self._controller.role_handle_to_node_handle(dataset)
        for key, dataset in self._ds_id_to_handle.items()
    }

    self._controller.node._links = {
        key: self._controller.role_handle_to_node_handle(link)
        for key, link in self._link_id_to_handle
    }

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
            # FIXME(KK): Some datasets should be spawned at a height != 0.  Determine that here.
            height=0,
            dataset_program_config=dataset_config['dataset_program_config']),
        participant=new_dataset.NewDataset,
        args=dict(requester=self._controller.new_handle(dataset_config['id'])))

  async def _spawn_link_and_wait(self, link_config):
    '''Spawn a link and wait for it to start.'''
    src_id = link_config['src_dataset_id']
    tgt_id = link_config['tgt_dataset_id']

    self._controller.enlist(self._ds_id_to_handle[src_id], SendStartSubscription,
                            dict(parent=self._controller.new_handle(self._src['id']), link_key=link_key))
    self._controller.enlist(self._ds_id_to_handle[tgt_id], ReceiveStartSubscription,
                            dict(requester=self._controller.new_handle(self._tgt['id']), link_key=link_key))

    ds_hello_parent = {}
    for i in range(2):
      hello_parent, kid_id = await self._controller.listen(type='hello_parent')
      ds_hello_parent[kid_id] = hello_parent

    self._controller.spawn_enlist(
        node_config=messages.link.link_node_config(
            node_id=link_config['id'],
            left_is_data=True,
            right_is_data=True,
            height=max(hello_parent['kid_summary']['height'] for hello_parent in ds_hello_parent.values()),
            link_key=link_config['link_key']),
        participant=create_link.CreateLink,
        args=dict(
            requester=self._controller.new_handle(link_config['id']),
            src=self._controller.transfer_handle(ds_hello_parent[src_id]['kid'], link_config['id']),
            tgt=self._controller.transfer_handle(ds_hello_parent[tgt_id]['kid'], link_config['id']),
        ))

    link_started, link_id = await self._controller.listen(type='link_started')
    self._link_id_to_handle[link_id] = link_started['link']
