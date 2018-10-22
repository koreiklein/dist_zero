from dist_zero import errors, ids, messages, connector


class BumpHeightTransaction(object):
  '''
  For spawning proxy children of a `ComputationNode` when the adjacent nodes bump their height.
  '''

  def __init__(self, node, proxy, kid_ids, variant):
    self._node = node

    self._kid_ids = kid_ids
    self._proxy_adjacent_variant = variant
    self._external_proxy = proxy

    self._proxy_adjacent_id = None
    '''The id of node that is spawned adjacent to the `DataNode`'s proxy.'''
    self._proxy_adjacent = None

    self._proxy_id = None
    '''The id of node that is spawned as this node's proxy.'''
    self._proxy = None

  def receive(self, message, sender_id):
    '''
    If this Transaction can handle the message, process it and return True
    otherwise, receive will return False and the surrounding code should be sure
    to delay the message until the transaction ends.
    '''
    if message['type'] == 'hello_parent' and sender_id == self._proxy_adjacent_id:
      self._proxy_adjacent = message['kid']
      self._spawn_proxy()
      return True
    elif message['type'] == 'hello_parent' and sender_id == self._proxy_id:
      self._proxy = message['kid']
      self._maybe_finished_bumping()
      return True
    elif message['type'] == 'goodbye_parent':
      if sender_id in self._old_kids:
        self._old_kids.pop(sender_id)
        self._maybe_finished_bumping()
        return True

    return False

  def _maybe_finished_bumping(self):
    if not self._old_kids and self._proxy and self._proxy_adjacent:
      self._finished_bumping()

  def _finished_bumping(self):
    # Pass on the right_configurations of self (as in dist_zero.connector.Spawner when the right gap child is spawned)
    self._node.send(self._proxy,
                    messages.migration.configure_right_parent(
                        migration_id=None, kid_ids=list(self._node._connector._right_configurations.keys())))
    self._node.send(self._proxy,
                    messages.migration.configure_new_flow_right(None, [
                        messages.migration.right_configuration(
                            parent_handle=self._node.transfer_handle(right_config['parent_handle'], self._proxy['id']),
                            height=right_config['height'],
                            is_data=right_config['is_data'],
                            n_kids=right_config['n_kids'],
                            connection_limit=right_config['connection_limit'],
                        ) for right_config in self._node._connector._right_configurations.values()
                    ]))

    left_root, = self._node._importers.keys()
    left_configuration = self._node._connector._left_configurations[left_root]
    left_configuration['kids'] = [{
        'connection_limit': self._node.system_config['SUM_NODE_SENDER_LIMIT'],
        'handle': self._external_proxy
    }]
    left_configuration['height'] += 1

    self._node._connector = connector.new_connector(
        self._node._connector_type,
        left_configurations={left_root: left_configuration},
        right_configurations=self._node._connector._right_configurations,
        computation_node=self._node)
    self._node._connector.fill_in(new_node_ids=[self._proxy_adjacent_id, self._proxy_id])
    self._node.height = self._node._connector.max_height()
    self._node.kids = {self._proxy_id: self._proxy, self._proxy_adjacent_id: self._proxy_adjacent}
    self._node.end_transaction()

  def _spawn_proxy(self):
    '''
    After an adjacent node bumps its height,
    a proxy for the adjacent will be spawned (its id will be stored in ``self._proxy_adjacent_id``)
    Once that proxy has reported that it is up and running, this node will call ``_spawn_proxy`` to
    spawn the second node to adopt the remaining kids of self as part of the process of bumping height.
    '''
    if self._proxy_adjacent_variant == 'input':
      senders = [self._node.transfer_handle(self._proxy_adjacent, self._proxy_id)]
      left_ids = [self._proxy_adjacent['id']]
      receivers = []
    elif self._proxy_adjacent_variant == 'output':
      import ipdb
      ipdb.set_trace()
      # FIXME(KK): Rethink, test and implement
      senders = []
      receivers = [self._node.transfer_handle(self._proxy_adjacent, self._proxy_id)]
    else:
      raise errors.InternalError("Unrecognized variant {}".format(self._proxy_adjacent_variant))

    self._node.send(self._proxy_adjacent,
                    messages.migration.configure_right_parent(migration_id=None, kid_ids=[self._proxy_id]))

    self._node._controller.spawn_node(
        messages.io.adopter_node_config(
            adoptees=[
                self._node.transfer_handle(kid, self._proxy_id) for kid in self._node.kids.values()
                if kid['id'] != self._proxy_adjacent['id']
            ],
            data_node_config=messages.computation.computation_node_config(
                node_id=self._proxy_id,
                parent=self._node.new_handle(self._proxy_id),
                height=self._node.height,
                leaf_config=self._node._leaf_config,
                left_is_data=False,
                right_is_data=False,
                configure_right_parent_ids=[self._node.id],
                connector=self._node._connector.non_left_part_json(),
                left_ids=left_ids,
                senders=senders,
                receiver_ids=[], # Make sure not actually send based on the right config for a differently layered parent.
                connector_type=self._node._connector_type,
                migrator=None)))

  def start(self):
    '''Called in response to an adjacent node informing self that it has bumped its height.'''
    self._old_kids = self._node.kids
    self._proxy_adjacent_id = ids.new_id('ComputationNode_{}_proxy_adjacent'.format(self._proxy_adjacent_variant))
    self._proxy_id = ids.new_id('ComputationNode_proxy')
    if self._proxy_adjacent_variant == 'input':
      left_root, = self._node._importers.keys()
      senders = [self._node.transfer_handle(self._external_proxy, self._proxy_adjacent_id)]
      receivers = [] # The receiver will be added later
      configure_right_parent_ids = [self._node.id]
      adoptee_ids = [
          computation_kid_id for io_kid in self._kid_ids
          for computation_kid_id in self._node._connector.graph.node_receivers(io_kid)
      ]
      left_ids = [self._external_proxy['id']]
    elif self._proxy_adjacent_variant == 'output':
      import ipdb
      ipdb.set_trace()
      senders = [] # The sender will be added later
      receivers = [self._node.transfer_handle(self._external_proxy, self._proxy_adjacent_id)]
      adoptee_ids = [
          computation_kid_id for io_kid in self._kid_ids
          for computation_kid_id in self._node._connector.graph.node_senders(io_kid)
      ]
    else:
      raise errors.InternalError("Unrecognized variant {}".format(self._proxy_adjacent_variant))
    self._node._controller.spawn_node(
        messages.io.adopter_node_config(
            adoptees=[
                self._node.transfer_handle(self._node.kids[adoptee_id], self._proxy_adjacent_id)
                for adoptee_id in adoptee_ids
            ],
            data_node_config=messages.computation.computation_node_config(
                node_id=self._proxy_adjacent_id,
                parent=self._node.new_handle(self._proxy_adjacent_id),
                left_is_data=self._proxy_adjacent_variant == 'input',
                right_is_data=self._proxy_adjacent_variant == 'output',
                leaf_config=self._node._leaf_config,
                configure_right_parent_ids=configure_right_parent_ids,
                left_ids=left_ids,
                height=self._node.height,
                connector=self._node._connector.left_part_json(parent_id=self._proxy_id),
                senders=senders,
                receiver_ids=None,
                connector_type=self._node._connector_type,
                migrator=None)))
