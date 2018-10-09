from dist_zero import errors, ids, messages


class ProxySpawner(object):
  '''
  For spawning proxy children of a `ComputationNode` when the adjacent nodes bump their height.
  '''

  def __init__(self, node):
    self._node = node

    self._proxy_adjacent_id = None
    '''The id of node that is spawned adjacent to the `InternalNode`'s proxy.'''

    self._proxy_adjacent_variant = None
    '''input' or 'output' depending on which node bumped its height.'''

    self._proxy_id = None
    '''The id of node that is spawned as this node's proxy.'''

  def spawned_a_kid(self, kid):
    '''
    Called when a child responds with hello_parent.
    '''
    if kid['id'] == self._proxy_adjacent_id:
      self._spawn_proxy(kid)
    elif kid['id'] == self._proxy_id:
      self._finished_bumping(kid)

  def _finished_bumping(self, proxy_handle):
    if len(self._node.kids) != 2:
      raise errors.InternalError("A computation node should have exactly 2 kids after it finishes bumping.")

    # Pass on the right_configurations of self (as in dist_zero.connector.Spawner when the right gap child is spawned)
    self._node.send(proxy_handle,
                    messages.migration.configure_right_parent(
                        migration_id=None, kid_ids=list(self._node._connector._right_configurations.keys())))
    self._node.send(proxy_handle,
                    messages.migration.configure_new_flow_right(None, [
                        messages.migration.right_configuration(
                            parent_handle=self._node.transfer_handle(right_config['parent_handle'], proxy_handle['id']),
                            height=right_config['height'],
                            is_data=right_config['is_data'],
                            n_kids=right_config['n_kids'],
                            connection_limit=right_config['connection_limit'],
                        ) for right_config in self._node._connector._right_configurations.values()
                    ]))

    left_root, = self._node._senders.keys()
    left_configuration = self._node._connector._left_configurations[left_root]
    left_configuration['kids'] = [{
        'connection_limit': self._node.system_config['SUM_NODE_SENDER_LIMIT'],
        'handle': self._left_proxy
    }]
    left_configuration['height'] += 1
    self._node.bumped_to_new_connector({left_root: left_configuration}, self._proxy_adjacent_id, self._proxy_id)

  def _spawn_proxy(self, proxy_adjacent_handle):
    '''
    After an adjacent node bumps its height,
    a proxy for the adjacent will be spawned (its id will be stored in ``self._proxy_adjacent_id``)
    Once that proxy has reported that it is up and running, this node will call ``_spawn_proxy`` to
    spawn the second node to adopt the remaining kids of self as part of the process of bumping height.
    '''
    self._proxy_id = ids.new_id('ComputationNode_proxy')
    if self._proxy_adjacent_variant == 'input':
      senders = [self._node.transfer_handle(proxy_adjacent_handle, self._proxy_id)]
      left_ids = [proxy_adjacent_handle['id']]
      receivers = []
    elif self._proxy_adjacent_variant == 'output':
      import ipdb
      ipdb.set_trace()
      # FIXME(KK): Rethink, test and implement
      senders = []
      receivers = [self._node.transfer_handle(proxy_adjacent_handle, self._proxy_id)]
    else:
      raise errors.InternalError("Unrecognized variant {}".format(self._proxy_adjacent_variant))

    self._node.send(proxy_adjacent_handle,
                    messages.migration.configure_right_parent(migration_id=None, kid_ids=[self._proxy_id]))

    self._node._controller.spawn_node(
        messages.computation.computation_node_config(
            node_id=self._proxy_id,
            parent=self._node.new_handle(self._proxy_id),
            height=self._node.height,
            left_is_data=False,
            right_is_data=False,
            configure_right_parent_ids=[self._node.id],
            adoptees=[
                self._node.transfer_handle(kid, self._proxy_id) for kid in self._node.kids.values()
                if kid['id'] != proxy_adjacent_handle['id']
            ],
            connector=self._node._connector.non_left_part_json(),
            left_ids=left_ids,
            senders=senders,
            receiver_ids=[], # Make sure not actually send based on the right config for a differently layered parent.
            migrator=None))

  def respond_to_bumped_height(self, proxy, kid_ids, variant):
    '''Called in response to an adjacent node informing self that it has bumped its height.'''
    self._proxy_adjacent_id = ids.new_id('ComputationNode_{}_proxy_adjacent'.format(variant))
    self._proxy_adjacent_variant = variant
    self._left_proxy = proxy
    if variant == 'input':
      left_root, = self._node._senders.keys()
      senders = [self._node.transfer_handle(proxy, self._proxy_adjacent_id)]
      receivers = [] # The receiver will be added later
      configure_right_parent_ids = [self._node.id]
      adoptee_ids = [
          computation_kid_id for io_kid in kid_ids
          for computation_kid_id in self._node._connector.graph.node_receivers(io_kid)
      ]
      left_ids = [proxy['id']]
    elif variant == 'output':
      import ipdb
      ipdb.set_trace()
      senders = [] # The sender will be added later
      receivers = [self._node.transfer_handle(proxy, self._proxy_adjacent_id)]
      adoptee_ids = [
          computation_kid_id for io_kid in kid_ids
          for computation_kid_id in self._node._connector.graph.node_senders(io_kid)
      ]
    else:
      raise errors.InternalError("Unrecognized variant {}".format(variant))
    self._node._controller.spawn_node(
        messages.computation.computation_node_config(
            node_id=self._proxy_adjacent_id,
            parent=self._node.new_handle(self._proxy_adjacent_id),
            left_is_data=variant == 'input',
            right_is_data=variant == 'output',
            configure_right_parent_ids=configure_right_parent_ids,
            left_ids=left_ids,
            height=self._node.height,
            adoptees=[
                self._node.transfer_handle(self._node.kids[adoptee_id], self._proxy_adjacent_id)
                for adoptee_id in adoptee_ids
            ],
            connector=self._node._connector.left_part_json(parent_id=self._node.id),
            senders=senders,
            receiver_ids=None,
            migrator=None))
