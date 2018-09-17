import logging
from collections import defaultdict

from dist_zero import messages, ids, errors, network_graph, connector
from dist_zero import topology_picker
from dist_zero.migration.right_configuration import ConfigurationReceiver

from .node import Node

logger = logging.getLogger(__name__)


class ComputationNode(Node):
  def __init__(self, node_id, configure_right_parent_ids, left_is_data, right_is_data, parent, height, senders,
               left_ids, receivers, migrator_config, adoptees, controller):
    self.id = node_id
    self._controller = controller

    # Later, these will be initialized to booleans
    self._left_gap = None
    self._right_gap = None

    self.left_is_data = left_is_data
    self.right_is_data = right_is_data

    self.parent = parent
    self.height = height
    self.kids = {}

    self._adoptees = adoptees
    self._pending_adoptees = None

    self._proxy_adjacent_id = None
    '''
    When responding to a proxy spawn by an adjacent `InternalNode`, this
    will be equal to the id of node that is spawned adjacent to the `InternalNode`'s proxy.
    '''
    self._proxy_adjacent_variant = None

    self._proxy_id = None
    '''
    When responding to a proxy spawn by an adjacent `InternalNode`, this
    will be equal to the id of node that is spawned as this node's proxy.
    '''

    self._exporters = {}

    self._senders = {sender['id']: sender for sender in senders}

    self._initial_migrator_config = migrator_config
    self._initial_migrator = None # It will be initialized later.

    self._receivers = {receiver['id']: receiver for receiver in receivers}

    # Sometimes, kids will be spawned without appropriate senders/receivers.
    # When that happens they will be temporarily added to these sets.
    # Once the kid says hello, it will be removed from this set once it is arranged that the kid
    # get the required senders/receivers.
    self._kids_missing_receivers = set()
    self._kids_missing_senders = set()

    self.left_ids = left_ids

    self._configuration_receiver = ConfigurationReceiver(
        node=self, configure_right_parent_ids=configure_right_parent_ids, left_ids=self.left_ids)
    self._configure_right_parent_ids = configure_right_parent_ids
    self._right_configurations_are_sent = False
    self._left_configurations_are_sent = False

    super(ComputationNode, self).__init__(logger)

    for sender_id in self._senders.keys():
      self._deltas.add_sender(sender_id)

    self._connector = None
    self._spawner = None
    self._incremental_spawner = None

    # FIXME(KK): Remove
    # topology_picker.OldTopologyPicker(
    #    graph=network_graph.NetworkGraph(),
    #    left_is_data=self.left_is_data,
    #    right_is_data=self.right_is_data,
    #    # TODO(KK): There is probably a better way to configure these standard limits than the below.
    #    # Look into it, write up some notes, and fix it.
    #    new_node_max_outputs=self.system_config['SUM_NODE_RECEIVER_LIMIT'],
    #    new_node_max_inputs=self.system_config['SUM_NODE_SENDER_LIMIT'],
    #    new_node_name_prefix='SumNode' if self.height == 0 else 'ComputationNode',
    #)

  def is_data(self):
    return False

  def checkpoint(self, before=None):
    pass

  def activate_swap(self, migration_id, kids, use_output=False, use_input=False):
    self.kids.update(kids)

  def initialize(self):
    self.logger.info(
        'Starting internal computation node {computation_node_id}', extra={
            'computation_node_id': self.id,
        })
    if self._adoptees is not None:
      self._pending_adoptees = {adoptee['id'] for adoptee in self._adoptees}
      for adoptee in self._adoptees:
        self.send(adoptee, messages.io.adopt(self.new_handle(adoptee['id'])))

    if self._initial_migrator_config:
      self._initial_migrator = self.attach_migrator(self._initial_migrator_config)

    if self._adoptees is None and self.parent and not self.kids:
      self._send_hello_parent()

    self._send_configure_right_to_left()
    self._configuration_receiver.initialize()

    self.linker.initialize()

  def _send_hello_parent(self):
    self.send(self.parent, messages.io.hello_parent(self.new_handle(self.parent['id'])))

  @staticmethod
  def from_config(node_config, controller):
    return ComputationNode(
        node_id=node_config['id'],
        left_is_data=node_config['left_is_data'],
        right_is_data=node_config['right_is_data'],
        configure_right_parent_ids=node_config['configure_right_parent_ids'],
        parent=node_config['parent'],
        height=node_config['height'],
        senders=node_config['senders'],
        left_ids=node_config['left_ids'],
        receivers=node_config['receivers'],
        adoptees=node_config['adoptees'],
        migrator_config=node_config['migrator'],
        controller=controller)

  def elapse(self, ms):
    pass

  def deliver(self, message, sequence_number, sender_id):
    pass

  def send_forward_messages(self, before=None):
    return 1 + self._linker.advance_sequence_number()

  def export_to_node(self, receiver):
    if receiver['id'] in self._exporters:
      raise errors.InternalError("Already exporting to this node.", extra={'existing_node_id': receiver['id']})
    self._exporters[receiver['id']] = self.linker.new_exporter(receiver=receiver)

  def _pick_new_receivers_for_kid(self):
    '''
    Return a list of nodes in self._graph that should function as receivers for a newly added kid.
    or None if no list is appropriate.
    '''
    if self.left_is_data:
      if len(self._connector.layers) >= 3:
        return [self.kids[node_id] for node_id in self._picker.get_layer(2)]
      else:
        return None
    else:
      if len(self._connector.layers) >= 2:
        return [self.kids[node_id] for node_id in self._picker.get_layer(1)]
      else:
        return None

  def _pick_new_sender_for_kid(self):
    '''
    Return a list of nodes in self._graph that should function as senders for a newly added kid.
    or None if no list is appropriate.
    '''
    if self.right_is_data:
      if len(self._connector.layers) >= 2:
        return [self.kids[node_id] for node_id in self._connector.layers[len(self._connector.layers) - 1]]
      else:
        return None
    else:
      if len(self._connector.layers) >= 1:
        return [self.kids[node_id] for node_id in self._picker.layers[len(self._connector.layers) - 1]]
      else:
        return None

  def _adjacent_node_bumped_height(self, proxy, kid_ids, variant):
    '''Called in response to an adjacent node informing self that it has bumped its height.'''
    node_id = ids.new_id('ComputationNode_{}_proxy_adjacent'.format(variant))
    self._proxy_adjacent_variant = variant
    if variant == 'input':
      senders = [self.transfer_handle(proxy, node_id)]
      receivers = [] # The receiver will be added later
      import ipdb
      ipdb.set_trace()
      # FIXME(KK): Figure out what to do here.
      return
      adoptee_ids = [
          computation_kid_id for io_kid in kid_ids for computation_kid_id in self._picker.graph.node_receivers(io_kid)
      ]
    elif variant == 'output':
      senders = [] # The sender will be added later
      receivers = [self.transfer_handle(proxy, node_id)]
      adoptee_ids = [
          computation_kid_id for io_kid in kid_ids for computation_kid_id in self._picker.graph.node_senders(io_kid)
      ]
    else:
      raise errors.InternalError("Unrecognized variant {}".format(variant))
    self._proxy_adjacent_id = node_id
    self._controller.spawn_node(
        messages.computation.computation_node_config(
            node_id=node_id,
            parent=self.new_handle(node_id),
            left_is_data=variant == 'input',
            right_is_data=variant == 'output',
            height=self.height,
            adoptees=[self.transfer_handle(self.kids[adoptee_id], node_id) for adoptee_id in adoptee_ids],
            senders=senders,
            receivers=receivers,
            migrator=None))

  def all_incremental_kids_are_spawned(self):
    last_node_ids = self._incremental_spawner.layers[-1]
    if any(not self._incremental_spawner.graph.node_receivers(node_id) for node_id in last_node_ids):
      import ipdb
      ipdb.set_trace()
      # Missing receivers, we should be sending update_left_configuration to our right siblings.
    else:
      for node_id in last_node_ids:
        for receiver_id in self._incremental_spawner.graph.node_receivers(node_id):
          self.send(self.kids[receiver_id],
                    messages.migration.added_sender(self.transfer_handle(self.kids[node_id], receiver_id)))

    self._incremental_spawner = None

  def all_kids_are_spawned(self, left_gap, right_gap):
    self._spawner = None
    if left_gap and right_gap:
      raise errors.InternalError("There may not be both a left and a right gap.")
    self._left_gap = left_gap
    self._right_gap = right_gap
    if self._left_configurations_are_sent:
      raise errors.InternalError("all_kids_are_spawned should only be called before left_configurations are sent.")

    self._send_configure_left_to_right()
    if self.parent:
      self._send_hello_parent()

  def _send_configure_left_to_right(self):
    self._left_configurations_are_sent = True

    self.logger.info("Sending configure_new_flow_left", extra={'receiver_ids': list(self._receivers.keys())})

    receiver_to_kids = self._receiver_to_kids()

    for receiver in self._connector.right_siblings.values():
      message = messages.migration.configure_new_flow_left(self.migration_id, [
          messages.migration.left_configuration(
              node=self.new_handle(receiver['id']),
              height=self.height,
              is_data=False,
              kids=[{
                  'handle': self.transfer_handle(self.kids[kid_id], receiver['id']),
                  'connection_limit': self.system_config['SUM_NODE_RECEIVER_LIMIT']
              } for kid_id in receiver_to_kids.get(receiver['id'], [])],
          )
      ])

      self.send(receiver, message)

  def _spawn_proxy(self, proxy_adjacent_handle):
    '''
    After an adjacent node bumps its height,
    a proxy for the adjacent will be spawned (its id will be stored in ``self._proxy_adjacent_id``)
    Once that proxy has reported that it is up and running, this node will call ``_spawn_proxy`` to
    spawn the second node to adopt the remaining kids of self as part of the process of bumping height.
    '''
    node_id = ids.new_id('ComputationNode_proxy')
    if self._proxy_adjacent_variant == 'input':
      senders = [self.transfer_handle(proxy_adjacent_handle, node_id)]
      receivers = []
    elif self._proxy_adjacent_variant == 'output':
      senders = []
      receivers = [self.transfer_handle(proxy_adjacent_handle, node_id)]
    else:
      raise errors.InternalError("Unrecognized variant {}".format(self._proxy_adjacent_variant))

    self._proxy_id = node_id
    self._controller.spawn_node(
        messages.computation.computation_node_config(
            node_id=node_id,
            parent=self.new_handle(node_id),
            height=self.height,
            left_is_data=False,
            right_is_data=False,
            adoptees=[self.transfer_handle(kid, node_id) for kid in self.kids.values()],
            senders=senders,
            receivers=receivers,
            migrator=None))

    self.kids[proxy_adjacent_handle['id']] = proxy_adjacent_handle

  def spawn_kid(self, layer_index, node_id, senders, configure_right_parent_ids, left_ids, migrator):
    self.kids[node_id] = None
    if self.height == 0:
      self._controller.spawn_node(
          messages.sum.sum_node_config(
              node_id=node_id,
              left_is_data=self.left_is_data and layer_index == 1,
              # TODO(KK): This business about right_map here is very ugly.
              # Think hard and try to come up with a way to avoid it.
              right_is_data=self.right_is_data and layer_index + 1 == len(self._connector.layers)
              and bool(self._connector.right_to_parent_ids[node_id]),
              senders=senders,
              receivers=[],
              configure_right_parent_ids=configure_right_parent_ids,
              parent=self.new_handle(node_id),
              migrator=migrator,
          ))
    else:
      self._controller.spawn_node(
          messages.computation.computation_node_config(
              node_id=node_id,
              configure_right_parent_ids=configure_right_parent_ids,
              parent=self.new_handle(node_id),
              left_ids=left_ids,
              height=self.height - 1,
              left_is_data=self.left_is_data and layer_index == 1,
              # TODO(KK): This business about right_map here is very ugly.
              #   Think hard and try to come up with a way to avoid it.
              right_is_data=self.right_is_data and layer_index + 1 == len(self._connector.layers)
              and bool(self._connector.right_to_parent_ids[node_id]),
              senders=senders,
              receivers=[],
              migrator=migrator))

  def _update_left_configuration(self, message):
    if self._left_gap:
      # FIXME(KK): Test this, and implement by forwarding the update_left_configuration message to the proper child.
      import ipdb
      ipdb.set_trace()
    else:
      if len(message['new_kids']) != 1:
        # FIXME(KK): The current implementation handles only a special case.
        import ipdb
        ipdb.set_trace()
        raise errors.InternalError("Not Yet Implemented")
      for kid in message['new_kids']:
        _lookup = lambda nid: kid if kid['id'] == nid else self.kids[nid]
        if self._connector is None:
          import ipdb
          ipdb.set_trace()

        #if self.id == 'ComputationNode_adjacent_gL1atm1WWIU6':
        #  import ipdb; ipdb.set_trace()

        new_layers, edges, hourglasses = self._connector.add_kid_to_left_configuration(
            parent_id=message['parent_id'], kid=kid)

        self._incremental_spawner = connector.IncrementalSpawner(
            new_layers=new_layers, connector=self._connector, node=self)
        self._incremental_spawner.start_spawning()

        if len(edges) >= 2 or (len(edges) == 1 and node_changes):
          raise errors.InternalError("Received a disallowed combination of results"
                                     " from Connector.add_kid_to_left_configuration")

        for src_id, tgt_id in edges:
          src, tgt = self.kids[src_id], self.kids[tgt_id]
          import ipdb
          ipdb.set_trace()

        # FIXME(KK): Move this into probably a _update_right_configuration message.
        #import ipdb
        #ipdb.set_trace()
        #self._picker.graph.add_node(kid['id'])
        #self._picker.layers[0].append(kid['id'])

        #if self.right_is_data:
        #  node_id = ids.new_id('{}_output_adjacent'.format('SumNode' if is_leaf else 'ComputationNode', ))
        #  self._picker.graph.add_node(node_id)
        #  self._picker.graph.add_edge(node_id, kid['id'])
        #  senders = self._pick_new_sender_for_kid()

        #  if senders is None:

        #    # Tell a parent receiver to find an actual sender for this kid
        #    self._kids_missing_senders.add(node_id)
        #  else:
        #    senders = [self.transfer_handle(sender, node_id) for sender in senders]
        #    for sender in senders:
        #      self._picker.graph.add_edge(sender['id'], node_id)
        #  self._spawn_node(
        #      is_leaf=is_leaf,
        #      left=False,
        #      node_id=node_id,
        #      senders=senders,
        #      receivers=[self.transfer_handle(handle=kid, for_node_id=node_id)])
        #else:
        #  import ipdb
        #  ipdb.set_trace()

  def _finished_bumping(self, proxy_handle):
    self.kids[proxy_handle['id']] = proxy_handle
    if len(self.kids) != 2:
      raise errors.InternalError("A computation node should have exactly 2 kids after it finishes bumping.")

    import ipdb
    ipdb.set_trace()
    self._graph = network_graph.NetworkGraph()
    self._picker.graph.add_node(self._proxy_adjacent_id)
    self._picker.graph.add_node(self._proxy_id)
    if self._proxy_adjacent_variant == 'input':
      self._picker.graph.add_edge(self._proxy_adjacent_id, self._proxy_id)
    elif self._proxy_adjacent_variant == 'output':
      self._picker.graph.add_edge(self._proxy_id, self._proxy_adjacent_id)
    else:
      raise errors.InternalError("Unrecognized variant {}".format(self._proxy_adjacent_variant))

    self._proxy_adjacent_id = None
    self._proxy_adjacent_variant = None
    self._proxy_id = None

  @property
  def migration_id(self):
    if self._initial_migrator is not None:
      return self._initial_migrator.migration_id
    else:
      return None

  def _send_configure_right_to_left(self):
    if not self._right_configurations_are_sent:
      self.logger.info("Sending configure_new_flow_right", extra={'receiver_ids': list(self._senders.keys())})
      for sender in self._senders.values():
        self.send(sender,
                  messages.migration.configure_new_flow_right(self.migration_id, [
                      messages.migration.right_configuration(
                          n_kids=None,
                          parent_handle=self.new_handle(sender['id']),
                          height=self.height,
                          is_data=False,
                          connection_limit=self.system_config['SUM_NODE_SENDER_LIMIT'],
                      )
                  ]))
      self._right_configurations_are_sent = True

  @property
  def fully_configured(self):
    return self._connector is not None

  def has_left_and_right_configurations(self, left_configurations, right_configurations):
    self.logger.info("Insertion migrator has received all Left and Right configurations. Ready to spawn.")

    for right_config in right_configurations.values():
      parent = right_config['parent_handle']
      self.export_to_node(parent)
      self._receivers[parent['id']] = parent

    if self._connector is not None:
      import ipdb
      ipdb.set_trace()

    self._connector = connector.Connector(
        height=self.height,
        left_configurations=left_configurations,
        left_is_data=self.left_is_data,
        right_is_data=self.right_is_data,
        right_configurations=right_configurations,
        max_outputs=self.system_config['SUM_NODE_RECEIVER_LIMIT'],
        max_inputs=self.system_config['SUM_NODE_SENDER_LIMIT'],
    )
    self._spawner = connector.Spawner(node=self, connector=self._connector)

    self.height = self._connector.max_height()

    self._spawner.start_spawning()

  def new_left_configurations(self, left_configurations):
    '''For when a fully configured node gets new left_configurations'''
    if len(left_configurations) != 1:
      import ipdb
      ipdb.set_trace()
      raise errors.InternalError("Not Yet Implemented")

    for left_configuration in left_configurations:
      new_layers, edges, hourglasses = self._connector.add_left_configuration(left_configuration)

      if new_layers:
        self._incremental_spawner = connector.IncrementalSpawner(
            new_layers=new_layers, connector=self._connector, node=self)
        self._incremental_spawner.start_spawning()

      if edges:
        left_kid_to_handle = {
            kid['handle']['id']: kid['handle']
            for left_config in left_configurations for kid in left_config['kids']
        }
        src_to_tgts = defaultdict(list)
        for src_id, tgt_id in edges:
          src_to_tgts[src_id].append(tgt_id)
          # TODO(KK): possibly refactor this bit, the return types from add_left_configuration and add_kid_to_left_configuration
          # look highly suspicious.
          src, tgt = left_kid_to_handle[src_id], self.kids[tgt_id]
          self.send(tgt, messages.migration.added_sender(self.transfer_handle(src, tgt['id'])))

        for src_id, tgt_ids in src_to_tgts.items():
          self.send(left_kid_to_handle[src_id],
                    messages.migration.configure_right_parent(migration_id=None, kid_ids=tgt_ids))

  def new_right_configurations(self, right_configurations):
    '''For when a fully configured node gets new right_configurations'''
    import ipdb
    ipdb.set_trace()

  def receive(self, message, sender_id):
    if self._configuration_receiver.receive(message=message, sender_id=sender_id):
      return
    elif message['type'] == 'added_receiver':
      self._receivers[message['node']['id']] = message['node']
    elif message['type'] == 'added_sender':
      node = message['node']
      self._senders[node['id']] = node
      self.send(node,
                messages.migration.configure_new_flow_right(None, [
                    messages.migration.right_configuration(
                        n_kids=None,
                        parent_handle=self.new_handle(node['id']),
                        height=self.height,
                        is_data=False,
                        connection_limit=self.system_config['SUM_NODE_SENDER_LIMIT'],
                    )
                ]))
    elif message['type'] == 'adopt':
      if self.parent is None:
        raise errors.InternalError("Root nodes may not adopt a new parent.")
      self.send(self.parent, messages.io.goodbye_parent())
      self.parent = message['new_parent']
      self._send_hello_parent()
    elif message['type'] == 'hello_parent':
      if sender_id == self._proxy_adjacent_id:
        self._spawn_proxy(message['kid'])
      elif sender_id == self._proxy_id:
        self._finished_bumping(message['kid'])
      elif self._pending_adoptees is not None and sender_id in self._pending_adoptees:
        self._pending_adoptees.remove(sender_id)
        if not self._pending_adoptees:
          self._pending_adoptees = None
          self._send_hello_parent()
      elif sender_id in self._kids_missing_receivers:
        self._kids_missing_receivers.remove(sender_id)
        for receiver in self._receivers.values():
          self.send(receiver,
                    messages.migration.update_left_configuration(
                        parent_id=self.id, new_kids=[message['kid']], height=self.height))
      elif sender_id in self._kids_missing_senders:
        self._kids_missing_senders.remove(sender_id)
        for sender in self._senders.values():
          import ipdb
          ipdb.set_trace()
          # FIXME(KK): We should use an update_right_configuration message here instead.
          self.send(sender, messages.io.added_sibling_kid(height=self.height, variant='output', kid=message['kid']))
      self.kids[sender_id] = message['kid']
      if self._spawner is not None:
        self._spawner.spawned_a_kid(message['kid'])
      elif self._incremental_spawner is not None:
        self._incremental_spawner.spawned_a_kid(message['kid'])
    elif message['type'] == 'goodbye_parent':
      if sender_id not in self.kids:
        raise errors.InternalError(
            "Got a goodbye_parent from a node that is not a kid of self.", extra={'kid_id': sender_id})
      self.kids.pop(sender_id)
    elif message['type'] == 'bumped_height':
      self._adjacent_node_bumped_height(proxy=message['proxy'], kid_ids=message['kid_ids'], variant=message['variant'])
    elif message['type'] == 'update_left_configuration':
      self._update_left_configuration(message)
    else:
      super(ComputationNode, self).receive(message=message, sender_id=sender_id)

  def _receiver_to_kids(self):
    if self._connector is not None:
      result = {}
      for right_node_id, receiver_ids in self._connector.right_to_parent_ids.items():
        for receiver_id in receiver_ids:
          if receiver_id not in result:
            result[receiver_id] = [right_node_id]
          else:
            result[receiver_id].append(right_node_id)
      return result
    else:
      return {receiver_id: [] for receiver_id in self._receivers.keys()}

  def stats(self):
    return {
        'height': self.height,
        'n_retransmissions': self.linker.n_retransmissions,
        'n_reorders': self.linker.n_reorders,
        'n_duplicates': self.linker.n_duplicates,
        'sent_messages': self.linker.least_unused_sequence_number,
        'acknowledged_messages': self.linker.least_unacknowledged_sequence_number(),
    }

  def handle_api_message(self, message):
    if message['type'] == 'get_kids':
      return self.kids
    elif message['type'] == 'get_senders':
      return self._senders
    elif message['type'] == 'get_receivers':
      return self._receivers
    else:
      return super(ComputationNode, self).handle_api_message(message)
