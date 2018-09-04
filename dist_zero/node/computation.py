import logging

from dist_zero import messages, ids, errors, network_graph

from .node import Node

logger = logging.getLogger(__name__)


class ComputationNode(Node):
  def __init__(self, node_id, parent, height, senders, receivers, migrator_config, adoptees, controller):
    self.id = node_id
    self._controller = controller

    self.parent = parent
    self.height = height
    self.kids = {}

    self._graph = network_graph.NetworkGraph()

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

    if migrator_config is None:
      self._senders = {sender['id']: sender for sender in senders}
    else:
      self._senders = {sender['id']: sender for sender in migrator_config['senders']}

    self._initial_migrator_config = migrator_config
    self._initial_migrator = None # It will be initialized later.

    self._receivers = {receiver['id']: receiver for receiver in receivers}

    super(ComputationNode, self).__init__(logger)

    for sender_id in self._senders.keys():
      self._deltas.add_sender(sender_id)

  def is_data(self):
    return False

  def set_graph(self, graph):
    self._graph = graph

  def checkpoint(self, before=None):
    pass

  def activate_swap(self, migration_id, new_receiver_ids, kids, use_output=False, use_input=False):
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
    else:
      if self.parent is not None and not self._pending_adoptees:
        self._send_hello_parent()

      for sender in self._senders.values():
        self.send(sender, messages.sum.added_receiver(self.new_handle(sender['id'])))

      for receiver in self._receivers.values():
        self.send(receiver, messages.sum.added_sender(self.new_handle(receiver['id'])))

    self.linker.initialize()

  def _send_hello_parent(self):
    self.send(self.parent, messages.io.hello_parent(self.new_handle(self.parent['id'])))

  @staticmethod
  def from_config(node_config, controller):
    return ComputationNode(
        node_id=node_config['id'],
        parent=node_config['parent'],
        height=node_config['height'],
        senders=node_config['senders'],
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

  def _pick_new_receiver_for_kid(self):
    '''
    Return a node in self._graph that could function as a receiver for a newly added kid.
    or None if no node is appropriate.
    '''
    possible = list(self.kids.values())
    if len(possible) == 0:
      return None

    io_ids = [node for node in self._graph.nodes() if node.startswith('LeafNode') or node.startswith('InternalNode')]
    io_adjacents = set(adjacent for leaf_id in io_ids for adjacent in self._graph.node_receivers(leaf_id))

    non_adjacent_kids = list(
        sorted(
            (receiver for receiver in possible if receiver['id'] not in io_adjacents),
            key=lambda receiver: len(self._graph.node_senders(receiver['id'])),
        ))

    if not non_adjacent_kids:
      return None

    return self._controller.random.choice(non_adjacent_kids)

  def _pick_new_sender_for_kid(self):
    '''
    Return a node in self._graph that could function as a sender for a newly added kid.
    or None if no node is appropriate.
    '''
    possible = list(self.kids.values())
    if len(possible) == 0:
      return None

    io_ids = [node for node in self._graph.nodes() if node.startswith('LeafNode') or node.startswith('InternalNode')]
    io_adjacents = set(adjacent for leaf_id in io_ids for adjacent in self._graph.node_senders(leaf_id))

    non_adjacent_kids = list(
        sorted(
            (sender for sender in possible if sender['id'] not in io_adjacents),
            key=lambda sender: len(self._graph.node_receivers(sender['id'])),
        ))

    if not non_adjacent_kids:
      return None

    return self._controller.random.choice(non_adjacent_kids)

  def _adjacent_node_bumped_height(self, proxy, kid_ids, variant):
    '''Called in response to an adjacent node informing self that it has bumped its height.'''
    node_id = ids.new_id('ComputationNode_{}_proxy_adjacent'.format(variant))
    self._proxy_adjacent_variant = variant
    if variant == 'input':
      senders = [self.transfer_handle(proxy, node_id)]
      receivers = [] # The receiver will be added later
      adoptee_ids = [
          computation_kid_id for io_kid in kid_ids for computation_kid_id in self._graph.node_receivers(io_kid)
      ]
    elif variant == 'output':
      senders = [] # The sender will be added later
      receivers = [self.transfer_handle(proxy, node_id)]
      adoptee_ids = [
          computation_kid_id for io_kid in kid_ids for computation_kid_id in self._graph.node_senders(io_kid)
      ]
    else:
      raise errors.InternalError("Unrecognized variant {}".format(variant))
    self._proxy_adjacent_id = node_id
    self._controller.spawn_node(
        messages.computation.computation_node_config(
            node_id=node_id,
            parent=self.new_handle(node_id),
            height=self.height,
            adoptees=[self.transfer_handle(self.kids[adoptee_id], node_id) for adoptee_id in adoptee_ids],
            senders=senders,
            receivers=receivers,
            migrator=None))

  def _spawn_proxy(self, proxy_adjacent_handle):
    '''
    After an adjacent node bumps it's height,
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
            adoptees=[self.transfer_handle(kid, node_id) for kid in self.kids.values()],
            senders=senders,
            receivers=receivers,
            migrator=None))

    self.kids[proxy_adjacent_handle['id']] = proxy_adjacent_handle

  def _adjacent_node_added_kid(self, message):
    kid = message['kid']
    is_leaf = message['height'] == 0
    node_id = ids.new_id('{}_{}_adjacent'.format(
        'SumNode' if is_leaf else 'ComputationNode',
        message['variant'],
    ))

    if message['variant'] == 'input':
      self._graph.add_node(node_id)
      self._graph.add_node(kid['id'])
      self._graph.add_edge(kid['id'], node_id)
      receiver = self._pick_new_receiver_for_kid()
      sender = self.transfer_handle(handle=kid, for_node_id=node_id)
      if receiver is None:
        receivers = []
        # FIXME(KK): Ensure receivers is set later.
        import ipdb
        ipdb.set_trace()
      else:
        receivers = [self.transfer_handle(receiver, node_id)]
        self._graph.add_edge(node_id, receiver['id'])
      if is_leaf:
        self._controller.spawn_node(
            messages.sum.sum_node_config(
                node_id=node_id,
                senders=[],
                parent=self.new_handle(node_id),
                receivers=receivers,
                input_node=sender,
            ))
      else:
        self._controller.spawn_node(
            messages.computation.computation_node_config(
                node_id=node_id,
                parent=self.new_handle(node_id),
                height=self.height - 1,
                senders=[sender],
                receivers=receivers,
                migrator=None,
            ))
    elif message['variant'] == 'output':
      self._graph.add_node(node_id)
      self._graph.add_node(kid['id'])
      self._graph.add_edge(node_id, kid['id'])
      sender = self._pick_new_sender_for_kid()
      receiver = self.transfer_handle(handle=kid, for_node_id=node_id)
      if sender is None:
        senders = []
        # FIXME(KK): Ensure senders is set later.
        import ipdb
        ipdb.set_trace()
      else:
        senders = [self.transfer_handle(sender, node_id)]
        self._graph.add_edge(sender['id'], node_id)
      if is_leaf:
        self._controller.spawn_node(
            messages.sum.sum_node_config(
                node_id=node_id,
                parent=self.new_handle(node_id),
                senders=senders,
                receivers=[],
                output_node=receiver,
            ))
      else:
        self._controller.spawn_node(
            messages.computation.computation_node_config(
                node_id=node_id,
                parent=self.new_handle(node_id),
                height=self.height - 1,
                senders=senders,
                receivers=[receiver],
                migrator=None,
            ))
    else:
      raise errors.InternalError("Unrecognized variant {}".format(message['variant']))

  def _finished_bumping(self, proxy_handle):
    self.kids[proxy_handle['id']] = proxy_handle
    if len(self.kids) != 2:
      raise errors.InternalError("A computation node should have exactly 2 kids after it finishes bumping.")

    self._graph = network_graph.NetworkGraph()
    self._graph.add_node(self._proxy_adjacent_id)
    self._graph.add_node(self._proxy_id)
    if self._proxy_adjacent_variant == 'input':
      self._graph.add_edge(self._proxy_adjacent_id, self._proxy_id)
    elif self._proxy_adjacent_variant == 'outpu':
      self._graph.add_edge(self._proxy_id, self._proxy_adjacent_id)
    else:
      raise errors.InternalError("Unrecognized variant {}".format(self._proxy_adjacent_variant))

    self._proxy_adjacent_id = None
    self._proxy_adjacent_variant = None
    self._proxy_id = None

  def receive(self, message, sender_id):
    if message['type'] == 'added_sender':
      self._senders[sender_id] = message['node']
    elif message['type'] == 'added_receiver':
      self._receivers[sender_id] = message['node']
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
      self.kids[sender_id] = message['kid']
    elif message['type'] == 'goodbye_parent':
      if sender_id not in self.kids:
        raise errors.InternalError(
            "Got a goodbye_parent from a node that is not a kid of self.", extra={'kid_id': sender_id})
      self.kids.pop(sender_id)
    elif message['type'] == 'bumped_height':
      self._adjacent_node_bumped_height(proxy=message['proxy'], kid_ids=message['kid_ids'], variant=message['variant'])
    elif message['type'] == 'added_adjacent_kid':
      self._adjacent_node_added_kid(message)
    else:
      super(ComputationNode, self).receive(message=message, sender_id=sender_id)

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
