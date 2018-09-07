import logging

from dist_zero import messages, ids, errors, network_graph
from dist_zero import topology_picker

from .node import Node

logger = logging.getLogger(__name__)


class ComputationNode(Node):
  def __init__(self, node_id, left_is_data, right_is_data, parent, height, senders, receivers, migrator_config,
               adoptees, controller):
    self.id = node_id
    self._controller = controller

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

    if migrator_config is None:
      self._senders = {sender['id']: sender for sender in senders}
    else:
      self._senders = {sender['id']: sender for sender in migrator_config['senders']}

    self._initial_migrator_config = migrator_config
    self._initial_migrator = None # It will be initialized later.

    self._receivers = {receiver['id']: receiver for receiver in receivers}

    # Sometimes, kids will be spawned without appropriate senders/receivers.
    # When that happens they will be temporarily added to these sets.
    # Once the kid says hello, it will be removed from this set once it is arranged that the kid
    # get the required senders/receivers.
    self._kids_missing_receivers = set()
    self._kids_missing_senders = set()

    super(ComputationNode, self).__init__(logger)

    for sender_id in self._senders.keys():
      self._deltas.add_sender(sender_id)

    self._picker = topology_picker.OldTopologyPicker(
        graph=network_graph.NetworkGraph(),
        left_is_data=self.left_is_data,
        right_is_data=self.right_is_data,
        # TODO(KK): There is probably a better way to configure these standard limits than the below.
        # Look into it, write up some notes, and fix it.
        new_node_max_outputs=self.system_config['SUM_NODE_RECEIVER_LIMIT'],
        new_node_max_inputs=self.system_config['SUM_NODE_SENDER_LIMIT'],
        new_node_name_prefix='SumNode' if self.height == 0 else 'ComputationNode',
    )

  def is_data(self):
    return False

  def set_picker(self, picker):
    self._picker = picker

  def checkpoint(self, before=None):
    pass

  def activate_swap(self, migration_id, new_receivers, kids, use_output=False, use_input=False):
    self._receivers.update(new_receivers.items())
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
        left_is_data=node_config['left_is_data'],
        right_is_data=node_config['right_is_data'],
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

  def _pick_new_receivers_for_kid(self):
    '''
    Return a list of nodes in self._graph that should function as receivers for a newly added kid.
    or None if no list is appropriate.
    '''
    if self.left_is_data:
      if self._picker.n_layers >= 3:
        return [self.kids[node_id] for node_id in self._picker.get_layer(2)]
      else:
        return None
    else:
      if self._picker.n_layers >= 2:
        return [self.kids[node_id] for node_id in self._picker.get_layer(1)]
      else:
        return None

  def _pick_new_sender_for_kid(self):
    '''
    Return a list of nodes in self._graph that should function as senders for a newly added kid.
    or None if no list is appropriate.
    '''
    if self.right_is_data:
      if self._picker.n_layers >= 2:
        return [self.kids[node_id] for node_id in self._picker.get_layer(self._picker.n_layers - 1)]
      else:
        return None
    else:
      if self._picker.n_layers >= 1:
        return [self.kids[node_id] for node_id in self._picker.get_layer(self._picker.n_layers - 1)]
      else:
        return None

  def _adjacent_node_bumped_height(self, proxy, kid_ids, variant):
    '''Called in response to an adjacent node informing self that it has bumped its height.'''
    node_id = ids.new_id('ComputationNode_{}_proxy_adjacent'.format(variant))
    self._proxy_adjacent_variant = variant
    if variant == 'input':
      senders = [self.transfer_handle(proxy, node_id)]
      receivers = [] # The receiver will be added later
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

  def _spawn_extreme_node(self, is_leaf, left, node_id, senders, receivers):
    '''spawn a node all the way at the left or right'''
    if is_leaf:
      self._controller.spawn_node(
          messages.sum.sum_node_config(
              node_id=node_id,
              left_is_data=self.left_is_data and left,
              right_is_data=self.right_is_data and not left,
              senders=senders,
              parent=self.new_handle(node_id),
              receivers=receivers,
          ))
    else:
      self._controller.spawn_node(
          messages.computation.computation_node_config(
              node_id=node_id,
              left_is_data=self.left_is_data and left,
              right_is_data=self.right_is_data and not left,
              parent=self.new_handle(node_id),
              height=self.height - 1,
              senders=senders,
              receivers=receivers,
              migrator=None,
          ))

  def _sibling_node_added_kid(self, message):
    kid = message['kid']

    is_leaf = message['height'] == 0

    if message['variant'] == 'input':
      if self.left_is_data:
        node_id = ids.new_id('{}_input_adjacent'.format('SumNode' if is_leaf else 'ComputationNode', ))
        receiver_ids = self._picker.complete_receivers_when_left_is_data(
            left_node_id=kid['id'], node_id=node_id, random=self._controller.random)
        if receiver_ids is None:
          receivers = []
          # Tell a parent receiver to find an actual receiver for this kid
          self._kids_missing_receivers.add(node_id)
        else:
          receivers = [self.transfer_handle(self.kids[receiver_id], node_id) for receiver_id in receiver_ids]
        self._spawn_extreme_node(
            is_leaf=is_leaf,
            left=True,
            node_id=node_id,
            senders=[self.transfer_handle(handle=kid, for_node_id=node_id)],
            receivers=receivers)
      else:
        receiver_ids = self._picker.complete_receivers(kid['id'], random=self._controller.random)
        if receiver_ids is not None:
          for receiver_id in receiver_ids:
            receiver = self.kids[receiver_id]
            self.send(kid, messages.sum.added_receiver(self.transfer_handle(receiver, kid['id'])))
            self.send(receiver, messages.sum.added_sender(self.transfer_handle(kid, receiver['id'])))
        else:
          import ipdb
          ipdb.set_trace()
    elif message['variant'] == 'output':
      self._picker.graph.add_node(kid['id'])
      self._picker.get_layer(0).append(kid['id'])

      if self.right_is_data:
        node_id = ids.new_id('{}_output_adjacent'.format('SumNode' if is_leaf else 'ComputationNode', ))
        self._picker.graph.add_node(node_id)
        self._picker.graph.add_edge(node_id, kid['id'])
        senders = self._pick_new_sender_for_kid()

        if senders is None:
          senders = []
          # Tell a parent receiver to find an actual sender for this kid
          self._kids_missing_senders.add(node_id)
        else:
          senders = [self.transfer_handle(sender, node_id) for sender in senders]
          for sender in senders:
            self._picker.graph.add_edge(sender['id'], node_id)
        self._spawn_extreme_node(
            is_leaf=is_leaf,
            left=False,
            node_id=node_id,
            senders=senders,
            receivers=[self.transfer_handle(handle=kid, for_node_id=node_id)])
      else:
        import ipdb
        ipdb.set_trace()
    else:
      raise errors.InternalError("Unrecognized variant {}".format(message['variant']))

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

  def receive(self, message, sender_id):
    if message['type'] == 'added_sender':
      self._senders[message['node']['id']] = message['node']
    elif message['type'] == 'added_receiver':
      self._receivers[message['node']['id']] = message['node']
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
          self.send(receiver, messages.io.added_sibling_kid(height=self.height, variant='input', kid=message['kid']))
      elif sender_id in self._kids_missing_senders:
        self._kids_missing_senders.remove(sender_id)
        for sender in self._senders.values():
          self.send(sender, messages.io.added_sibling_kid(height=self.height, variant='output', kid=message['kid']))
      self.kids[sender_id] = message['kid']
    elif message['type'] == 'goodbye_parent':
      if sender_id not in self.kids:
        raise errors.InternalError(
            "Got a goodbye_parent from a node that is not a kid of self.", extra={'kid_id': sender_id})
      self.kids.pop(sender_id)
    elif message['type'] == 'bumped_height':
      self._adjacent_node_bumped_height(proxy=message['proxy'], kid_ids=message['kid_ids'], variant=message['variant'])
    elif message['type'] == 'added_sibling_kid':
      self._sibling_node_added_kid(message)
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
