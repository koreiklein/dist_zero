import logging

import dist_zero.ids
from dist_zero import settings, messages, errors, recorded, importer, exporter, misc, ids, ticker
from dist_zero.network_graph import NetworkGraph
from dist_zero.node.node import Node

logger = logging.getLogger(__name__)


class InternalNode(Node):
  '''
  The root of a tree of `LeafNode` instances of the same ``variant``.

  Each `InternalNode` instance is responsible for keeping track of the state of its subtree, and for growing
  or shrinking it as necessary.  In particular, when new leaves are created, `InternalNode.create_kid_config` must
  be called on the desired immediate parent to generate the node config for starting that child.

  Each `InternalNode` will have an associated height.  The assignment of heights to internal nodes is the unique
  minimal assignment such that n.height+1 == n.parent.height for every node n that has a parent.
  '''

  def __init__(self, node_id, parent, variant, height, adjacent, adoptees, initial_state, controller):
    '''
    :param str node_id: The id to use for this node
    :param parent: If this node is the root, then `None`.  Otherwise, the :ref:`handle` of its parent `Node`.
    :type parent: :ref:`handle` or `None`
    :param str variant: 'input' or 'output'
    :param int height: The height of the node in the tree.  See `InternalNode`
    :param adjacent: The :ref:`handle` of the adjacent node or `None` if this node should not start with an adjacent.
    :type adjacent: :ref:`handle` or `None`
    :param adoptees: Nodes to adopt upon initialization.
    :type adoptees: list[:ref:`handle`]
    :param `MachineController` controller: The controller for this node.
    :param object initial_state: A json serializeable starting state for all leaves spawned from this node.
      This state is important for output leaves that update that state over time.
    '''
    self._controller = controller
    self._parent = parent
    self._variant = variant
    self._height = height
    self.id = node_id
    self._pending_adoptees = None if parent is None else {adoptee['id']: False for adoptee in adoptees}
    self._kids = {adoptee['id']: adoptee for adoptee in adoptees}
    self._kid_summaries = {}
    self._initial_state = initial_state
    self._adjacent = adjacent

    self._current_state = None

    self._leaving_kids = None
    '''
    None if this node is not merging with another node.
    Otherwise, the set of kids that must leave this node before it has lost all its kids and it's safe to terminate.
    '''

    self._merging_kid_ids = set()
    '''Set of kid ids of kids that are in process of merging with another kid.'''

    self._graph = NetworkGraph()

    self._root_proxy_id = None
    '''
    While in the process of bumping its height, the root node sets this to the id of the node that will take over as its
    proxy.
    '''

    self._root_consuming_proxy_id = None
    '''
    While in the process of decreasing its height, the root node sets this to the id of the proxy node that it is
    consuming.
    '''

    # To limit excessive warnings regarding being at low capacity.
    self._warned_low_capacity = False

    super(InternalNode, self).__init__(logger)

    self._ticker = ticker.Ticker(interval_ms=self.system_config['KID_SUMMARY_INTERVAL'])

    self._time_since_no_mergable_kids_ms = 0
    self._time_since_no_consumable_proxy = 0

  def is_data(self):
    return True

  @property
  def height(self):
    return self._height

  def get_adjacent_id(self):
    return None if self._adjacent is None else self._adjacent['id']

  def checkpoint(self, before=None):
    pass

  def sink_swap(self, deltas, old_sender_ids, new_senders, new_importers, linker):
    if self._variant == 'output':
      if len(new_senders) != 1:
        raise errors.InternalError(
            "sink_swap should be called on an edge internal node only when there is a unique new sender.")
      self._adjacent = new_senders[0]
    elif self._variant == 'input':
      raise errors.InternalError("An input InternalNode should never function as a sink node in a migration.")
    else:
      raise errors.InternalError('Unrecognized variant "{}"'.format(self._variant))

  def switch_flows(self, migration_id, old_exporters, new_exporters, new_receivers):
    if self._variant == 'input':
      if len(new_receivers) != 1:
        raise errors.InternalError("Not sure how to set an input node's receives to a list not of length 1.")
      if self._adjacent is not None and self._adjacent['id'] != new_receivers[0]['id']:
        raise errors.InternalError("Not sure how to set an input node's receives when it already has an adjacent.")
      self._adjacent = new_receivers[0]
    elif self._variant == 'output':
      raise errors.InternalError("Output InternalNode should never function as a source migrator in a migration.")
    else:
      raise errors.InternalError('Unrecognized variant "{}"'.format(self._variant))

    for kid in self._kids.values():
      self.send(kid, messages.migration.switch_flows(migration_id))

  def initialize(self):
    if self._kids:
      # Must adopt any kids that were added before initialization.
      for kid in self._kids.values():
        self.send(kid, messages.io.adopt(self.new_handle(kid['id'])))

    if self._adjacent is not None:
      if self._variant == 'input':
        self.logger.info("internal node sending 'set_input' message to adjacent node")
        self.send(self._adjacent, messages.io.set_input(self.new_handle(self._adjacent['id'])))
      elif self._variant == 'output':
        self.logger.info("internal node sending 'set_output' message to adjacent node")
        self.send(self._adjacent, messages.io.set_output(self.new_handle(self._adjacent['id'])))
      else:
        raise errors.InternalError("Unrecognized variant {}".format(self._variant))

    if self._height > 0 and len(self._kids) == 0:
      # unless we are height 0, we must have a new kid.
      self._spawn_kid()

    if self._parent is not None and not self._pending_adoptees:
      self._send_hello_parent()

  def _send_hello_parent(self):
    self.send(self._parent, messages.io.hello_parent(self.new_handle(self._parent['id'])))

  def _spawn_kid(self):
    if self._height == 0:
      raise errors.InternalError("height 0 InternalNode instances can not spawn kids")
    elif self._root_proxy_id is not None:
      raise errors.InternalError("Root nodes may not spawn new kids while their are bumping their height.")
    elif self._root_consuming_proxy_id is not None:
      raise errors.InternalError("Root nodes may not spawn new kids while their are decreasing their height "
                                 "by consuming a proxy.")
    else:
      node_id = ids.new_id("InternalNode_kid")
      self.logger.info("InternalNode is spawning a new kid", extra={'new_kid_id': node_id})
      self._controller.spawn_node(
          messages.io.internal_node_config(
              node_id=node_id,
              parent=self.new_handle(node_id),
              variant=self._variant,
              height=self._height - 1,
              adjacent=None,
              initial_state=self._initial_state,
              adoptees=[],
          ))

  def _check_for_kid_limits(self):
    '''In case the kids of self are hitting any limits, address them.'''
    if self._height > 0:
      self._check_for_low_capacity()

  def _check_for_consumable_proxy(self, ms):
    TIME_TO_WAIT_BEFORE_CONSUME_PROXY_MS = 4 * 1000

    if self._parent is None:
      if len(self._kids) == 1 and not self._root_consuming_proxy_id and self._height > 1:
        self._time_since_no_consumable_proxy += ms
        if self._time_since_no_consumable_proxy >= TIME_TO_WAIT_BEFORE_CONSUME_PROXY_MS:
          self._consume_proxy()
      else:
        self._time_since_no_consumable_proxy = 0

  def _consume_proxy(self):
    '''Method for a root node to absorb its unique kid.'''
    if self._parent is not None or len(self._kids) != 1:
      raise errors.InternalError("Must have a unique kid and be root to consume a proxy.")

    if self._root_consuming_proxy_id is not None:
      raise errors.InternalError("Root node is already in the process of consuming a separate proxy node.")

    proxy = next(iter(self._kids.values()))
    self._root_consuming_proxy_id = proxy['id']
    self.send(proxy, messages.io.merge_with(self.new_handle(proxy['id'])))

  def _check_for_mergeable_kids(self, ms):
    '''Check whether any two kids should be merged.'''
    TIME_TO_WAIT_BEFORE_KID_MERGE_MS = 4 * 1000

    if self._height > 0:
      best_pair = self._best_mergeable_kids()
      if best_pair is None or self._merging_kid_ids:
        self._time_since_no_mergable_kids_ms = 0
      else:
        self._time_since_no_mergable_kids_ms += ms

        if self._time_since_no_mergable_kids_ms >= TIME_TO_WAIT_BEFORE_KID_MERGE_MS:
          self._merge_kids(*best_pair)

  def _merge_kids(self, left_kid_id, right_kid_id):
    '''
    Merge the kids identified by left_kid_id and right_kid_id

    :param str left_kid_id: The id of one kid to merge.
    :param str right_kid_id: The id of another kid to merge.
    '''
    self._merging_kid_ids.add(left_kid_id)
    self.send(self._kids[left_kid_id],
              messages.io.merge_with(self.transfer_handle(self._kids[right_kid_id], left_kid_id)))

  def _best_mergeable_kids(self):
    '''
    Find the best pair of mergeable kids if they exist.

    :return: None if no 2 kids are mergable.  Otherwise, a pair of the ids of two mergeable kids.
    '''
    # Current algorithm: 2 kids can be merged if each has n_kids less than 1/3 the max
    if len(self._kid_summaries) >= 2:
      MAX_N_KIDS = self.system_config['INTERNAL_NODE_KIDS_LIMIT']
      MERGEABLE_N_KIDS = MAX_N_KIDS // 3
      n_kids_kid_id_pairs = [(kid_summary['n_kids'], kid_id) for kid_id, kid_summary in self._kid_summaries.items()]
      n_kids_kid_id_pairs.sort()
      (least_n_kids, least_id), (next_least_n_kids, next_least_id) = n_kids_kid_id_pairs[:2]

      if least_n_kids <= MERGEABLE_N_KIDS and next_least_n_kids <= MERGEABLE_N_KIDS:
        return least_id, next_least_id
    return None

  def _check_for_low_capacity(self):
    '''Check whether the total capacity of this node's kids is too low.'''
    total_kid_capacity = sum(
        self._kid_capacity_limit - kid_summary['size'] for kid_summary in self._kid_summaries.values())

    if total_kid_capacity <= self.system_config['TOTAL_KID_CAPACITY_TRIGGER']:
      if len(self._kids) < self.system_config['INTERNAL_NODE_KIDS_LIMIT']:
        if self._root_proxy_id is None:
          self._spawn_kid()
        else:
          self.logger.warning("Can't spawn children while bumping root node height.")
      else:
        if self._parent is None:
          if self._root_proxy_id is None:
            self._bump_height()
          else:
            # This happens when we've tried to bump the height once already, and the trigger fires again
            # while the newly spawned node bumping the height has not yet confirmed that it is running properly.
            self.logger.warning("Can't bump root node height, as we are waiting for a proxy to spawn.")
        else:
          if not self._warned_low_capacity:
            self._warned_low_capacity = True
            self.logger.warning("nonroot InternalNode instance had too little capacity and no room to spawn more kids. "
                                "Capacity is remaining low and is not being increased.")
    else:
      self._warned_low_capacity = False

  def _bump_height(self):
    if self._parent is not None:
      raise errors.InternalError("Only the root node may bump its height.")

    self._root_proxy_id = ids.new_id('InternalNode_root_proxy')
    self._height += 1
    self._controller.spawn_node(
        messages.io.internal_node_config(
            node_id=self._root_proxy_id,
            parent=self.new_handle(self._root_proxy_id),
            variant=self._variant,
            height=self._height - 1,
            adjacent=None,
            initial_state=self._initial_state,
            adoptees=[self.transfer_handle(kid, self._root_proxy_id) for kid in self._kids.values()],
        ))

  def receive(self, message, sender_id):
    if message['type'] == 'hello_parent':
      if sender_id == self._root_proxy_id:
        self._kid_summaries = {}
        self._kids = {sender_id: message['kid']}
        self._graph = NetworkGraph()
        self._graph.add_node(sender_id)
        self._root_proxy_id = None
      else:
        self._kids[sender_id] = message['kid']
        self._graph.add_node(sender_id)
        if self._pending_adoptees is not None:
          if sender_id in self._pending_adoptees:
            self._pending_adoptees[sender_id] = True
          if all(self._pending_adoptees.values()):
            self._pending_adoptees = None
            self._send_hello_parent()

        if self._height == 0:
          self._added_leaf(message['kid'])

        self._graph.add_node(sender_id)

      self._send_kid_summary()
    elif message['type'] == 'goodbye_parent':
      if sender_id in self._merging_kid_ids:
        self._merging_kid_ids.remove(sender_id)
      if sender_id in self._kids:
        self._kids.pop(sender_id)
      if sender_id in self._kid_summaries:
        self._kid_summaries.pop(sender_id)
        self._send_kid_summary()

      if self._leaving_kids is not None and sender_id in self._leaving_kids:
        self._leaving_kids.remove(sender_id)
        if not self._leaving_kids:
          self._controller.terminate_node(self.id)

      if sender_id == self._root_consuming_proxy_id:
        self._complete_consuming_proxy()
    elif message['type'] == 'kid_summary':
      if sender_id in self._kids:
        self._kid_summaries[sender_id] = message
        self._check_for_kid_limits()
    elif message['type'] == 'merge_with':
      if self._parent is None:
        raise errors.InternalError("Root nodes can not merge with other nodes.")
      new_parent = message['node']
      for kid in self._kids.values():
        self.send(kid, messages.io.adopt(self.transfer_handle(new_parent, kid['id'])))
      self.send(self._parent, messages.io.goodbye_parent())
      self._leaving_kids = set(self._kids.keys())
    elif message['type'] == 'adopt':
      if self._parent is None:
        raise errors.InternalError("Root nodes may not adopt a new parent.")
      self.send(self._parent, messages.io.goodbye_parent())
      self._parent = message['new_parent']
      self._send_hello_parent()
    else:
      super(InternalNode, self).receive(message=message, sender_id=sender_id)

  def _complete_consuming_proxy(self):
    if self._parent is not None:
      raise errors.InternalError("Only root nodes should complete consuming a proxy node.")
    if self._height < 2:
      raise errors.InternalError("A root node should have a height >= 2 when it completes consuming its proxy.")
    self._height -= 1
    self._root_consuming_proxy_id = None

  def _set_input(self, node):
    if self._adjacent is not None:
      raise errors.InternalError("InternalNodes can have only a single adjacent node.")
    self._adjacent = node

  def _set_output(self, node):
    if self._adjacent is not None:
      raise errors.InternalError("InternalNodes can have only a single adjacent node.")
    self._adjacent = node

  @staticmethod
  def from_config(node_config, controller):
    return InternalNode(
        node_id=node_config['id'],
        parent=node_config['parent'],
        controller=controller,
        adjacent=node_config['adjacent'],
        adoptees=node_config['adoptees'],
        variant=node_config['variant'],
        height=node_config['height'],
        initial_state=node_config['initial_state'])

  def elapse(self, ms):
    n_ticks = self._ticker.elapse(ms)
    if n_ticks > 0:
      self._send_kid_summary()
      self._check_for_mergeable_kids(self._ticker.interval_ms * n_ticks)
      self._check_for_consumable_proxy(self._ticker.interval_ms * n_ticks)

  def _send_kid_summary(self):
    if self._parent is not None:
      self.send(self._parent,
                messages.io.kid_summary(
                    size=(sum(kid_summary['size'] for kid_summary in self._kid_summaries.values())
                          if self._height > 0 else len(self._kids)),
                    n_kids=len(self._kids)))

  @property
  def _branching_factor(self):
    return self.system_config['INTERNAL_NODE_KIDS_LIMIT']

  @property
  def _kid_capacity_limit(self):
    return self._branching_factor**self._height

  def _get_capacity(self):
    # find the best kid
    highest_capacity_kid_id, max_kid_capacity, size = None, 0, 0
    for kid_id, kid_summary in self._kid_summaries.items():
      size += kid_summary['size']
      kid_capacity = self._kid_capacity_limit - kid_summary['size']
      if kid_capacity > max_kid_capacity:
        highest_capacity_kid_id, max_kid_capacity = kid_id, kid_capacity

    if highest_capacity_kid_id is None:
      if self._height == 0:
        highest_capacity_kid = None
      else:
        raise errors.NoCapacityError()
    else:
      highest_capacity_kid = self._kids[highest_capacity_kid_id]

    return {
        'height': self._height,
        'size': size,
        'max_size': self._kid_capacity_limit * self._branching_factor,
        'highest_capacity_kid': highest_capacity_kid,
    }

  def stats(self):
    return {
        'height': self._height,
        'n_retransmissions': self.linker.n_retransmissions,
        'n_reorders': self.linker.n_reorders,
        'n_duplicates': self.linker.n_duplicates,
        'sent_messages': self.linker.least_unused_sequence_number,
        'acknowledged_messages': self.linker.least_unacknowledged_sequence_number(),
    }

  def handle_api_message(self, message):
    if message['type'] == 'create_kid_config':
      return self.create_kid_config(name=message['new_node_name'], machine_id=message['machine_id'])
    elif message['type'] == 'get_capacity':
      return self._get_capacity()
    elif message['type'] == 'get_kids':
      return self._kids
    elif message['type'] == 'get_senders':
      if self._adjacent is not None:
        if self._variant == 'input':
          return {}
        elif self._variant == 'output':
          return {self._adjacent['id']: self._adjacent}
        else:
          raise errors.InternalError('Unrecognized variant "{}"'.format(self._variant))
      else:
        return {}
    elif message['type'] == 'get_receivers':
      if self._adjacent is not None:
        if self._variant == 'input':
          return {self._adjacent['id']: self._adjacent}
        elif self._variant == 'output':
          return {}
        else:
          raise errors.InternalError('Unrecognized variant "{}"'.format(self._variant))
      else:
        return {}
    elif message['type'] == 'get_adjacent_handle':
      return self._adjacent
    else:
      return super(InternalNode, self).handle_api_message(message)

  def create_kid_config(self, name, machine_id):
    '''
    Generate a config for a new child leaf node, and mark it as a pending child on this parent node.

    :param str name: The name to use for the new node.

    :param str machine_id: The id of the MachineController which will run the new node.
    :return: A config for the new child node.
    :rtype: :ref:`message`
    '''
    if self._height != 0:
      raise errors.InternalError("Only InternalNode instances of height 0 should create kid configs.")

    node_id = dist_zero.ids.new_id('LeafNode_{}'.format(name))
    self.logger.info(
        "Registering a new leaf node config for an internal node. name='{node_name}'",
        extra={
            'internal_node_id': self.id,
            'leaf_node_id': node_id,
            'node_name': name
        })
    self._kids[node_id] = None

    return messages.io.leaf_config(
        node_id=node_id,
        name=name,
        parent=self.new_handle(node_id),
        variant=self._variant,
        initial_state=self._initial_state,
    )

  def _added_leaf(self, kid):
    '''
    :param kid: The :ref:`handle` of the leaf node that was just added.
    :type kid: :ref:`handle`
    '''
    if kid['id'] not in self._kids:
      self.logger.error(
          "added_leaf: Could not find node matching id {missing_child_node_id}",
          extra={'missing_child_node_id': kid['id']})
    else:
      self._graph.add_node(kid['id'])
      self._kids[kid['id']] = kid

      if self._adjacent is None:
        self.logger.info("added_leaf: No adjacent was set when a kid was added."
                         "  Unable to forward an added_leaf message to the adjacent.")
      else:
        self.send(self._adjacent,
                  messages.io.added_adjacent_leaf(
                      kid=self.transfer_handle(handle=kid, for_node_id=self._adjacent['id']), variant=self._variant))

  def deliver(self, message, sequence_number, sender_id):
    raise errors.InternalError("Messages should not be delivered to internal nodes.")
