import logging

import dist_zero.ids
from dist_zero import settings, messages, errors, recorded, importer, exporter, misc, ids
from dist_zero.network_graph import NetworkGraph
from dist_zero.node.node import Node

logger = logging.getLogger(__name__)


class InternalNode(Node):
  '''
  The root of a tree of `LeafNode` instances of the same ``variant``.

  Each `InternalNode` instance is responsible for keeping track of the state of its subtree, and for growing
  or shrinking it as necessary.  In particular, when new leaves are created, `InternalNode.create_kid_config` must
  be called on the desired immediate parent to generate the node config for starting that child.

  Each `InternalNode` will have an associated depth.  The assignment of depths to internal nodes is the unique
  minimal assignment such that n.depth+1 == n.parent.depth for every node n that has a parent.
  '''

  def __init__(self, node_id, parent, variant, depth, adjacent, adoptees, spawner_adjacent, initial_state, controller):
    '''
    :param str node_id: The id to use for this node
    :param parent: If this node is the root, then `None`.  Otherwise, the :ref:`handle` of its parent `Node`.
    :type parent: :ref:`handle` or `None`
    :param str variant: 'input' or 'output'
    :param int depth: The depth of the node in the tree.  See `InternalNode`
    :param adjacent: The :ref:`handle` of the adjacent node or `None` if this node should not start with an adjacent.
    :type adjacent: :ref:`handle` or `None`
    :param adoptees: Nodes to adopt upon initialization.
    :type adoptees: list[:ref:`handle`]
    :param spawner_adjacent: The node adjacent to the node that spawned self.  When provided, adjacent should be None,
      and the spawner_adjacent node will be responsible for setting up an adjacent node for self.
    :type spawner_adjacent: `None` or :ref:`handle`
    :param `MachineController` controller: The controller for this node.
    :param object initial_state: A json serializeable starting state for all leaves spawned from this node.
      This state is important for output leaves that update that state over time.
    '''
    self._controller = controller
    self._parent = parent
    self._variant = variant
    self._depth = depth
    self.id = node_id
    self._kids = {} # A map from kid node id to either None or its handle
    self._initial_state = initial_state
    self._adjacent = adjacent

    self._current_state = None

    # When being spawned as part of a split:
    self._adoptees = {adoptee['id']: adoptee for adoptee in adoptees} if adoptees is not None else None
    self._spawner_adjacent = spawner_adjacent

    self._graph = NetworkGraph()

    self._current_split = None

    super(InternalNode, self).__init__(logger)

  def is_data(self):
    return True

  def _finish_split(self):
    self._current_split = None

  @property
  def depth(self):
    return self._depth

  def get_adjacent_id(self):
    return None if self._adjacent is None else self._adjacent['id']

  def checkpoint(self, before=None):
    pass

  def sink_swap(self, deltas, old_sender_ids, new_senders, new_importers, linker):
    if self._variant == 'output':
      if len(new_senders) != 1:
        import ipdb
        ipdb.set_trace()
        raise errors.InternalError(
            "sink_swap should be called on an edge internal node only when there is a unique new sender.")
      self._adjacent = new_senders[0]
    elif self._variant == 'input':
      # FIXME(KK): Test and implement this
      import ipdb
      ipdb.set_trace()
    else:
      raise errors.InternalError('Unrecognized variant "{}"'.format(self._variant))

  def switch_flows(self, migration_id, old_exporters, new_exporters, new_receivers):
    if self._variant == 'input':
      if len(new_receivers) != 1:
        raise errors.InternalError("Not sure how to set an input node's receives to a list not of length 1.")
      if self._adjacent is not None and self._adjacent['id'] != new_receivers[0]['id']:
        import ipdb
        ipdb.set_trace()
        raise errors.InternalError("Not sure how to set an input node's receives when it already has an adjacent.")
      self._adjacent = new_receivers[0]
      self.send(self._adjacent, messages.migration.swapped_to_duplicate(migration_id, first_live_sequence_number=0))
    elif self._variant == 'output':
      import ipdb
      ipdb.set_trace()
    else:
      raise errors.InternalError('Unrecognized variant "{}"'.format(self._variant))

    for kid in self._kids.values():
      self.send(kid, messages.migration.switch_flows(migration_id))

  def initialize(self):
    if self._adoptees is not None:
      for kid in self._adoptees.values():
        self.send(kid, messages.io.adopt(self.new_handle(kid['id'])))
      self.send(self._spawner_adjacent,
                messages.io.adjacent_has_split(
                    self.new_handle(self._spawner_adjacent['id']), stolen_io_kid_ids=list(self._adoptees.keys())))

    if self._adjacent is not None:
      if self._variant == 'input':
        self.logger.info("internal node sending 'set_input' message to adjacent node")
        self.send(self._adjacent, messages.io.set_input(self.new_handle(self._adjacent['id'])))
      elif self._variant == 'output':
        self.logger.info("internal node sending 'set_output' message to adjacent node")
        self.send(self._adjacent, messages.io.set_output(self.new_handle(self._adjacent['id'])))
      else:
        raise errors.InternalError("Unrecognized variant {}".format(self._variant))

    if self._parent is not None:
      self.send(self._parent, messages.io.hello_parent(self.new_handle(self._parent['id'])))

    if self._parent is None:
      # Root nodes must spawn their unique depth 0 kid.
      node_id = ids.new_id("InternalNode_first_root_kid")
      if self._depth != 0:
        raise errors.InternalError("Root node must be spawned with depth 0")
      self._depth += 1
      self._controller.spawn_node(
          messages.io.internal_node_config(
              node_id=node_id,
              parent=self.new_handle(node_id),
              variant=self._variant,
              depth=0,
              adjacent=None,
              spawner_adjacent=None
              if self._adjacent is None else self.transfer_handle(self._adjacent, for_node_id=node_id),
              initial_state=self._initial_state,
              adoptees=None,
          ))

  def receive(self, message, sender_id):
    if message['type'] == 'hello_parent':
      self._kids[sender_id] = message['kid']
      self._graph.add_node(sender_id)
    elif message['type'] == 'added_leaf':
      self.added_leaf(message['kid'])
    elif message['type'] == 'adopted_by':
      if self._current_split is None:
        self.logger.warning("Received adopted_by without a current active split, discarding.")
      else:
        self._current_split.adopted_by(kid_id=sender_id, new_parent_id=message['new_parent_id'])
    elif message['type'] == 'adopted':
      self._kids[sender_id] = self._adoptees[sender_id]
    elif message['type'] == 'connect_node':
      if message['direction'] == 'receiver':
        self._set_output(message['node'])
      elif message['direction'] == 'sender':
        self._set_input(message['node'])
      else:
        raise errors.InternalError('Unrecognized direction "{}"'.format(message['direction']))
    else:
      super(InternalNode, self).receive(message=message, sender_id=sender_id)

  def _set_input(self, node):
    if self._adjacent is not None:
      raise errors.InternalError("InternalNodes can have only a single adjacent node.")
    self._adjacent = node

  def _set_output(self, node):
    if self._adjacent is not None:
      import ipdb
      ipdb.set_trace()
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
        spawner_adjacent=node_config['spawner_adjacent'],
        variant=node_config['variant'],
        depth=node_config['depth'],
        initial_state=node_config['initial_state'])

  def elapse(self, ms):
    pass

  def handle_api_message(self, message):
    if message['type'] == 'create_kid_config':
      return self.create_kid_config(name=message['new_node_name'], machine_id=message['machine_id'])
    elif message['type'] == 'get_kids':
      return self._kids
    elif message['type'] == 'get_adjacent_handle':
      return self._adjacent
    else:
      return super(InternalNode, self).handle_api_message(message)

  def _maybe_too_many_kids(self):
    '''If there are too many kids, then remedy that problem.'''
    KIDS_LIMIT = self.system_config['INTERNAL_NODE_KIDS_LIMIT']
    if len([val for val in self._kids.values() if val is not None]) > KIDS_LIMIT:
      if self._current_split is None:
        if self._parent is None:
          self._root_split()
        else:
          self._non_root_split()
      else:
        self.logger.warning(
            "InternalNode is not splitting in response to too many kids, as there as already a split in progress.")

  def _root_split(self):
    '''Split a root node'''
    self._current_split = _RootSplit(self)
    self._current_split.split()

  def _non_root_split(self):
    '''Split a non-root node'''
    # FIXME(KK): Implement this
    raise RuntimeError("Not Yet Implemented")

  def create_kid_config(self, name, machine_id):
    '''
    Generate a config for a new child leaf node, and mark it as a pending child on this parent node.

    :param str name: The name to use for the new node.

    :param str machine_id: The id of the MachineController which will run the new node.
    :return: A config for the new child node.
    :rtype: :ref:`message`
    '''
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

  def added_leaf(self, kid):
    '''
    :param kid: The :ref:`handle` of the leaf node that was just added.
    :type kid: :ref:`handle`
    '''
    if kid['id'] not in self._kids:
      self.logger.error(
          "added_leaf: Could not find node matching id {missing_child_node_id}",
          extra={'missing_child_node_id': kid['id']})
    elif self._adjacent is None:
      import ipdb
      ipdb.set_trace()
      self.logger.error(
          "added_leaf: No adjacent was set in time.  Unable to forward an added_leaf message to the adjacent.")
    else:
      self._kids[kid['id']] = kid
      self._maybe_too_many_kids()

      self.send(self._adjacent,
                messages.io.added_adjacent_leaf(
                    kid=self.transfer_handle(handle=kid, for_node_id=self._adjacent['id']), variant=self._variant))

  def deliver(self, message, sequence_number, sender_id):
    raise errors.InternalError("Messages should not be delivered to internal nodes.")


class _RootSplit(object):
  '''
  For splitting up the root node when it has too many kids.
  This class manages a change in topology whereby a root node spawns an intermediate non-root node between
  itself and its kids.

    Root                     Root
  |   |   |         ->        |
  k0  k1  k2               new_parent
                           |   |   |
                           k0  k1  k2

  The corresponding topology on adjacent nodes will also be updated to match.
  '''

  def __init__(self, node):
    self._node = node
    self._new_node_ready = False

  def adopted_by(self, kid_id, new_parent_id):
    if new_parent_id != self._new_parent_id:
      raise errors.InternalError("Impossible!  A kid got the wrong parent.")

    self._kid_has_left[kid_id]
    self._maybe_ready()

  def _maybe_ready(self):
    if self._new_node_ready and all(self._kid_has_left.values()):
      import ipdb
      ipdb.set_trace()

  def split(self):
    self._kid_has_left = {kid_id: False for kid_id in self._node._kids.keys()}
    node_id = ids.new_id('InternalNode_root_child')
    self._node._controller.spawn_node(
        messages.io.internal_node_config(
            node_id=node_id,
            parent=self._node.new_handle(node_id),
            adoptees=[self._node.transfer_handle(kid, for_node_id=node_id) for kid in self._node._kids.values()],
            spawner_adjacent=self._node.transfer_handle(self._node._adjacent, for_node_id=node_id),
            variant=self._node._variant,
            depth=self._node._depth,
            adjacent=None))
    self._new_parent_id = node_id
    self._node._depth += 1


class _NonRootSplit(object):
  def __init__(self, node):
    self._node = node
    self._has_left = {} # Map each kid id to whether it has confirmed that it has left for a newly spawned parent.
    self._partition = {} # Map each newly spawned node id to the list of kids that it should steal.

  def adopted_by(self, kid_id, new_parent_id):
    self._has_left[kid_id] = True
    if kid_id in self._node._kids:
      self._node._kids.pop(kid_id)
    else:
      self._node.logger.warning("Received adopted_by for a node that isn't currently a kid.")

    if all(self._has_left.values()):
      self._node.logger.info("Finished RootSplit")
      self._node._finish_split()

  def split(self):
    N_NEW_NODES = 2

    self._partition = {
        ids.new_id('InternalNode_from_root_split'): kids
        for kids in misc.partition(items=list(self._node._kids.values()), n_buckets=N_NEW_NODES)
    }
    for kid in self._node._kids.values():
      self._has_left[kid['id']] = False

    for node_id, kids in self._partition.items():
      self._node._controller.spawn_node(
          messages.io.internal_node_config(
              node_id=node_id,
              parent=self._node.new_handle(node_id),
              adoptees=[self._node.transfer_handle(kid, for_node_id=node_id) for kid in kids],
              spawner_adjacent=self._node.transfer_handle(self._node._adjacent, for_node_id=node_id),
              variant=self._node._variant,
              depth=self._node._depth,
              adjacent=None))
