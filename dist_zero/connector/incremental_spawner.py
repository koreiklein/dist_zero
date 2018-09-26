from collections import defaultdict

from dist_zero import errors, messages


class IncrementalSpawner(object):
  '''
  For spawning, one at a time, the new layers of nodes that are being added to a preexisting `Connector` instance.
  '''

  def __init__(self, new_layers, last_edges, hourglasses, connector, is_left, node):
    self._layers = new_layers
    self._last_edges = last_edges
    self._mid_node_by_id = {}
    self._hourglasses = hourglasses
    self._connector = connector
    self._is_left = is_left
    self._layer_offset = 1 if is_left else len(self._connector.layers) - len(new_layers)
    self._node = node

    self._hourglass_results = None

    self._awaiting_last_edges = {}

    self.finished = False

  @property
  def graph(self):
    return self._connector.graph

  @property
  def layers(self):
    return self._layers

  def start_spawning(self):
    if self._layers:
      befores = set(before for node in self._layers[0] for before in self._connector.graph.node_senders(node))
      for before in befores:
        self._node.send(
            self._id_to_handle(before),
            messages.migration.configure_right_parent(migration_id=None, kid_ids=self.graph.node_receivers(before)))
    self._spawn_layer(0)

  def _id_to_handle(self, node_id):
    '''
    Get the handle for a child or left-child node.

    :param str node_id: The id of a kid node, or a node to the left of a kid node.
    :return: The :ref:`handle` associated with ``node_id``
    :rtype: :ref:`handle`
    '''
    if node_id in self._connector._left_layer:
      return self._connector._left_layer[node_id]
    elif node_id in self._node.kids:
      return self._node.kids[node_id]
    else:
      raise errors.InternalError('Node id "{}" was not found in the kids on left-kids.'.format(node_id))

  def _get_right_parent_ids_for_kid(self, node_id, layer_index):
    return [self._node.id]

  def _maybe_spawned_kids(self):
    if not self.finished and \
        all(val is not None for val in self._node.kids.values()):

      self._spawn_layer(self._current_spawning_layer + 1)

  def spawned_a_kid(self, node):
    self._maybe_spawned_kids()

  def _finished_spawning_edges(self):
    self._hourglass_results = {}
    self._hourglass_by_id = {
        mid_node_id: (mid_node_id, senders, receivers)
        for (mid_node_id, senders, receivers) in self._hourglasses
    }
    for hourglass in self._hourglasses:
      self._start_hourglass(*hourglass)
    self._maybe_finished_hourglasses()

  def mid_node_up(self, mid_node):
    mid_node_id, senders, receivers = self._hourglass_by_id[mid_node['id']]
    self._mid_node_by_id[mid_node_id] = mid_node
    self._hourglass_results[mid_node_id] = 'connecting'
    self._node.send(mid_node, messages.migration.configure_right_parent(migration_id=None, kid_ids=receivers))

    for sender in senders:
      self._node.send(
          self._id_to_handle(sender),
          messages.hourglass.start_hourglass(
              receiver_ids=receivers, mid_node=self._node.transfer_handle(mid_node, sender)))

  def _start_hourglass(self, mid_node, senders, receivers):
    self._hourglass_results[mid_node] = 'pending'
    if self._is_left:
      if self._connector._left_is_data:
        layer_index = 2
      else:
        layer_index = 1
    else:
      layer_index = len(self._connector.layers) - 1

    self._node.spawn_kid(
        node_id=mid_node,
        layer_index=layer_index,
        senders=[self._id_to_handle(sender) for sender in senders],
        configure_right_parent_ids=[self._node.id],
        is_mid_node=True,
        left_ids=senders,
        migrator=None)

  def mid_node_ready(self, mid_node_id):
    mid_node_id, senders, receivers = self._hourglass_by_id[mid_node_id]
    _lookup_tgt = self._make_lookup_tgt()
    receiver_handles = [_lookup_tgt(receiver_id) for receiver_id in receivers]
    mid_node_handle = self._mid_node_by_id[mid_node_id]
    for receiver_handle in receiver_handles:
      self._node.send(receiver_handle,
                      messages.hourglass.hourglass_receive_from_mid_node(
                          mid_node=self._node.transfer_handle(mid_node_handle, receiver_handle['id']),
                          n_hourglass_senders=len(senders),
                      ))

  def _maybe_finished_hourglasses(self):
    if all(val == 'done' for val in self._hourglass_results.values()):
      self._node.all_incremental_kids_are_spawned()

  def _make_lookup_src(self):
    left_kid_to_handle = {
        kid['handle']['id']: kid['handle']
        for left_config in self._connector._left_configurations.values() for kid in left_config['kids']
    }
    return lambda nid: left_kid_to_handle[nid] if nid in left_kid_to_handle else self._node.kids[nid]

  def _make_lookup_tgt(self):
    right_kid_to_handle = {
        kid['id']: kid
        for right_config in self._connector._right_configurations.values()
        for kid in right_config.get('known_kids', [])
    }
    return lambda nid: right_kid_to_handle[nid] if nid in right_kid_to_handle else self._node.kids[nid]

  def _finished_spawning_layers(self):
    if not self._last_edges:
      raise errors.InternalError("There should always be last_edges for the incremental_spawner")

    _lookup_src = self._make_lookup_src()
    _lookup_tgt = self._make_lookup_tgt()

    src_to_tgts = defaultdict(list)
    for src_id, tgt_id in self._last_edges:
      src_to_tgts[src_id].append(tgt_id)
      self._awaiting_last_edges[tgt_id] = False
      self._node.send(
          _lookup_tgt(tgt_id),
          messages.migration.added_sender(
              self._node.transfer_handle(_lookup_src(src_id), tgt_id), respond_to=self._node.new_handle(tgt_id)))

    for src_id, tgt_ids in src_to_tgts.items():
      self._node.send(
          _lookup_src(src_id), messages.migration.configure_right_parent(migration_id=None, kid_ids=tgt_ids))

    self._maybe_finished_spawning_edges()

  def finished_adding_sender(self, src_id, tgt_id):
    self._awaiting_last_edges[tgt_id] = True
    self._maybe_finished_spawning_edges()

  def _maybe_finished_spawning_edges(self):
    if all(self._awaiting_last_edges.values()):
      self._finished_spawning_edges()

  def _spawn_layer(self, layer_index):
    if layer_index >= len(self._layers):
      self.finished = True
      self._finished_spawning_layers()
    else:
      if layer_index >= 1:
        self._send_configure_right_parent(layer_index - 1)

      self._current_spawning_layer = layer_index

      for node_id in self._layers[layer_index]:
        left_ids = self.graph.node_senders(node_id)

        self._node.spawn_kid(
            layer_index=layer_index + self._layer_offset,
            configure_right_parent_ids=self._get_right_parent_ids_for_kid(node_id, layer_index),
            node_id=node_id,
            left_ids=left_ids,
            senders=[self._id_to_handle(sender_id) for sender_id in left_ids],
            migrator=None)

  def _send_configure_right_parent(self, layer_index):
    for node_id in self._layers[layer_index]:
      self._node.send(
          self._id_to_handle(node_id),
          messages.migration.configure_right_parent(migration_id=None, kid_ids=self.graph.node_receivers(node_id)))
