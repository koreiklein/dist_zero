from collections import defaultdict

from dist_zero import errors, messages


class IncrementalSpawner(object):
  '''
  For spawning, one at a time, the new layers of nodes that are being added to a preexisting `Connector` instance.
  '''

  def __init__(self, new_layers, last_edges, hourglasses, connector, node):
    self._layers = new_layers
    self._last_edges = last_edges
    self._hourglasses = hourglasses
    self._connector = connector
    self._node = node

    self.finished = False

  @property
  def graph(self):
    return self._connector.graph

  @property
  def layers(self):
    return self._layers

  def start_spawning(self):
    if self._hourglasses:
      # FIXME(KK): Implement this correctly
      import ipdb
      ipdb.set_trace()

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

  def _finished_spawning_layers(self):
    if self._last_edges:
      left_kid_to_handle = {
          kid['handle']['id']: kid['handle']
          for left_config in self._connector._left_configurations.values() for kid in left_config['kids']
      }
      right_kid_to_handle = {
          kid['id']: kid
          for right_config in self._connector._right_configurations.values()
          for kid in right_config.get('known_kids', [])
      }
      _lookup_src = lambda nid: left_kid_to_handle[nid] if nid in left_kid_to_handle else self._node.kids[nid]
      _lookup_tgt = lambda nid: right_kid_to_handle[nid] if nid in right_kid_to_handle else self._node.kids[nid]
      src_to_tgts = defaultdict(list)
      for src_id, tgt_id in self._last_edges:
        src_to_tgts[src_id].append(tgt_id)
        self._node.send(
            _lookup_tgt(tgt_id), messages.migration.added_sender(
                self._node.transfer_handle(_lookup_src(src_id), tgt_id)))

      for src_id, tgt_ids in src_to_tgts.items():
        self._node.send(
            _lookup_src(src_id), messages.migration.configure_right_parent(migration_id=None, kid_ids=tgt_ids))

    else:
      # Missing receivers, we should be sending update_left_configuration to our right siblings.
      import ipdb
      ipdb.set_trace()

    self._node.all_incremental_kids_are_spawned()

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
            layer_index=layer_index + 1,
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
