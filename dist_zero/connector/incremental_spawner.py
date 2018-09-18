from dist_zero import errors, messages


class IncrementalSpawner(object):
  '''
  For spawning, one at a time, the new layers of nodes that are being added to a preexisting `Connector` instance.
  '''

  def __init__(self, new_layers, connector, node):
    self._layers = new_layers
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
    if self.graph.node_receivers(node_id):
      return [self._node.id]
    else:
      import ipdb
      ipdb.set_trace()
      # FIXME(KK): Figure out the proper behavior here.
      #return self._connector.right_to_parent_ids[node_id]

  def _maybe_spawned_kids(self):
    if not self.finished and \
        all(val is not None for val in self._node.kids.values()):

      self._spawn_layer(self._current_spawning_layer + 1)

  def spawned_a_kid(self, node):
    self._maybe_spawned_kids()

  def _spawn_layer(self, layer_index):
    if layer_index >= len(self._layers):
      self.finished = True
      self._node.all_incremental_kids_are_spawned()
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
