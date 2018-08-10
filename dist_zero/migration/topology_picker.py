from dist_zero import errors


class TopologyPicker(object):
  '''
  Each instance of `TopologyPicker` will take some constraints about nodes in a network topology,
  and determine a new set of nodes and their connections in such a way that it meets the constraints.
  '''

  def __init__(self, left_layer, right_layer, outgoing_edge_limit, incomming_edge_limit, right_n_kids):
    self._layers = [left_layer, right_layer]

    self._outgoing_edge_limit = outgoing_edge_limit
    self._incomming_edge_limit = incomming_edge_limit

    self._right_n_kids = right_n_kids # None if the right layer is not an edge

    self._outgoing_edges = {}
    self._incomming_edges = {}
    self._connections = []

    # Add a single complete connection
    self._add_complete_connection(0)

  def new_nodes(self):
    return [node_id for layer in self._layers[1:-1] for node_id in layer]

  def new_rightmost_nodes(self):
    if len(self._layers) <= 2:
      return []
    else:
      return self._layers[len(self._layers) - 2]

  def _add_complete_connection(self, i):
    self._connections.insert(i, {'type': 'complete'})
    for left_node in self._layers[i]:
      for right_node in self._layers[i + 1]:
        edge = (left_node, right_node)
        self._outgoing_edges[left_node] = edge
        self._incomming_edges[right_node] = edge

  def _get_violation(self):
    # Outgoing violations:
    for i in range(len(self._layers) - 1):
      violation = self._get_outgoing_violation(i)
      if violation is not None:
        return violation

    # Incomming violations
    for i in range(1, len(self._layers)):
      violation = self._get_incomming_violation(i)
      if violation is not None:
        return violation

    return None

  def _get_outgoing_violation(self, i):
    for node_id in self._layers[i]:
      if len(self._outgoing_edges[node_id]) > self._outgoing_edge_limit[node_id]:
        return {'type': 'too_many_outgoing_edges', 'layer': i}

    return None

  def _get_incomming_violation(self, i):
    if i + 1 == len(self._layers) and self._right_n_kids is not None and any(val > 0
                                                                             for val in self._right_n_kids.values()):
      if i == 0 or self._connections[i - 1]['type'] != 'right_edge_adjacents':
        return {'type': 'no_right_edge_adjacents'}

    for node_id in self._layers[i]:
      if (len(self._incomming_edges[node_id])
          if node_id in self._incomming_edges else 0) > self._incomming_edge_limit[node_id]:
        return {'type': 'too_many_incomming_edges', 'layer': i}

    return None

  def _fix_violation(self, violation):
    # FIXME(KK): Test and implement all of these.
    if violation['type'] == 'no_right_edge_adjacents':
      import ipdb
      ipdb.set_trace()
      raise RuntimeError("Not Yet Implemented")
    elif violation['type'] == 'too_many_incomming_edges':
      raise RuntimeError("Not Yet Implemented")
    elif violation['type'] == 'too_many_outgoing_edges':
      raise RuntimeError("Not Yet Implemented")
    else:
      raise errors.InternalError('Unrecognized node topology violation type "{}"'.format(violation['type']))

  def fix_all_violations(self):
    while True:
      violation = self._get_violation()
      if violation is None:
        return
      else:
        self._fix_violation(violation)
