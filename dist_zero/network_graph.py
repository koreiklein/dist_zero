from dist_zero import errors


class NetworkGraph(object):
  def __init__(self):
    self._nodes = set()
    self._outgoing_edges = {}
    self._incomming_edges = {}

    self._edges = set()

  def to_json(self):
    return {
        'nodes': list(self._nodes),
        'outgoing_edges': self._outgoing_edges,
        'incomming_edges': self._incomming_edges,
        'edges': list(self._edges),
    }

  @staticmethod
  def from_json(j):
    g = NetworkGraph()
    g._nodes = set(j['nodes'])
    g._outgoing_edges = j['outgoing_edges']
    g._incomming_edges = j['incomming_edges']
    g._edges = set([(src, tgt) for src, tgt in j['edges']]) # Need to cast the pairs to tuples in order to hash.
    return g

  def transitive_incomming_with_duplicates(self, node):
    result = []

    def _recurse(node):
      if len(result) > len(self._nodes)**2:
        raise errors.InternalError("There should never be this many incomming nodes in a NetworkGraph")
      incomming_edges = self._incomming_edges[node]
      if len(incomming_edges) == 0:
        result.append(node)
      else:
        for src, tgt in incomming_edges:
          _recurse(src)

    _recurse(node)
    return result

  def transitive_outgoing_with_duplicates(self, node):
    result = []

    def _recurse(node):
      if len(result) > len(self._nodes)**2:
        raise errors.InternalError("There should never be this many outgoing nodes in a NetworkGraph")
      outgoing_edges = self._outgoing_edges[node]
      if len(outgoing_edges) == 0:
        result.append(node)
      else:
        for src, tgt in outgoing_edges:
          _recurse(tgt)

    _recurse(node)
    return result

  def nodes(self):
    return list(self._nodes)

  def add_node(self, node_id):
    if node_id not in self._nodes:
      self._nodes.add(node_id)
      self._outgoing_edges[node_id] = []
      self._incomming_edges[node_id] = []

  def remove_node(self, node_id):
    if node_id not in self._nodes:
      raise errors.InternalError("Node is not in graph and can't be removed.")
    if self._outgoing_edges[node_id] or self._incomming_edges[node_id]:
      raise errors.InternalError("Cannot remove a node when it has edges.")
    self._outgoing_edges.pop(node_id)
    self._incomming_edges.pop(node_id)
    self._nodes.remove(node_id)

  def remove_edge(self, src, tgt):
    edge = (src, tgt)
    if edge not in self._edges:
      raise errors.InternalError("Edge was not found in graph.")

    self._outgoing_edges[src].remove(edge)
    self._incomming_edges[tgt].remove(edge)
    self._edges.remove(edge)

  def add_edge(self, src, tgt):
    edge = (src, tgt)
    if edge not in self._edges:
      self._outgoing_edges[src].append(edge)
      self._incomming_edges[tgt].append(edge)
      self._edges.add(edge)

  def node_senders(self, node_id):
    return [sender for sender, receiver in self._incomming_edges[node_id]]

  def node_receivers(self, node_id):
    return [receiver for sender, receiver in self._outgoing_edges[node_id]]
