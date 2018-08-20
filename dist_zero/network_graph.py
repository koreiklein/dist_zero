class NetworkGraph(object):
  def __init__(self):
    self._nodes = []
    self._outgoing_edges = {}
    self._incomming_edges = {}

  def nodes(self):
    return list(self._nodes)

  def add_node(self, node_id):
    self._nodes.append(node_id)
    self._outgoing_edges[node_id] = []
    self._incomming_edges[node_id] = []

  def add_edge(self, src, tgt):
    edge = (src, tgt)
    self._outgoing_edges[src].append(edge)
    self._incomming_edges[tgt].append(edge)

  def node_senders(self, node_id):
    return [sender for sender, receiver in self._incomming_edges[node_id]]

  def node_receivers(self, node_id):
    return [receiver for sender, receiver in self._outgoing_edges[node_id]]
