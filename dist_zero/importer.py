from dist_zero import messages


class Importer(object):
  '''
  Instances of Importer will be used by nodes internal to a computation to represent
  a source of input messages to that node.
  '''

  def __init__(self, node, sender):
    '''
    :param node: The internal node.
    :type node: `Node`

    :param sender: The :ref:`handle` of the node sending to this internal node.
    :type sender: :ref:`handle`
    '''
    self._node = node
    self._sender = sender

  def initialize(self):
    self._node.send(self._sender,
                    messages.migration.connect_internal(node=self._node.handle(self._sender), direction='receiver'))

  @property
  def sender_id(self):
    return self._sender['id']

  def duplicate_paired_exporters_to(self, other_node):
    self._node.send(self._sender,
                    messages.migration.start_duplicating(
                        old_receiver_id=self._node.id,
                        receiver=self.convert_handle_for_new_node(handle=other_node, new_node_id=self._sender['id']),
                    ))

  def finish_duplicating_paired_exporters(self):
    self._node.send(self._sender, messages.migration.finish_duplicating(receiver_id=self._node.id))
