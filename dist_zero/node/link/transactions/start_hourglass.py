from collections import defaultdict

from dist_zero import errors, messages


class StartHourglassTransaction(object):
  def __init__(self, node, mid_node_id):
    self._node = node

    self._mid_node_id = mid_node_id

    self._mid_node = None
    self._terminal_sequence_number = {}
    self._n_hourglass_senders = None

  def start(self):
    self._node.send_forward_messages()

  def receive(self, message, sender_id):
    if message['type'] == 'hourglass_swap' and message['mid_node_id'] == self._mid_node_id:
      self._terminal_sequence_number[sender_id] = message['sequence_number']
      self._maybe_swap_mid_node()
      return True
    elif message['type'] == 'hourglass_receive_from_mid_node' and message['mid_node']['id'] == self._mid_node_id:
      self._mid_node = message['mid_node']
      self._n_hourglass_senders = message['n_hourglass_senders']
      self._maybe_swap_mid_node()
      return True

    return False

  def _maybe_swap_mid_node(self):
    if self._mid_node is not None and len(self._terminal_sequence_number) == self._n_hourglass_senders:
      self._node.linker.remove_importers(list(self._terminal_sequence_number.keys()))
      for sender_id in self._terminal_sequence_number.keys():
        self._node._importers.pop(sender_id)

      self._node.import_from_node(self._mid_node)
      self._node.send(
          self._mid_node,
          messages.migration.configure_new_flow_right(None, [
              messages.migration.right_configuration(
                  n_kids=None,
                  parent_handle=self._node.new_handle(self._mid_node_id),
                  height=self._node.height,
                  is_data=False,
                  connection_limit=self._node.system_config['SUM_NODE_SENDER_LIMIT'],
              )
          ]))
      self._node.end_transaction()
