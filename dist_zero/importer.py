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

    self._least_unreceived_remote_sequence_number = 0 # see `Importer.least_unreceived_remote_sequence_number`

    self._remote_sequence_number_to_early_message = {}
    '''
    A map from:
    (sequence numbers greater than self._least_unreceived_remote_sequence_number) -> (a received message with that number)
    '''

  @property
  def least_unreceived_remote_sequence_number(self):
    '''The least sequence number (in the sender's sequence) that has never been received by self.'''
    return self._least_unreceived_remote_sequence_number

  def acknowledge(self, remote_sequence_number):
    self._node.send(self._sender, messages.sum.acknowledge(remote_sequence_number))

  def initialize(self):
    self._node.send(self._sender,
                    messages.migration.connect_internal(
                        node=self._node.new_handle(self._sender['id']), direction='receiver'))

  def receive(self, message):
    rsn = message['sequence_number']
    if rsn < self._least_unreceived_remote_sequence_number or rsn in self._remote_sequence_number_to_early_message:
      self._node.logger.warning(
          ("Received duplicate message for sequence number {remote_sequence_number}."
           " Message {duplicate_message}"),
          extra={
              'remote_sequence_number': rsn,
              'duplicate_message': message,
          })
    else:
      self._remote_sequence_number_to_early_message[rsn] = message
      while self._least_unreceived_remote_sequence_number in self._remote_sequence_number_to_early_message:
        message = self._remote_sequence_number_to_early_message.pop(self._least_unreceived_remote_sequence_number)
        self._node.deliver(message['amount'])
        self._least_unreceived_remote_sequence_number += 1

  @property
  def sender_id(self):
    return self._sender['id']

  def duplicate_paired_exporters_to(self, other_node):
    self._node.send(self._sender,
                    messages.migration.start_duplicating(
                        old_receiver_id=self._node.id,
                        receiver=self._node.transfer_handle(handle=other_node, for_node_id=self._sender['id']),
                    ))

  def finish_duplicating_paired_exporters(self):
    self._node.send(self._sender, messages.migration.finish_duplicating(receiver_id=self._node.id))
