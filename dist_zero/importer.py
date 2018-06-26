from dist_zero import messages


class Importer(object):
  '''
  Instances of Importer will be used by nodes internal to a computation to represent
  a source of input messages to that node.

  As messages arrive from the sender, the underlying `Node` should pass them to the `Importer.import_message` method.
  Internally, the `Importer` will de-duplicate and re-order messages, and eventually call `SumNode.deliver` on
  each message, exactly once, and in the right order.
  '''

  def __init__(self, node, linker, sender):
    '''
    :param node: The internal node.
    :type node: `Node`

    :param linker: The linker associated with this importer
    :type linker: `Linker`

    :param sender: The :ref:`handle` of the node sending to this internal node.
    :type sender: :ref:`handle`
    '''
    self._node = node
    self._linker = linker
    self._sender = sender

    # Set to try when this importer should ignore all inputs.
    self._deactivated = False

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
    '''
    Send an acknowledgement to the associated sender.

    :param int remote_sequence_number: A sequence number from the sender.
      When acknowledge is called, all messages with sequence numbers less than remote_sequence_number must
      be acknowledged by the underlying `Node`.
    '''
    self._node.logger.debug(
        "importer sending remote acknowledgement message. remote sequence number: {remote_sequence_number}",
        extra={'remote_sequence_number': remote_sequence_number})
    self._node.send(self._sender, messages.linker.sequence_message_acknowledge(remote_sequence_number))

  def initialize(self):
    self._node.send(self._sender,
                    messages.migration.connect_internal(
                        node=self._node.new_handle(self._sender['id']), direction='receiver'))

  def import_message(self, message, sender_id):
    '''
    Receive a message from the linked `Node`

    :param message: The newly received message.
    :type message: :ref:`message`
    :param str sender_id: The id of the `Node` that sent the message.
    '''
    if self._deactivated:
      self._node.logger.info("Deactivated importer is ignoring a new message")
    else:
      rsn = message['sequence_number']
      if rsn < self._least_unreceived_remote_sequence_number or rsn in self._remote_sequence_number_to_early_message:
        self._linker.n_duplicates += 1
        self._node.logger.warning(
            ("Received duplicate message for sequence number {remote_sequence_number}"
             " from sender {sender_id}"),
            extra={
                'remote_sequence_number': rsn,
                'sender_id': sender_id,
            })
      else:
        self._remote_sequence_number_to_early_message[rsn] = message

        message = None
        while self._least_unreceived_remote_sequence_number in self._remote_sequence_number_to_early_message:
          message = self._remote_sequence_number_to_early_message.pop(self._least_unreceived_remote_sequence_number)
          # The deliver code should run with a correct least_unreceived_remote_sequence_number.
          # Make sure to update it BEFORE calling deliver.
          self._least_unreceived_remote_sequence_number += 1
          self._node.deliver(message['message'])

        if message is None:
          # This message was not processed immediately, consider it reordered.
          self._linker.n_reorders += 1

  @property
  def sender_id(self):
    return self._sender['id']

  @property
  def active(self):
    return not self._deactivated

  def duplicate_paired_exporters_to(self, other_node):
    self._node.send(self._sender,
                    messages.migration.start_duplicating(
                        old_receiver_id=self._node.id,
                        receiver=self._node.transfer_handle(handle=other_node, for_node_id=self._sender['id']),
                    ))

  def finish_duplicating_paired_exporters(self):
    self._node.send(self._sender, messages.migration.finish_duplicating(receiver_id=self._node.id))

  def deactivate(self):
    self._deactivated = True
