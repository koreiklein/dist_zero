from dist_zero import messages, errors


class Exporter(object):
  '''
  Instances of Exporter will be used by nodes internal to a computation to represent
  a destination for messages leaving the node.

  During migrations, they are responsible for duplicating messages.

  Retransmission:

  When a node tries to export a message with the exporter, the exporter will send the message,
  and watch to see whether the message is acknowleded.  If after enough time the message is not acknowledged,
  it will resubmit it.
  '''

  PENDING_EXPIRATION_TIME_MS = 1 * 400
  '''
  When a message has been sent, it will be put into a pending state.
  PENDING_EXPIRATION_TIME_MS milliseconds after a message is sent, if it still hasn't been acknowledged,
  it will be considered expired.

  Expired messages will be retransmitted during calls to `Exporter.retransmit_expired_pending_messages`
  '''

  def __init__(self, node, receiver, linker, least_unacknowledged_sequence_number):
    '''
    :param node: The internal node.
    :type node: `Node`

    :param linker: The linker associated with this exporter
    :type linker: `Linker`

    :param receiver: The :ref:`handle` of the node receiving from this internal node.
    :type receiver: :ref:`handle`

    :param int least_unacknowledged_sequence_number: The least sequence number this Exporter will ever be responsible for.
    '''
    self._node = node
    self._receiver = receiver

    self._linker = linker

    # If None, then this Exporter is not duplicating.
    # Otherwise, the list of Exporter instances to duplicate to.
    self._duplicated_exporters = None

    # A list of tuples (time_sent_ms, sequence_number_when_sent, message)
    # of all messages that have been sent but not acknowledged, along with
    # the time and sequence number at which they were sent.
    self._pending_messages = []

    # See `Exporter.least_unacknowledged_sequence_number`
    self._least_unacknowledged_sequence_number = least_unacknowledged_sequence_number

  def has_pending_messages(self):
    '''return True iff this exporter has pending messages for which it is waiting for an acknowledgement.'''
    return True if self._pending_messages else False

  def retransmit_expired_pending_messages(self):
    '''
    Retransmit any pending messages that have been pending for too long.

    :param int time_ms: The current time in milliseconds on the sending `Node`
    '''
    cutoff_send_time_ms = self._linker.now_ms - Exporter.PENDING_EXPIRATION_TIME_MS
    while self._pending_messages and self._pending_messages[0][0] <= cutoff_send_time_ms:
      t, sequence_number, message = self._pending_messages.pop(0)
      self._linker.n_retransmissions += 1
      self._node.logger.warning("Retransmitting message {sequence_number}", extra={'sequence_number': sequence_number})
      self.export_message(message=message, sequence_number=sequence_number)

  @property
  def least_unacknowledged_sequence_number(self):
    '''
    The least natural number N such that
      - this `Exporter` has not received an acknowledgement for N
      - This `Exporter`'s receiver would be expected to acknowledge N
    '''
    return self._least_unacknowledged_sequence_number

  def initialize(self):
    self._node.send(self._receiver,
                    messages.migration.connect_internal(
                        node=self._node.new_handle(self._receiver['id']), direction='sender'))

  @property
  def receiver_id(self):
    '''The id of the node receiving from this exporter'''
    return self._receiver['id']

  def acknowledge(self, sequence_number):
    '''
    Acknowledge the receipt of all sequence numbers less than sequence_number.

    :param int sequence_number: Some sequence number for which all smaller sequence numbers should now be acknowledged.
    '''
    self._least_unacknowledged_sequence_number = max(self._least_unacknowledged_sequence_number, sequence_number)

    self.logger.debug(
        "exporter acknowledges all sequence numbers below {acknowledged_sequence_number}",
        extra={'acknowledged_sequence_number': sequence_number})

    # PERF(KK): binary search is possible here.
    self._pending_messages = [(t, sn, msg) for t, sn, msg in self._pending_messages
                              if sn >= self._least_unacknowledged_sequence_number]

  def export_message(self, message, sequence_number):
    '''
    Export a message to the receiver.

    :param message: The message
    :type message: :ref:`message`
    :param int sequence_number: The sequence number of the new message.
    '''
    self._pending_messages.append((self._linker.now_ms, sequence_number, message))
    self._node.send(self._receiver,
                    messages.linker.sequence_message_send(message=message, sequence_number=sequence_number))

  def duplicate(self, exporters):
    '''
    Start duplicating this exporter to a new receiver.

    prerequisite: The exporter must not already be duplicating.

    :param list exporters: A list of uninitialized `Exporter` instances to duplicate to.
    '''
    if self._duplicated_exporters is not None:
      raise errors.InternalError("Can not duplicate while already duplicating.")

    self._duplicated_exporters = exporters

    for exporter in exporters:
      exporter.initialize()

  @property
  def logger(self):
    return self._node.logger

  def finish_duplicating(self):
    '''
    End duplication to the original receiver.

    prerequisite: The exporter must in the process of duplicating.

    Typically, when an `Exporter` finishes duplicating, it will never be used again and should be left
    for cleanup by the garbage collector.

    :return: The list of exporters this node was duplicating to.
    :rtype: list[`Exporter`]
    '''
    self.logger.info(
        "Finishing duplication phase for {cur_node_id} ."
        "  Cutting back from {n_old_receivers} receivers to 1.",
        extra={
            'n_old_receivers': len(self._duplicated_exporters),
        })
    self._node.send(self._receiver, messages.migration.finished_duplicating())
    self._linker.remove_exporter(self)
    return self._duplicated_exporters
