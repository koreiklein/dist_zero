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

  PENDING_EXPIRATION_TIME_MS = 1 * 1000
  '''
  When a message has been sent, it will be put into a pending state.
  PENDING_EXPIRATION_TIME_MS milliseconds after a message is sent, if it still hasn't been acknowledged,
  it will be considered expired.

  Expired messages will be retransmitted during calls to `Exporter.retransmit_expired_pending_messages`
  '''

  def __init__(self, node, receiver, least_unacknowledged_sequence_number):
    '''
    :param node: The internal node.
    :type node: `Node`

    :param receiver: The :ref:`handle` of the node receiving from this internal node.
    :type receiver: :ref:`handle`

    :param int least_unacknowledged_sequence_number: The least sequence number this Exporter will ever be responsible for.
    '''
    self._node = node
    self._receiver = receiver

    # If None, then this Exporter is not duplicating.
    # Otherwise, the list of Exporter instances to duplicate to.
    self._duplicated_exporters = None

    # A list of pairs (time_sent_ms, message) of all messages that have been sent but not acknowledged, along with
    # the time at which they were sent.
    self._pending_messages = []

    # See `Exporter.least_unacknowledged_sequence_number`
    self._least_unacknowledged_sequence_number = least_unacknowledged_sequence_number

  def retransmit_expired_pending_messages(self, time_ms):
    '''
    Retransmit any pending messages that have been pending for too long.

    :param int time_ms: The current time in milliseconds on the sending `Node`
    '''
    cutoff_send_time_ms = time_ms - Exporter.PENDING_EXPIRATION_TIME_MS
    while self._pending_messages and self._pending_messages[0][0] <= cutoff_send_time_ms:
      t, message = self._pending_messages.pop(0)
      self._node.n_retransmissions += 1
      self._node.logger.warning(
          "Retransmitting message {sequence_number}", extra={'sequence_number': message['sequence_number']})
      self.export_message(message=message, time_ms=time_ms)

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

    # PERF(KK): binary search is possible here.
    self._pending_messages = [(t, msg) for t, msg in self._pending_messages
                              if msg['sequence_number'] >= self._least_unacknowledged_sequence_number]

  def export_message(self, message, time_ms):
    '''
    Export a message to the receiver.

    :param message: The message
    :type message: :ref:`message`
    :param int time_ms: The current time in milliseconds on the sending `Node`
    '''
    self._pending_messages.append((time_ms, message))
    self._node.send(self._receiver, message)

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
    return self._duplicated_exporters
