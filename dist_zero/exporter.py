from dist_zero import messages, errors


class Exporter(object):
  '''
  Each instance of Exporter will be used by a `Node` to represent its end of a connection
  on which it is sending messages to another `Node` .

  Retransmission:

  When a node tries to export a message with the exporter, the exporter will send the message,
  and watch to see whether the message is acknowleded.  If after enough time the message is not acknowledged,
  it will resubmit it.

  '''

  PENDING_EXPIRATION_TIME_MS = 2 * 1000
  '''
  When a message has been sent, it will be put into a pending state.
  PENDING_EXPIRATION_TIME_MS milliseconds after a message is sent, if it still hasn't been acknowledged,
  it will be considered expired.

  Expired messages will be retransmitted during calls to `Exporter.retransmit_expired_pending_messages`
  '''

  def __init__(self, receiver, linker):
    '''
    :param receiver: The :ref:`handle` of the node receiving from this data node.
    :type receiver: :ref:`handle`

    :param linker: The linker associated with this exporter
    :type linker: `Linker`
    '''
    self._receiver = receiver

    self._linker = linker
    self.logger = self._linker.logger

    # If None, then this Exporter is not duplicating.
    # Otherwise, the list of Exporter instances to duplicate to.
    self.duplicated_exporters = None

    # A list of tuples (time_sent_ms, internal_sequence_number_sent, message)
    # of all messages that have been sent but not acknowledged, along with
    # the time and sequence number at which they were sent.
    self._pending_messages = []
    self._internal_sequence_number = 0
    self._internal_sequence_number_to_sequence_number = {}

    # See `Exporter.least_unacknowledged_sequence_number`
    self._least_internal_unacknowledged_sequence_number = 0
    self._least_unacknowledged_sequence_number = self._linker.least_unused_sequence_number

  @property
  def internal_sequence_number(self):
    return self._internal_sequence_number

  def switch_linker(self, linker):
    self._linker = linker

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
      t, internal_sequence_number, message = self._pending_messages.pop(0)
      self._linker.n_retransmissions += 1

      self.logger.warning(
          "Retransmitting message {internal_sequence_number}",
          extra={'internal_sequence_number': internal_sequence_number})
      self._export_message_self_only(message=message, internal_sequence_number=internal_sequence_number)

  @property
  def least_unacknowledged_sequence_number(self):
    '''
    The least natural number N such that
      - this `Exporter` has not received an acknowledgement for N
      - This `Exporter`'s receiver would be expected to acknowledge N
    '''
    return self._least_unacknowledged_sequence_number

  @property
  def receiver(self):
    '''The handle of the node receiving from this exporter'''
    return self._receiver

  @property
  def receiver_id(self):
    '''The id of the node receiving from this exporter'''
    return self._receiver['id']

  def acknowledge(self, internal_sequence_number):
    '''
    Acknowledge the receipt of all sequence numbers less than sequence_number.

    :param int internal_sequence_number: Some internal sequence number for which all
      smaller sequence numbers should now be acknowledged.
    '''
    self.logger.debug(
        "exporter acknowledges all sequence numbers below {acknowledged_sequence_number}",
        extra={'acknowledged_sequence_number': internal_sequence_number})

    if internal_sequence_number > self._least_internal_unacknowledged_sequence_number:
      self._least_internal_unacknowledged_sequence_number = internal_sequence_number
      self._least_unacknowledged_sequence_number = self._internal_sequence_number_to_sequence_number[
          internal_sequence_number]
      # Clear out extra entries in self._internal_sequence_number_to_sequence_number to keep it from growing unboundedly.
      # PERF(KK): An ordered list could change the asymptotics here... but the dictionary may never be large enough
      #   for that perf improvement to matter.
      self._internal_sequence_number_to_sequence_number = {
          k: v
          for k, v in self._internal_sequence_number_to_sequence_number.items() if k >= internal_sequence_number
      }

      # PERF(KK): binary search is possible here.
      self._pending_messages = [(t, sn, msg) for t, sn, msg in self._pending_messages
                                if sn >= self._least_internal_unacknowledged_sequence_number]

  def export_message(self, message, sequence_number):
    '''
    Export a message to whichever of the receiver or the duplicated receiver should receive it.

    :param message: The message
    :type message: :ref:`message`
    :param int sequence_number: The sequence number of the new message.
    '''
    if self.duplicated_exporters:
      # NOTE(KK): Call _export_message_self_only instead of export_message,
      #   as the whole idea of a middle node setting up a new
      #   duplicate while it is still migrating is super complex.
      #    We should try to avoid ever having to handle that case.
      for duplicated_exporter in self.duplicated_exporters:
        duplicated_exporter.export_message(message, sequence_number)

    self._export_message_self_only(message, self._internal_sequence_number)

    self._internal_sequence_number += 1
    self._internal_sequence_number_to_sequence_number[self._internal_sequence_number] = sequence_number + 1

  def _export_message_self_only(self, message, internal_sequence_number):
    '''
    Export a message to the receiver of self, but no duplicated receiver.

    :param message: The message
    :type message: :ref:`message`
    :param int internal_sequence_number: The sequence number of the new message.
    '''
    self._pending_messages.append((self._linker.now_ms, internal_sequence_number, message))
    sequence_message = messages.linker.sequence_message_send(message=message, sequence_number=internal_sequence_number)
    self._linker.send(self._receiver, sequence_message)
