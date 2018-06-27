from dist_zero import importer, exporter, errors


class Linker(object):
  '''
  For linking a possibly empty set of `Importer` instances to a possibly empty set of `Exporter` instances.

  Each linker is associated with some `Node`.
  As that node adds and removes `Importer` and `Exporter` classes, it will register them with its associated `Linker`
  object.
  It will also import messages on its importers
  When it wants to generate messages for exporters, it will call advance_sequence_number on the linker, and use
  that sequence number to export.
  It will also elapse time through the linker.

  The linker is then expected to make sure that `Exporter` instances retransmit at the right times
  and that `Importer` instances generate proper acknowledgements.
  '''

  TIME_BETWEEN_ACKNOWLEDGEMENTS_MS = 30
  '''The number of ms between acknowledgements sent to senders.'''

  TIME_BETWEEN_RETRANSMISSION_CHECKS_MS = 20
  '''The number of ms between checks for whether we should retransmit to receivers.'''

  def __init__(self, node):
    self._node = node

    self.now_ms = 0

    self._least_unused_sequence_number = 0
    self._branching = []
    '''
    An ordered list of pairs
    (sent_sequence_number, pairs)

    where sent_sequence_number is a sequence number that has been sent on all exporters
    and pairs is a list of pairs (importer, least_unreceived_sequence_number)
      where each pair gives the least unreceived sequence number of the importer at the time
      that sent_sequence_number was generated.
    '''

    self._importers = {}
    self._exporters = {}

    self.n_retransmissions = 0
    '''Number of times this node has retransmitted a message'''
    self.n_reorders = 0
    '''Number of times this node has received an out-of-order message'''
    self.n_duplicates = 0
    '''Number of times this node has received a message that was already received'''

    self._time_since_sent_acknowledgements = 0
    self._time_since_retransmitted_expired_pending_messages = 0
    self._initialized = False

  def initialize(self):
    if self._initialized:
      raise errors.InternalError("linker has already been initialized")

    self._initialized = True

    for importer in self._importers.values():
      importer.initialize()

    for exporter in self._exporters.values():
      exporter.initialize()

  @property
  def least_unused_sequence_number(self):
    return self._least_unused_sequence_number

  def advance_sequence_number(self):
    '''
    Generate and return a new sequence number.

    This method also tracks internally which Importer sequence numbers this sequence number corresponds to.
    '''
    result = self._least_unused_sequence_number
    self._branching.append((result, [(importer, importer.least_undelivered_remote_sequence_number)
                                     for sender_id, importer in self._importers.items()]))

    self._least_unused_sequence_number += 1

    return result

  def new_importer(self, sender):
    '''
    Generate and return a new `Importer` instance.

    :param sender: The :ref:`handle` of the node that will send for this importer.
    :type sender: :ref:`handle`
    '''
    result = importer.Importer(node=self._node, linker=self, sender=sender)
    self._importers[sender['id']] = result
    return result

  def new_exporter(self, receiver):
    '''
    Generate and return a new `Exporter` instance.

    :param receiver: The :ref:`handle` of the node that will receive for this exporter.
    :type receiver: :ref:`handle`
    '''
    result = exporter.Exporter(
        node=self._node,
        linker=self,
        receiver=receiver,
        # NOTE(KK): When you implement robustness during a migration, you should think very very carefully
        #   about how to set this parameter to guarantee correctness.
        least_unacknowledged_sequence_number=self._least_unused_sequence_number)
    self._exporters[receiver['id']] = result
    return result

  def receive_sequence_message(self, message, sender_id):
    if message['type'] == 'acknowledge':
      if sender_id in self._exporters:
        self._exporters[sender_id].acknowledge(message['sequence_number'])
      else:
        # In past cases where the exporter for a given sender id is not present, it was often
        # the case that the exporter was removed prematurely.
        # In fact, whenever a sender is removed, some acknowledgement message could in theory still be in flight
        # and arrive later on.  It's best to ingore these.
        self._node.logger.info(
            "Ignoring an acknowledgement for an unknown exporter.  It was likely already removed.",
            extra={'unrecognized_sender_id': sender_id})

    elif message['type'] == 'receive':
      self._importers[sender_id].import_message(message, sender_id)
    else:
      raise errors.InternalError('Unrecognized message type "{}"'.format(message['type']))

  def least_unacknowledged_sequence_number(self):
    '''
    The least sequence number that has not been acknowledged by every Exporter responsible for it.
    '''
    result = self._least_unused_sequence_number
    for exporter in self._exporters.values():
      if exporter.has_pending_messages():
        result = min(result, exporter.least_unacknowledged_sequence_number)
    return result

  def elapse(self, ms):
    '''
    Elapse time

    :param int ms: The number of elapsed milliseconds
    '''
    self.now_ms += ms

    self._time_since_sent_acknowledgements += ms
    self._time_since_retransmitted_expired_pending_messages += ms

    if self._time_since_sent_acknowledgements > Linker.TIME_BETWEEN_ACKNOWLEDGEMENTS_MS:
      self._send_acknowledgement_messages()
      self._time_since_sent_acknowledgements = 0

    if self._time_since_retransmitted_expired_pending_messages > Linker.TIME_BETWEEN_RETRANSMISSION_CHECKS_MS:
      self._retransmit_expired_pending_messages()
      self._time_since_retransmitted_expired_pending_messages = 0

  def _retransmit_expired_pending_messages(self):
    for exporter in self._exporters.values():
      exporter.retransmit_expired_pending_messages()

  def _send_acknowledgement_messages(self):
    least_unacknowledged_sequence_number = self.least_unacknowledged_sequence_number()

    branching_index = 0
    while branching_index < len(
        self._branching) and self._branching[branching_index][0] < least_unacknowledged_sequence_number:
      # PERF(KK): binary search is possible here.
      branching_index += 1

    if branching_index == 0:
      # No new messages need to be acknowledged.
      pass
    else:
      # The last pairings of sender_id with least_undelivered_remote_sequence_number before branching_index
      # will contain all the acknowledgements we need to send.
      for importer, least_undelivered_remote_sequence_number in self._branching[branching_index - 1][1]:
        if least_undelivered_remote_sequence_number == 0:
          # Do not send acknowledgements to importers that have never sent us a message.
          continue
        else:
          importer.acknowledge(least_undelivered_remote_sequence_number)

      self._branching = self._branching[branching_index:]

  def remove_exporter(self, exporter):
    del self._exporters[exporter.receiver_id]

  def remove_importer(self, importer):
    del self._importers[importer.sender_id]
