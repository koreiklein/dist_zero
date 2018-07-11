from dist_zero import importer, exporter, errors, messages


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

  def __init__(self, node, logger, deliver):
    '''
    :param object node: An object implementing methods with the format of `Node.send`, `Node.deliver`, `Node.new_handle`
    '''
    self._node = node
    self.logger = logger

    self.now_ms = 0

    self._importers = {}
    self._exporters = {}

    self.deliver = deliver

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
      self.send(importer.sender,
                messages.migration.connect_node(node=self.new_handle(importer.sender_id), direction='receiver'))

    for exporter in self._exporters.values():
      self.send(exporter.receiver,
                messages.migration.connect_node(node=self.new_handle(exporter.receiver_id), direction='sender'))

  @property
  def _branching(self):
    return self._node._branching

  @_branching.setter
  def _branching(self, value):
    self._node._branching = value

  def remove_exporters(self, receiver_ids):
    for receiver_id in receiver_ids:
      self._exporters.pop(receiver_id)

  def remove_importers(self, sender_ids):
    '''
    Remove a set of importers from this linker entirely.
    :param set[str] sender_ids: The identifiers for the importers to remove.
    '''
    for sender_id in sender_ids:
      self._importers.pop(sender_id)

    for i in range(len(self._branching)):
      sent_sequence_number, pairs = self._branching[i]
      self._branching[i] = (sent_sequence_number,
                            [(importer, least_unreceived_sequence_number)
                             for importer, least_unreceived_sequence_number in self._branching[i][1]
                             if importer.sender_id not in sender_ids])

  def new_handle(self, for_node_id):
    return self._node.new_handle(for_node_id)

  def send(self, receiver, message):
    self._node.send(receiver=receiver, message=message)

  @property
  def least_unused_sequence_number(self):
    return self._node.least_unused_sequence_number

  def advance_sequence_number(self):
    return self._node.advance_sequence_number(self._importers)

  def new_importer(self, sender, first_sequence_number=0, remote_sequence_number_to_early_message=None):
    '''
    Generate and return a new `Importer` instance.

    :param sender: The :ref:`handle` of the node that will send for this importer.
    :type sender: :ref:`handle`
    :param int first_sequence_number: The first sequence number this importer should expect to receive.
    '''
    result = importer.Importer(
        linker=self,
        sender=sender,
        first_sequence_number=first_sequence_number,
        remote_sequence_number_to_early_message=remote_sequence_number_to_early_message)
    self._importers[sender['id']] = result
    return result

  def new_exporter(self, receiver, migration_id=None):
    '''
    Generate and return a new `Exporter` instance.

    :param receiver: The :ref:`handle` of the node that will receive for this exporter.
    :type receiver: :ref:`handle`

    :param str migration_id: If the exporter will be running as part of the new flow during a migration,
      then the id of the migration.  Otherwise, `None`
    '''
    result = exporter.Exporter(
        linker=self,
        receiver=receiver,
        migration_id=migration_id,
    )
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
        self.logger.warning(
            "Ignoring an acknowledgement for an unknown exporter.  It was likely already removed.",
            extra={'unrecognized_sender_id': sender_id})

    elif message['type'] == 'receive':
      if hasattr(self._node, 'id') and self._node.id.startswith('SumNode_internal') and self._node._FIXME_swapped:
        if not hasattr(self, '_FIXME_receive_after_swap'):
          self._FIXME_receive_after_swap = 0
          self._FIXME_receive_after_swap_seq_numbers = set()
        else:
          if message['sequence_number'] not in self._FIXME_receive_after_swap_seq_numbers:
            self._FIXME_receive_after_swap += message['message']['amount']
            self._FIXME_receive_after_swap_seq_numbers.add(message['sequence_number'])
            #print('linker receive after swap total {}'.format(self._FIXME_receive_after_swap))
      if sender_id in self._importers:
        self._importers[sender_id].import_message(message, sender_id)
      else:
        # In case certain messages take a long time to arrive, it's possible
        # that an importer might be removed while the paired exporter is still retransmitting.
        # That's okay, and can be ignored.  We log a warning because it should be suspicious
        # when messages take too long to arrive.
        self.logger.warning(
            "Ignoring a message for an unknown importer.  It was likely already removed.",
            extra={'unrecognized_sender_id': sender_id})
    else:
      raise errors.InternalError('Unrecognized message type "{}"'.format(message['type']))

  def least_unacknowledged_sequence_number(self):
    '''
    The least sequence number that has not been acknowledged by every Exporter responsible for it.
    '''
    result = self.least_unused_sequence_number
    for exporter in self._exporters.values():
      if exporter.has_pending_messages():
        result = min(result, exporter.least_unacknowledged_sequence_number)
    return result

  def absorb_linker(self, linker):
    for k, v in linker._exporters.items():
      v.switch_linker(self)
      self._exporters[k] = v

    for k, v in linker._importers.items():
      v.switch_linker(self)
      self._importers[k] = v

  def elapse(self, ms):
    '''
    Elapse time

    :param int ms: The number of elapsed milliseconds
    '''
    self.now_ms += ms

    self._time_since_sent_acknowledgements += ms
    self._time_since_retransmitted_expired_pending_messages += ms

    if self._time_since_sent_acknowledgements > Linker.TIME_BETWEEN_ACKNOWLEDGEMENTS_MS:
      self.send_acknowledgement_messages()
      self._time_since_sent_acknowledgements = 0

    if self._time_since_retransmitted_expired_pending_messages > Linker.TIME_BETWEEN_RETRANSMISSION_CHECKS_MS:
      self._retransmit_expired_pending_messages()
      self._time_since_retransmitted_expired_pending_messages = 0

  def _retransmit_expired_pending_messages(self):
    for exporter in self._exporters.values():
      exporter.retransmit_expired_pending_messages()

  def _branching_index_for_least_unacknowledged_sequence_number(self):
    least_unacknowledged_sequence_number = self.least_unacknowledged_sequence_number()

    branching_index = 0
    while branching_index < len(
        self._branching) and self._branching[branching_index][0] < least_unacknowledged_sequence_number:
      # PERF(KK): binary search is possible here.
      branching_index += 1

    return branching_index

  def send_acknowledgement_messages(self):
    branching_index = self._branching_index_for_least_unacknowledged_sequence_number()

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
