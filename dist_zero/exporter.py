from dist_zero import messages, errors


class Exporter(object):
  '''
  Instances of Exporter will be used by nodes internal to a computation to represent
  a destination for messages leaving the node.

  During migrations, they are responsible for duplicating messages.
  '''

  def __init__(self, node, receiver, retransmit, least_unacknowledged_sequence_number):
    '''
    :param node: The internal node.
    :type node: `Node`

    :param receiver: The :ref:`handle` of the node receiving from this internal node.
    :type receiver: :ref:`handle`

    :param bool retransmit: True iff this `Exporter` should retransmit messages which are not acknowledged within
      an appropriate amount of time.

    :param int least_unacknowledged_sequence_number: The least sequence number this Exporter will ever be responsible for.
    '''
    self._node = node
    self._receiver = receiver

    self._retransmit = retransmit

    # If None, then this Exporter is not duplicating.
    # Otherwise, the list of Exporter instances to duplicate to.
    self._duplicated_exporters = None

    # Messages that have been sent, but not yet acknowledged.
    self._pending_messages = []

    # See `Exporter.least_unacknowledged_sequence_number`
    self._least_unacknowledged_sequence_number = least_unacknowledged_sequence_number

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
    return self._receiver['id']

  def acknowledge(self, sequence_number):
    self._least_unacknowledged_sequence_number = max(self._least_unacknowledged_sequence_number, sequence_number)

    # PERF(KK): binary search is possible here.
    self._pending_messages = [
        msg for msg in self._pending_messages if msg['sequence_number'] < self._least_unacknowledged_sequence_number
    ]

  def export(self, message):
    self._pending_messages.append(message)
    self._node.send(self._receiver, message)

  def duplicate(self, exporters):
    '''
    Start duplicating this exporter.

    prerequisite: The exporter may not already be duplicating.

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
    self.logger.info(
        "Finishing duplication phase for {cur_node_id} ."
        "  Cutting back from {n_old_receivers} receivers to 1.",
        extra={
            'n_old_receivers': len(self._duplicated_exporters),
        })
    self._node.send(self._receiver, messages.migration.finished_duplicating())
    return self._duplicated_exporters
