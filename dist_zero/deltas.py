from dist_zero import errors


class Deltas(object):
  '''
  Each instance of `Deltas` will organize a set of newly received messages,
  and periodically combine and return them.
  '''

  def __init__(self):
    # Map from sender_id to a list of pairs (remote_sequence_number, message)
    self._sender_id_to_rsn_message_pairs = {}

    # Map from sender_id to the leaste sequence_number that has not been popped for that sender.
    self._first_unpopped = {}

  def add_sender(self, sender_id):
    '''
    Start tracking deltas for a new sender.
    '''
    if sender_id in self._sender_id_to_rsn_message_pairs or sender_id in self._first_unpopped:
      raise errors.InternalError("Sender was already added")
    self._sender_id_to_rsn_message_pairs[sender_id] = []
    self._first_unpopped[sender_id] = 0

  def add_message(self, sender_id, sequence_number, message):
    '''
    Store a message in self for use later on.
    '''
    if self.first_unseen_rsn(sender_id) == sequence_number:
      self._sender_id_to_rsn_message_pairs[sender_id].append((sequence_number, message))
    else:
      raise errors.InternalError("add_message was not called on the next sequential sequence number.")

  def first_unseen_rsn(self, sender_id):
    pairs = self._sender_id_to_rsn_message_pairs[sender_id]
    if pairs:
      rsn, msg = pairs[-1]
      return rsn + 1
    else:
      return self._first_unpopped[sender_id]

  def has_data(self):
    return any(self._sender_id_to_rsn_message_pairs.values())

  def covers(self, before):
    '''
    :param dict[str, int] before: A dictionary mapping sender ids to sequence numbers.
    :return: True iff for each pair (sender_id, sequence_number), self has received all messages with
      sequence numbers before sequence_number for that sender.
    :rtype: bool
    '''
    return all(self.first_unseen_rsn(sender_id) >= sequence_number for sender_id, sequence_number in before.items())

  def pop_deltas(self, before=None):
    '''
    Remove deltas from self, combine them, and return the result.

    :param dict before: None or a dict that maps each sender_id to a sequence_number from that sender.
      When this parameter is provided, pop_deltas will not remove only deltas for a sender_id and sequence_number
      where before[sender_id] < sequence_number.

    :return: A list of messages that have elapsed before before (or ever if before is None).
    :rtype: list
    '''
    increment = []
    for sender_id, pairs in list(self._sender_id_to_rsn_message_pairs.items()):
      new_pairs = []
      cap_number = before and before.get(sender_id, None)
      # PERF(KK): binary search would be faster
      for rsn, delta_message in pairs:
        if cap_number is None or rsn < cap_number:
          increment.append(delta_message)
          self._first_unpopped[sender_id] = max(self._first_unpopped[sender_id], rsn + 1)
        else:
          new_pairs.append((rsn, delta_message))

      self._sender_id_to_rsn_message_pairs[sender_id] = new_pairs

    return increment
