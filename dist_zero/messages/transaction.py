'''
Messages for dealing with transactions.
'''


def start_participant_role(transaction_id, typename, args):
  '''
  Informs a node that it should eventually start running the specified `ParticipantRole`

  :param str transaction_id: The id of the associated transaction.
  :param str typename: A string identifying which subclass of `ParticipantRole` to instantiate.
  :param dict args: A dictionary of kwargs to pass to that subclass's initializer.
  '''
  return {
      'type': 'start_participant_role',
      'transaction_id': transaction_id,
      'typename': typename,
      'args': args,
  }


def transaction_message(transaction_id, message):
  return {
      'type': 'transaction_message',
      'transaction_id': transaction_id,
      'message': message,
  }
