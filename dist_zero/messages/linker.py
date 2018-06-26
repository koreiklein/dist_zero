def sequence_message_send(message, sequence_number):
  '''
  Informs the receiving `Node` that it has just received a message with a sequence number.

  :param message: Any message
  :type message: :ref:`message`

  :param int sequence_number: The sequence number associated with this message.
  '''
  return {
      'type': 'sequence_message',
      'value': {
          'type': 'receive',
          'message': message,
          'sequence_number': sequence_number
      }
  }


def sequence_message_acknowledge(sequence_number):
  '''
  This message acknowledges the receipt of all sequence numbers < sequence_number on the recipient.

  :param int sequence_number: The least sequence number that sender has not acknowledged.
    All lower sequence numbers are acknowledged.
  '''
  return {'type': 'sequence_message', 'value': {'type': 'acknowledge', 'sequence_number': sequence_number}}
