from dist_zero import messages

class SumNode(object):
  '''
  An internal node for summing in a tree
  '''
  def __init__(self, senders, receivers, controller):
    self._senders = senders
    self._receivers = receivers
    self._controller = controller

    # Invariants:
    #   At certain points in time, a increment message is sent to every receiver.
    #   self._unsent_time_ms is the number of elapsed milliseconds since the last such point in time
    #   self._sent_total is the total amount of increment sent to receivers as of that point in time
    #     (note: the amonut is always identical for every receiver)
    #   self._unsent_total is the total amonut of increment received since that point in time.
    #   None of the increment in self._unsent_total has been sent.
    self._sent_total = 0
    self._unsent_total = 0
    self._unsent_time_ms = 0

  def receive(self, sender, message):
    if message['type'] == 'increment':
      self._unsent_total += message['amount']
    elif message['type'] == 'add_sender':
      self._senders.append(message['sender'])
    elif message['type'] == 'add_receiver':
      self._receivers.append(message['receiver'])
    else:
      raise RuntimeError("Unrecognized message {}".format(message))

  def elapse(self, ms):
    self._unsent_time_ms += ms
    if self._unsent_total > 0 and self._unsent_time_ms > SumNode.SEND_INTERVAL:
      self._send_to_all()

  def _send_to_all(self):
    for receiver in self._receivers:
      message = messages.increment(self._unsent_total)
      self._controller.send(receiver, message)
    self._unsent_time_ms = 0
    self._sent_total += self._unsent_total
    self._unsent_total = 0


