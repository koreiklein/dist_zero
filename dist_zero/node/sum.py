import logging

from dist_zero import messages, errors
from .node import Node

logger = logging.getLogger(__name__)


class SumNode(Node):
  SEND_INTERVAL_MS = 30 # Number of ms between sends to receivers.
  '''
  An internal node for summing all increments from its senders and forwarding the total to its receivers.
  '''

  def __init__(self, node_id, senders, receivers, controller):
    '''
    :param list senders: A list of :ref:`handle` of the nodes sending increments
    :param list receivers: A list of :ref:`handle` of the nodes to receive increments
    '''
    self._senders = senders
    self._receivers = receivers
    self._controller = controller
    self.id = node_id

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

    super(SumNode, self).__init__(logger)

  def handle(self):
    return {'type': 'SumNode', 'id': self.id, 'controller_id': self._controller.id}

  @staticmethod
  def from_config(node_config, controller):
    return SumNode(
        node_id=node_config['id'],
        senders=node_config['senders'],
        receivers=node_config['receivers'],
        controller=controller)

  def receive(self, sender, message):
    if message['type'] == 'increment':
      self._unsent_total += message['amount']
    elif message['type'] == 'add_link':
      node = message['node']
      direction = message['direction']
      transport = message['transport']

      if direction == 'sender':
        self._senders.append(node)
      elif direction == 'receiver':
        self._receivers.append(node)
      else:
        raise errors.InternalError("Unrecognized direction parameter '{}'".format(direction))

      self.set_transport(node, transport)
      self.send(node, messages.added_link(self.new_transport_for(node['id'])))
    else:
      self.logger.error("Unrecognized message {bad_msg}", extra={'bad_msg': message})

  def elapse(self, ms):
    self._unsent_time_ms += ms
    if self._unsent_total > 0 and self._unsent_time_ms > SumNode.SEND_INTERVAL_MS:
      self.logger.info("current n_senders = {n_senders}", extra={'n_senders': len(self._senders)})

      SENDER_LIMIT = 15
      if len(self._senders) > SENDER_LIMIT:
        self.logger.info("Hit sender limit of {sender_limit} senders", extra={'sender_limit': SENDER_LIMIT})
      self._send_to_all()

  def _send_to_all(self):
    self.logger.debug(
        "Sending new increment of {unsent_total} to all receivers", extra={'unsent_total': self._unsent_total})
    for receiver in self._receivers:
      message = messages.increment(self._unsent_total)
      self.send(receiver, message)
    self._unsent_time_ms = 0
    self._sent_total += self._unsent_total
    self._unsent_total = 0
