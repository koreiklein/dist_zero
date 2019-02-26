import asyncio
import logging

from collections import defaultdict

from cryptography.fernet import Fernet

import dist_zero.logging
from dist_zero import messages, linker, deltas, errors, ids, transaction

logger = logging.getLogger(__name__)


class Node(object):
  '''Abstract base class for nodes'''

  def __init__(self, logger):
    self.logger = dist_zero.logging.LoggerAdapter(logger, extra={'cur_node_id': self.id})

    # For encryption/decryption
    self._fernet_key = Fernet.generate_key().decode(messages.ENCODING)
    self.fernet = Fernet(self._fernet_key)

    self.least_unused_sequence_number = 0

    # Queue of transaction roles to run.  Iff nonempty, a coroutine is running self._transaction_role_queue[0]
    self._transaction_role_queue = []
    # Maps transaction_id to ordered list of messages that have
    #  - been received while that transaction was not active
    #  - not yet been delivered
    self._postponed_transaction_messages = defaultdict(list)

    self.linker = linker.Linker(self, logger=self.logger, deliver=self.deliver)
    self.system_config = self._controller.system_config
    '''
    System configuration parameters.
    '''

    self._deltas = deltas.Deltas()
    self.deltas_only = set()
    '''
    When true, this node should never apply deltas to its current state.  It should collect them in the deltas
    map instead.
    '''

  def set_fernet(self, node):
    self._fernet_key = node._fernet_key
    self.fernet = node.fernet

  def send(self, receiver, message):
    '''
    Encrypt and send a message to a receiver.

    :param receiver: A :ref:`handle` for the intended recipient `Node` of the message.
    :type receiver: :ref:`handle`
    :param message: The message.
    :type message: :ref:`message`
    '''
    self._controller.send(node_handle=receiver, message=message, sending_node=self)

  def new_handle(self, for_node_id):
    '''
    Create a new handle for sending to self.

    :param str for_node_id: The id of the node that will be sending via the new handle.
    :return: A :ref:`handle` that the ``for_node`` can use to send messages to self.
    :rtype: :ref:`handle`
    '''
    return self._handle(transport=self._controller.new_transport(node=self, for_node_id=for_node_id))

  def transfer_handle(self, handle, for_node_id):
    '''
    Given a handle for use by self, create a new handle for use by another node.

    :param handle: A :ref:`handle` that self can use to send to another node.
    :type handle: :ref:`handle`
    :param str for_node_id: The id of some other node.

    :return: A :ref:`handle` that the ``for_node`` will be able to use to send to the node referenced by handle.
    :rtype: :ref:`handle`
    '''
    return {
        'id': handle['id'],
        'fernet_key': handle['fernet_key'],
        'controller_id': handle['controller_id'],
        'transport': self._controller.transfer_transport(transport=handle['transport'], for_node_id=for_node_id),
    }

  def _handle(self, transport):
    return {
        'id': self.id,
        'controller_id': self._controller.id,
        'transport': transport,
        'fernet_key': self._fernet_key,
    }

  def elapse(self, ms):
    '''
    Elapse ms of time on this node.

    :param int ms: A number of milliseconds.
    '''
    raise RuntimeError('Abstract Superclass')

  def deliver(self, message, sequence_number, sender_id):
    '''
    Abstract method for delivering new messages to this node.
    '''
    raise RuntimeError('Abstract Superclass')

  def receive(self, message, sender_id):
    '''
    Receive a message from a sender.

    :param str message: The message.
    :param str sender_id: The id of the sender `Node`, or `None` if the message was not generated by a sender.
      (pre-recorded messages will not have a sender).
    '''
    if message['type'] == 'sequence_message':
      self.linker.receive_sequence_message(message['value'], sender_id)
    elif message['type'] == 'start_participant_role':
      self.start_participant_role(message)
    elif message['type'] == 'transaction_message':
      if len(self._transaction_role_queue) > 0:
        active_role, active_role_controller = self._transaction_role_queue[0]

        if active_role_controller.transaction_id == message['transaction_id']:
          active_role_controller.deliver(message['message'], sender_id)
        else:
          self._postponed_transaction_messages[message['transaction_id']].append((message['message'], sender_id))
    else:
      raise errors.InternalError("Unrecognized message type {}".format(message['type']))

  def start_participant_role(self, message):
    from dist_zero.transaction import ParticipantRole
    self._start_transaction_participant_eventually(
        transaction_id=message['transaction_id'],
        role=ParticipantRole.from_config(typename=message['typename'], args=message['args']))

  def checkpoint(self, before=None):
    '''
    Flush all delayed messages that need to be sent and advance the linker.
    '''
    raise RuntimeError('Abstract Superclass: {}'.format(self.__class__))

  def stats(self):
    '''
    :return: A dictionary of statistics about this `Node`
    :rtype: dict
    '''
    return {
        'n_retransmissions': self.linker.n_retransmissions,
        'n_reorders': self.linker.n_reorders,
        'n_duplicates': self.linker.n_duplicates,
        'sent_messages': self.linker.least_unused_sequence_number,
        'acknowledged_messages': self.linker.least_unacknowledged_sequence_number(),
    }

  def handle_api_message(self, message):
    '''
    Handle an api message.
    '''
    if message['type'] == 'create_kid_config':
      return self.create_kid_config(name=message['new_node_name'], machine_id=message['machine_id'])
    elif message['type'] == 'new_handle':
      self.logger.debug(
          "API is creating a new handle for a new node {new_node_id} to send to the existing local node {local_node_id}",
          extra={
              'local_node_id': self.id,
              'new_node_id': message['new_node_id'],
          })
      return self.new_handle(for_node_id=message['new_node_id'])
    elif message['type'] == 'get_stats':
      return self.stats()
    else:
      self.logger.error('Unrecognized node api message of type "{}"'.format(message['type']))

  # Methods relevant to transactions

  def start_transaction_eventually(self, role: 'dist_zero.transaction.OriginatorRole'):
    '''
    Ensure that ``role.run`` will eventually be called with a `TransactionRoleController` for a new transaction on self.

    :param role: The originator role instance defining the behavior of the overall transaction.
    :type role: `OriginatorRole`
    '''
    controller = transaction.TransactionRoleController(
        node=self, transaction_id=ids.new_id(f'Transaction__{role.__class__.__name__}'), role_class=role.__class__)
    self._start_role_eventually(role, controller)

  def _start_role_eventually(self, role, controller):
    if role.log_starts_and_stops:
      self.logger.debug("Enqueueing role {role_name}", extra={'role_name': role.__class__.__name__})
    self._transaction_role_queue.append((role, controller))
    if len(self._transaction_role_queue) == 1:
      # No coroutine is running transactions (the queue used to be empty), we need to create one
      self._controller.create_task(self._run_transaction_roles_till_empty())
    else:
      # A coroutine is already running transactions, it will eventually get to ``role``
      pass

  def _start_transaction_participant_eventually(self, transaction_id: str,
                                                role: 'dist_zero.transaction.ParticipantRole'):
    controller = transaction.TransactionRoleController(
        node=self, transaction_id=transaction_id, role_class=role.__class__)
    self._start_role_eventually(role, controller)

  async def _run_transaction_roles_till_empty(self):
    while len(self._transaction_role_queue) > 0:
      # Note that while awaiting the below call to ``run``, more roles may be added
      await self._run_transaction_role(*self._transaction_role_queue[0])
      self._transaction_role_queue.pop(0)

  async def _deliver_postponed_transaction_messages(self, role_controller, msgs):
    for (msg, sender_id) in msgs:
      role_controller.deliver(msg, sender_id)

  async def _run_transaction_role(self, role, controller):
    if controller.transaction_id in self._postponed_transaction_messages:
      msgs = self._postponed_transaction_messages.pop(controller.transaction_id)
      self._controller.create_task(self._deliver_postponed_transaction_messages(controller, msgs))
    if role.log_starts_and_stops:
      controller.logger.info("Starting Transaction Role: {role}")
    await role.run(controller)
    if role.log_starts_and_stops:
      controller.logger.info("Finished Transaction Role: {role}")
