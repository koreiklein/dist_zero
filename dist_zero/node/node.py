import asyncio
import logging

from collections import defaultdict

from cryptography.fernet import Fernet

import dist_zero.logging
from dist_zero import messages, linker, migration, deltas, errors

logger = logging.getLogger(__name__)


class AsyncNode(object):
  def __init__(self, node_id, controller):
    self.id = node_id

    self._controller = controller

    # For encryption/decryption
    self._fernet_key = Fernet.generate_key().decode(messages.ENCODING)
    self.fernet = Fernet(self._fernet_key)

    self.logger = dist_zero.logging.LoggerAdapter(logger, extra={'cur_node_id': self.id})

    self.linker = linker.Linker(self, logger=self.logger, deliver=self.deliver)

    # A list of pairs (dict, future), each dict of which can receive any message it maps.
    self._message_receiver_dicts = []

  def listen(self, listeners):
    '''
    Install listeners for some message types. And listen for the first message that matches.
    When a message of these types is received, resolve with the result
    of applying that coroutine.

    :param dict listeners: Maps messages types to functions f where f(message, sender_id) returns a coroutine.

    :return: Whatever awaitable is returned by the function f.
    '''
    future = asyncio.get_event_loop().create_future()
    self._message_receiver_dicts.append((listeners, future))
    return future

  async def receive(self, message, sender_id):
    new_pairs = []
    t = message['type']
    for d, f in self._message_receiver_dicts:
      if t in d:
        result = await d[t](message=message, sender_id=sender_id)
        f.set_result(result)
      else:
        new_pairs.append((d, f))
    self._message_receiver_dicts = new_pairs

  def sleep_ms(self, ms):
    return self._controller.sleep_ms(ms)

  def deliver(self, message, sequence_number, sender_id):
    # FIXME(KK): Implement this
    raise RuntimeError("Not Yet Implemented")

  def _handle(self, transport):
    return {
        'id': self.id,
        'controller_id': self._controller.id,
        'transport': transport,
        'fernet_key': self._fernet_key,
    }

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


class Node(object):
  '''Abstract base class for nodes'''

  def __init__(self, logger):
    self.logger = dist_zero.logging.LoggerAdapter(logger, extra={'cur_node_id': self.id})

    # For encryption/decryption
    self._fernet_key = Fernet.generate_key().decode(messages.ENCODING)
    self.fernet = Fernet(self._fernet_key)

    self.least_unused_sequence_number = 0

    self.migrators = {}
    '''
    A map from migration id to `Migrator` instance.
    It gives the migrators for the set of migrations that are currently migrating this `Node`
    '''

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

  def attach_migrator(self, migrator_config):
    migration_id = migrator_config['migration']['id']
    if len(self.migrators) > 0:
      # At some point, it may make sense to allow multiple migrators to run at once.  For now,
      # we disallow doing so.
      raise errors.InternalError("Not allowing more than a single migrator to run on a node at a given time.")
    elif migration_id in self.migrators:
      self.logger.error(
          "There is already a migration running on {cur_node_id} for migration {migration_id}",
          extra={'migration_id': migration_id})
      return self.migrators[migration_id]
    else:
      migrator = migration.migrator_from_config(migrator_config=migrator_config, node=self)
      self.migrators[migration_id] = migrator
      migrator.initialize()
      return migrator

  def remove_migrator(self, migration_id):
    '''Remove a migrator for self.migrators.'''
    self.migrators.pop(migration_id)

  def initialize(self):
    '''Called exactly once, when a node starts to run.'''
    pass

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
    elif message['type'] == 'attach_migrator':
      self.attach_migrator(message['migrator_config'])
    elif message['type'] == 'migration':
      migration_id, migration_message = message['migration_id'], message['message']
      if migration_id is None:
        self.receive(message=migration_message, sender_id=sender_id)
      elif migration_id not in self.migrators:
        # Possible, when a migration was removed at about the same time as some of the last few
        # acknowledgement or retransmission messages came through.
        self.logger.warning(
            "Got a migration message for a migration which is not running on this node.",
            extra={
                'migration_id': migration_id,
                'migration_message_type': migration_message['type']
            })
      else:
        self.migrators[migration_id].receive(sender_id=sender_id, message=migration_message)
    else:
      raise errors.InternalError("Unrecognized message type {}".format(message['type']))

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
