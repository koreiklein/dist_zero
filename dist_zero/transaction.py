import asyncio
from collections import defaultdict

import dist_zero.logging
from dist_zero import messages, ids, errors


class TransactionRoleController(object):
  '''
  Controller object to allow `TransactionRole` instances to interact with the overall transaction.
  '''

  def __init__(self, node: 'dist_zero.node.node.Node', transaction_id: str):
    self.node = node
    self.transaction_id = transaction_id

    self.logger = dist_zero.logging.LoggerAdapter(
        self.node.logger, extra={
            'cur_node_id': self.node.id,
            'transaction_id': self.transaction_id,
        })

    self._matcher = _Matcher()

  def new_handle(self, for_node_id):
    '''Create a new role handle that the identified role can use to send to self.'''
    handle = self.node.new_handle(for_node_id)
    handle['transaction_id'] = self.transaction_id
    return handle

  def transfer_handle(self, role_handle, for_node_id):
    '''
    Convert a role handle that self can use to send to a role ``r`` into a handle that
    another role can use to send to ``r``

    :param object role_handle: The handle that self can use to send to ``r``.
    :param str for_node_id: The id of the node that will run some other role ``s``.
    :return: A role handle that ``s`` can use to send to ``r``.
    :rtype: object
    '''
    handle = self.node.transfer_handle(self.role_handle_to_node_handle(role_handle), for_node_id)
    handle['transaction_id'] = self.transaction_id
    return handle

  def role_handle_to_node_handle(self, role_handle):
    '''Turn a role handle into a node handle for the underlying node.'''
    return {key: value for key, value in role_handle.items() if key != 'transaction_id'}

  def spawn_enlist(self, node_config: object, participant, args: dict):
    '''
    Spawn a new node and immediately enlist it in the current transaction with the provided `ParticipantRole`.

    :param object node_config: The node config describing what kind of node to spawn.
    :param participant: Identifies which `ParticipantRole` class will run on that node.
    :type participant: One of the `ParticipantRole` subclasses.
    :param dict args: Arguments to the ``__init__`` method of the identified `ParticipantRole` subclass.
    '''
    participant_typename = self._participant_typename(participant)

    new_config = add_participant_role_to_node_config(node_config, self.transaction_id, participant_typename, args)
    self.node._controller.spawn_node(new_config)

  def _participant_typename(self, participant):
    if not issubclass(participant, ParticipantRole):
      raise errors.InternalError(f"Unrecognized participant {participant} should be a subclass of ParticipantRole")
    return participant.__name__

  def enlist(self, node_handle: object, participant, args: dict):
    '''
    Enlist an existing node into this transaction by giving it a role.

    Importantly, across all `Node` instances in DistZero, there must exist a tree
    of ownership between nodes such that nodes are only even enlisted by their immediate parent.

    The existence of such a tree guarantees that transactions can not wind up in deadlock, as at any point
    in time, any transaction "lowest" in the tree is guaranteed to make progress.

    :param node_handle: The handle of a node "owned" by self.
    :type node_handle: :ref:`handle`
    :param participant: Identifies which `ParticipantRole` class will run on that node.
    :type participant: One of the `ParticipantRole` subclasses.
    :param dict args: Arguments to the ``__init__`` method of the identified `ParticipantRole` subclass.
    '''
    participant_typename = self._participant_typename(participant)

    self.node.send(
        node_handle,
        messages.transaction.start_participant_role(
            transaction_id=self.transaction_id,
            typename=participant_typename,
            args=args,
        ))

  def send(self, role_handle, message):
    '''
    Send a message to the handle of another role in the same transaction.

    The other role can call `TransactionRoleController.listen` to receive the message.

    :param object role_handle: The role handle of another role in the same transaction.
    :param object message: A serializable message to be sent to the other role.
    '''
    self.node.send(
        self.role_handle_to_node_handle(role_handle),
        messages.transaction.transaction_message(
            transaction_id=self.transaction_id,
            message=message,
        ))

  def deliver(self, message, sender_id):
    '''
    Deliver a message to the role.

    This method is to be called by general purpose message delivery code to indicate that a message has arrived
    for this role.
    '''
    self._matcher.deliver(message, sender_id)

  def listen(self, type):
    '''
    Return an awaitable that resolves when a particular type of message arrives for this role.
    '''
    future = self._matcher.install(type)
    return future


class _Matcher(object):
  def __init__(self):
    self._messages_by_type = defaultdict(list)
    self._future_by_type = {}

  def install(self, type):
    if self._messages_by_type[type]:
      future = asyncio.get_event_loop().create_future()
      future.set_result(self._messages_by_type[type].pop(0))
      return future
    elif type in self._future_by_type:
      raise errors.InternalError(f"A listener was already installed for messages of type \"{type}\".")
    else:
      future = asyncio.get_event_loop().create_future()
      self._future_by_type[type] = future
      return future

  def deliver(self, message, sender_id):
    type = message['type']
    if type in self._future_by_type:
      self._future_by_type.pop(type).set_result((message, sender_id))
    else:
      self._messages_by_type[type].append((message, sender_id))


class TransactionRole(object):
  '''
  Abstract base class for all transaction roles.
  '''

  async def run(self, controller: TransactionRoleController):
    '''
    Run exactly once when this `TransactionRole` instance starts to run.

    :param controller: A controller object for interacting with the overall transaction.
    :type controller: TransactionRoleController
    '''
    raise RuntimeError(f"Abstract Superclass {self.__class__}")


class OriginatorRole(TransactionRole):
  '''
  Abstract base class for `TransactionRole` instances that originate their transaction.
  '''


class ParticipantRole(TransactionRole):
  '''
  Abstract base class for `TransactionRole` instances that participate in a
  transaction originated by a separate `TransactionRole`.
  '''

  @staticmethod
  def from_config(typename: str, args: object):
    '''
    Parse a config generated by `start_participant_role`

    :param str typename: A string identitying a particular subclass of `ParticipantRole`
    :param object arg: The kwargs arguments to pass to that subclass's initializer.

    :return: The parsed `ParticipantRole`.
    :rtype: `ParticipantRole`
    '''
    from dist_zero import all_transactions
    if typename in all_transactions.__dict__:
      result = all_transactions.__dict__[typename](**args)
      return result
    else:
      raise errors.InternalError(
          f"Unrecognized transaction type name \"{typename}\" not found in dist_zero.all_transactions.")


def add_participant_role_to_node_config(node_config, transaction_id, participant_typename, args):
  new_config = {
      'start_participant_role':
      messages.transaction.start_participant_role(
          transaction_id=transaction_id,
          typename=participant_typename,
          args=args,
      )
  }
  new_config.update(node_config)
  return new_config
