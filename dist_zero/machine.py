import heapq
import json
import logging
import re

from cryptography.fernet import Fernet
from random import Random

from dist_zero import errors, messages

from .node import io
from .node.sum import SumNode
from .node.computation import ComputationNode
from .migration.migration_node import MigrationNode

logger = logging.getLogger(__name__)


class MachineController(object):
  '''
  The interface that `Node` instances will use to interact with the underlying hardware.
  '''

  def send(self, node_handle, message, sending_node_id):
    '''
    Send a message to a node either managed by self, or linked to self.

    :param handle node_handle: The handle of a node
    :type node_handle: :ref:`handle`
    :param message message: A message for that node
    :type message: :ref:`message`
    :param str sending_node_id: The id of the sending node.
    '''
    raise RuntimeError("Abstract Superclass")

  def spawn_node(self, node_config):
    '''
    Asynchronously trigger the creation of a new node on a linked machine.

    :param json node_config: A JSON serializable message that describes how to run the node.
    :param on_machine: The handle of a :any:`MachineController`.
    :type on_machine: :ref:`handle`
    :return: The id of the newly created node.
    :rtype: str
    '''
    raise RuntimeError("Abstract Superclass")

  def terminate_node(self, node_id):
    '''
    Stop running a `Node`.

    :param str node_id: The id of the node to terminate.
    '''
    raise RuntimeError("Abstract Superclass")

  def new_transport(self, node, for_node_id):
    '''
    Create a new transport for sending to a local node.

    :param node: A `Node` managed by self.
    :type node: `Node`
    :param str for_node_id: The id of some other `Node`

    :return: A :ref:`transport` that the other `Node` can use to send to ``node``.
    :rtype: :ref:`transport`
    '''
    raise RuntimeError("Abstract Superclass")

  def transfer_transport(self, transport, for_node_id):
    '''
    Create a new transport suitable for a distinct sender.

    :param transport: A :ref:`transport` that some local sender `Node` can use to send to some receiver `Node`.
    :type transport: :ref:`transport`
    :param str for_node_id: The id of some new sender `Node`.

    :return: A :ref:`transport` that the new sender `Node` can use to send to the same receiver `Node`.
    :rtype: :ref:`transport`
    '''
    raise RuntimeError("Abstract Superclass")


class NodeManager(MachineController):
  '''
  The only implementation of `MachineController`.  A `NodeManager` instance exposes two interfaces.

  The first interface is the `MachineController` interface to the `Node` instances it manages.

  The second interface is to whatever is running the `NodeManager` .  In virtual and cloud modes,
  the `NodeManager` is probably managed by a `MachineRunner` runloop for the process responding to the passage of time
  and to events on a socket.  In simulated mode, the `NodeManager` is managed by a `SimulatedSpawner`
  instance.  Either way, whatever manages the `NodeManager` must elapse time,
  and deliver ordinary messages and api messages.
  '''

  MIN_POSTPONE_TIME_MS = 10
  '''Minimum time a message will be postpone when simulating a network drop or reorder'''
  MAX_POSTPONE_TIME_MS = 1200
  '''Maximum time a message will be postpone when simulating a network drop or reorder'''

  def __init__(self, machine_config, spawner, ip_host, send_to_machine):
    '''
    :param machine_config: A configuration message of type 'machine_config'
    :type machine_config: :ref:`message`

    :param spawner: A `Spawner` instance to use when creating new nodes.
    :type spawner: `Spawner`

    :param str ip_host: The host parameter to use when generating transports that send to this machine.
    :param func send_to_machine: A function send_to_machine(message, transport)
      where message is a :ref:`message`, and transport is a :ref:`transport` for a receiving node.
    '''

    self.id = machine_config['id']
    self.name = machine_config['machine_name']
    self.mode = machine_config['mode']

    self.system_config = machine_config['system_config']

    self._random = Random(machine_config['random_seed']) if machine_config['random_seed'] is not None else Random()

    self._network_errors_config = self._parse_network_errors_config(machine_config['network_errors_config'])

    self.system_id = machine_config['system_id']

    self._spawner = spawner

    self._ip_host = ip_host

    self._node_by_id = {}

    self._now_ms = 0 # Current elapsed time in milliseconds
    # a heap (as in heapq) of tuples (ms_of_occurence, send_receive, args)
    # where args are the args to self._send_without_error_simulation or self._receive_without_error_simulation
    # depending on whether send_or_receive is 'send' or 'receive'
    self._pending_events = []

    self._output_node_state_by_id = {} # dict from output node id to it's current state

    self._send_to_machine = send_to_machine

  def _parse_network_errors_config(self, network_errors_config):
    return {
        direction: {
            key: {
                'rate': value['rate'],
                'regexp': re.compile(value['regexp'])
            }
            for key, value in direction_config.items()
        }
        for direction, direction_config in network_errors_config.items()
    }

  def send(self, node_handle, message, sending_node):
    sending_node_id = None if sending_node is None else sending_node.id
    error_type = self._get_simulated_network_error(message, direction='outgoing')
    send_args = (node_handle, message, sending_node_id)
    if error_type:
      logger.info(
          'Simulating {error_type} of an outgoing message',
          extra={
              'sender_id': sending_node_id,
              'error_type': error_type
          })
      if error_type == 'drop':
        pass
      elif error_type == 'reorder':
        heapq.heappush(self._pending_events, (self._postpone_ms(), 'send', json.dumps(send_args)))
      elif error_type == 'duplicate':
        self._send_without_error_simulation(node_handle, message, sending_node_id)
        heapq.heappush(self._pending_events, (self._postpone_ms(), 'send', json.dumps(send_args)))
      else:
        raise errors.InternalError("Unrecognized error type '{}'".format(error_type))
    else:
      self._send_without_error_simulation(*send_args)

  def _postpone_ms(self):
    return (self._now_ms + NodeManager.MIN_POSTPONE_TIME_MS +
            int(self._random.random() * (NodeManager.MAX_POSTPONE_TIME_MS - NodeManager.MIN_POSTPONE_TIME_MS)))

  def _send_without_error_simulation(self, node_handle, message, sending_node_id):
    '''Like `NodeManager.send`, but do not simulate any errors'''
    logger.debug(
        "Sending message from {sending_node_id} to {recipient_handle}: {message_type}",
        extra={
            'sending_node_id': sending_node_id,
            'recipient_handle': node_handle,
            'message_type': message['type'],
        })

    fernet = Fernet(node_handle['fernet_key'])
    encoded_message = fernet.encrypt(json.dumps(message).encode(messages.ENCODING)).decode(messages.ENCODING)
    self._send_to_machine(
        message=messages.machine.machine_deliver_to_node(
            node_id=node_handle['id'], message=encoded_message, sending_node_id=sending_node_id),
        transport=node_handle['transport'])

  def spawn_node(self, node_config):
    # In general, the config should be serialized and deserialized at some point.
    # Do it here so that simulated tests don't accidentally share data.
    node_config = json.loads(json.dumps(node_config))
    # PERF(KK): This serialization/deserialization can be taken out when not in simulated mode.

    # TODO(KK): Always running the new Node on the same controller that spawns it is clearly
    #   broken.  Come up with test cases that nodes are spawned in more reasonable placed and fix it.
    self.start_node(node_config)

    return node_config['id']

  def terminate_node(self, node_id):
    self._node_by_id.pop(node_id)

  def new_transport(self, node, for_node_id):
    return messages.machine.ip_transport(self._ip_host)

  def transfer_transport(self, transport, for_node_id):
    return dict(transport)

  def get_node_by_id(self, node_id):
    return self._node_by_id[node_id]

  @property
  def n_nodes(self):
    return len(self._node_by_id)

  def _update_output_node_state(self, node_id, f):
    if self._output_node_state_by_id[node_id] is None:
      import ipdb
      ipdb.set_trace()
    new_state = f(self._output_node_state_by_id[node_id])
    self._output_node_state_by_id[node_id] = new_state

  def start_node(self, node_config):
    logger.info(
        "Starting new '{node_type}' node {node_id} on machine '{machine_name}'",
        extra={
            'node_type': node_config['type'],
            'node_id': self._format_node_id_for_logs(node_config['id']),
            'machine_name': self.name,
        })
    if node_config['type'] == 'LeafNode':
      self._output_node_state_by_id[node_config['id']] = node_config['initial_state']
      node = io.LeafNode.from_config(
          node_config=node_config,
          controller=self,
          update_state=lambda f: self._update_output_node_state(node_config['id'], f))
    elif node_config['type'] == 'InternalNode':
      node = io.InternalNode.from_config(node_config, controller=self)
    elif node_config['type'] == 'SumNode':
      node = SumNode.from_config(node_config, controller=self)
    elif node_config['type'] == 'MigrationNode':
      node = MigrationNode.from_config(node_config, controller=self)
    elif node_config['type'] == 'ComputationNode':
      node = ComputationNode.from_config(node_config, controller=self)
    else:
      raise RuntimeError("Unrecognized type {}".format(node_config['type']))

    self._node_by_id[node.id] = node
    node.initialize()
    return node

  def _get_node_by_id(self, node_id):
    '''
    :param str node_id: The id of a node managed by self.
    :return: The node instance itself.
    '''
    return self._node_by_id[node_id]

  def handle_api_message(self, message):
    '''
    :param object message: A json message for the API
    :return: The API response to the message
    :rtype: object
    '''
    logger.info("API Message of type {message_type}", extra={'message_type': message['type']})
    if message['type'] == 'api_node_message':
      node = self._node_by_id[message['node_id']]
      return {
          'status': 'ok',
          'data': node.handle_api_message(message['message']),
      }
    else:
      logger.error("Unrecognized API message type {message_type}", extra={'message_type': message['type']})
      return {
          'status': 'failure',
          'reason': 'Unrecognized message type {}'.format(message['type']),
      }

  def get_output_state(self, node_id):
    return self._output_node_state_by_id[node_id]

  def _format_node_id_for_logs(self, node_id):
    if node_id is None:
      return 'None'
    else:
      return node_id[:8]

  def handle_message(self, message):
    '''
    Handle an arbitrary machine message for this `MachineController` instance.

    :param message: A machine :ref:`message` for this `MachineController` instance.
    :type message: :ref:`message`
    '''
    if message['type'] == 'machine_start_node':
      self.start_node(message['node_config'])
    elif message['type'] == 'machine_deliver_to_node':
      sender_id = message['sending_node_id']
      node_id = message['node_id']
      node = self._get_node_by_id(node_id)

      decoded_message = json.loads(
          node.fernet.decrypt(message['message'].encode(messages.ENCODING)).decode(messages.ENCODING))

      error_type = self._get_simulated_network_error(message, direction='outgoing')
      receive_args = (node_id, decoded_message, sender_id)
      if error_type:
        logger.info(
            'Simulating {error_type} of an outgoing message', extra={
                'sender_id': sender_id,
                'error_type': error_type
            })
        if error_type == 'drop':
          pass
        elif error_type == 'reorder':
          heapq.heappush(self._pending_events, (self._postpone_ms(), 'receive', json.dumps(receive_args)))
        elif error_type == 'duplicate':
          self._receive_without_error_simulation(node_id, decoded_message, sender_id)
          heapq.heappush(self._pending_events, (self._postpone_ms(), 'receive', json.dumps(receive_args)))
        else:
          raise errors.InternalError("Unrecognized error type '{}'".format(error_type))
      else:
        self._receive_without_error_simulation(*receive_args)
    else:
      logger.error("Unrecognized message type {unrecognized_type}", extra={'unrecognized_type': message['type']})

  def _receive_without_error_simulation(self, node_id, message, sender_id):
    '''receive a message to the proper node without any network error simulations'''
    node = self._get_node_by_id(node_id)
    logger.info(
        "Node is receiving message of type {message_type} from {sender_id}",
        extra={
            'message_type': message['type'],
            'sender_id': sender_id,
        })
    node.receive(message=message, sender_id=sender_id)

  def elapse_nodes(self, ms):
    '''
    Elapse ms milliseconds of time on all nodes managed by self.

    Also, simulate any postponed network activity from network errors generated earlier.

    :param int ms: The number of milliseconds to elapse.
    '''

    final_time_ms = self._now_ms + ms

    while self._pending_events and self._pending_events[0][0] <= final_time_ms:
      t, send_or_receive, args = heapq.heappop(self._pending_events)
      if send_or_receive == 'send':
        self._send_without_error_simulation(*json.loads(args))
      elif send_or_receive == 'receive':
        self._receive_without_error_simulation(*json.loads(args))
      else:
        raise errors.InternalError("Unrecognized 'send' or 'receive': {}".format(send_or_receive))

      self._elapse_nodes_without_simulated_network_messages(t - self._now_ms)
      self._now_ms = t

    self._elapse_nodes_without_simulated_network_messages(final_time_ms - self._now_ms)
    self._now_ms = final_time_ms

  def _elapse_nodes_without_simulated_network_messages(self, ms):
    '''Like elapse_nodes, but do no simulate any postponed network activity.'''
    # Elapsing time on nodes can create new nodes, thus changing the list of nodes.
    # Therefore, to avoid updating a dictionary while iterating over it, we make a copy
    for node in list(self._node_by_id.values()):
      node.elapse(ms)

  def _get_simulated_network_error(self, message, direction):
    '''
    Determine whether we should simulate a network error on a message.

    :param message: Any message that a `Node` on this machine is trying to send.
    :type message: :ref:`message`
    :param str direction: 'incomming' or 'outgoing'. Indicates whether the message is being received or sent respectively.

    :return: The error type to simulate, or `None` if we should not simulate a network error at all.
    :rtype: str
    '''
    network_errors_config = self._network_errors_config[direction]
    for error_type, match_parameters in network_errors_config.items():
      rate = match_parameters['rate']
      regexp = match_parameters['regexp']
      if rate > 0.0 and self._random.random() < rate and regexp.match(json.dumps(message)):
        return error_type

    return False
