import json
import logging

from dist_zero import errors, messages

from .node import io
from .node.sum import SumNode

logger = logging.getLogger(__name__)


class MachineController(object):
  '''
  The interface that `Node` instances will use to interact with the underlying hardware.
  '''

  def set_transport(self, sender, receiver, transport):
    '''
    Set the transport for messages from sender to receiver.

    :param sender: The :ref:`handle` of the sending node. It must be managed by self.
    :type sender: :ref:`handle`

    :param receiver: The :ref:`handle` of the receiving node.
    :type receiver: :ref:`handle`
    '''
    raise RuntimeError("Abstract Superclass")

  def convert_transport_for(self, current_sender, new_sender_id, receiver_id):
    '''
    If there exists a transport that a node on self uses to talk to a receiver,
    this method creates a new transport allowing a different node to talk to the same receiver.

    :param current_sender: A node managed by self.
    :type current_sender: :ref:`handle`
    :param str new_sender: Any node id
    :param str receiver: Any node id for which current_sender has a working transport.

    :return: A transport that new_sender can use to talk to receiver
    :rtype: :ref:`transport`
    '''
    raise RuntimeError("Abstract Superclass")

  def send(self, node_handle, message, sending_node_handle):
    '''
    Send a message to a node either managed by self, or linked to self.
    A transport must be set for the sender to send to the destination node.

    :param handle node_handle: The handle of a node
    :type node_handle: :ref:`handle`
    :param message message: A message for that node
    :type message: :ref:`message`
    :param sending_node_handle: The handle of the sending node.
    :type sending_node_handle: :ref:`handle`
    '''
    raise RuntimeError("Abstract Superclass")

  def spawn_node(self, node_config):
    '''
    Start creating a new node on a linked machine.

    :param json node_config: A JSON serializable message that describes how to run the node.
    :param on_machine: The handle of a :any:`MachineController`.
    :type on_machine: :ref:`handle`
    :return: The handle of the newly created node.
    :rtype: :ref:`handle`
    '''
    raise RuntimeError("Abstract Superclass")

  def new_transport_for(self, local_node_id, remote_node_id):
    '''
    Create a transport that a remote node can use to link to a node on this machine.

    :param str local_node_id: The id of a node on this machine.
    :param str remote_node_id: The id of any node.

    :return: A transport that the remote node can use to send to the local node.
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

  def __init__(self, machine_id, machine_name, mode, system_id, ip_host, send_to_machine):
    '''
    :param str machine_id: The unique id of this machine.
    :param str machine_name: A name to use for this machine.
    :param str mode: A mode (from `dist_zero.spawners`) (simulated, virtual, or cloud)
    :param str system_id: The id of the overall distributed system
    :param str ip_host: The host parameter to use when generating transports that send to this machine.
    :param func send_to_machine: A function send_to_machine(message, transport)
      where message is a :ref:`message`, and transport is a :ref:`transport` for a receiving node.
    '''
    self.id = machine_id
    self.name = machine_name
    self.mode = mode

    self.system_id = system_id

    self._ip_host = ip_host

    self._node_by_id = {}

    self._output_node_state_by_id = {} # dict from output node id to it's current state

    self._send_to_machine = send_to_machine

    # A dict taking a pair (sender_node_id, receiver_node_id) to the transport to be used to send
    # from that sender to that receiver.
    self._transports = {}

  def set_transport(self, sender, receiver, transport):
    self._transports[(sender['id'], receiver['id'])] = transport

  def convert_transport_for(self, current_sender, new_sender_id, receiver_id):
    old_transport = self._get_transport(current_sender['id'], receiver_id)

    # TODO(KK): Once we start to add security to transports, it will no longer be possible
    #   to re-use the old transport as is.
    new_transport = old_transport

    return new_transport

  def _get_transport(self, sender_id, receiver_id):
    transport = self._transports.get((sender_id, receiver_id), None)

    if transport is None:
      raise errors.NoTransportError(sender_id=sender_id, receiver_id=receiver_id)
    else:
      return transport

  def send(self, node_handle, message, sending_node_handle):
    self._send_to_machine(
        message=messages.machine_deliver_to_node(node=node_handle, message=message, sending_node=sending_node_handle),
        transport=self._get_transport(sending_node_handle['id'], node_handle['id']))

  def spawn_node(self, node_config):
    # TODO(KK): Rethink how the machine for each node is chosen.  Always running on the same machine
    #   is easy, but an obviously flawed approach.

    # In general, the config should be serialized and deserialized at some point.
    # Do it here so that simulated tests don't accidentally share data.
    node_config = json.loads(json.dumps(node_config))

    return self.start_node(node_config).handle()

  def new_transport_for(self, local_node_id, remote_node_id):
    return messages.ip_transport(self._ip_host)

  def get_node(self, handle):
    return self._node_by_id[handle['id']]

  def _update_output_node_state(self, node_id, f):
    new_state = f(self._output_node_state_by_id[node_id])
    self._output_node_state_by_id[node_id] = new_state

  def start_node(self, node_config):
    logger.info("Starting new '{node_type}' node", extra={'node_type': node_config['type']})
    if node_config['type'] == 'OutputLeafNode':
      self._output_node_state_by_id[node_config['id']] = node_config['initial_state']
      node = io.OutputLeafNode.from_config(
          node_config=node_config,
          controller=self,
          update_state=lambda f: self._update_output_node_state(node_config['id'], f))
    elif node_config['type'] == 'InputLeafNode':
      node = io.InputLeafNode.from_config(node_config, controller=self)
    elif node_config['type'] == 'OutputLeafNode':
      node = io.OutputLeafNode.from_config(node_config, controller=self)
    elif node_config['type'] == 'InputNode':
      node = io.InputNode.from_config(node_config, controller=self)
    elif node_config['type'] == 'OutputNode':
      node = io.OutputNode.from_config(node_config, controller=self)
    elif node_config['type'] == 'SumNode':
      node = SumNode.from_config(node_config, controller=self)
    else:
      raise RuntimeError("Unrecognized type {}".format(node_config['type']))

    self._node_by_id[node.id] = node
    node.initialize()
    return node

  def _get_node_by_handle(self, node_handle):
    '''
    :param node_handle: The handle of a node managed by self.
    :type node_handle: :ref:`handle`
    :return: The node instance itself.
    '''
    return self._node_by_id[node_handle['id']]

  def handle_api_message(self, message):
    '''
    :param object message: A json message for the API
    :return: The API response to the message
    :rtype: object
    '''
    logger.info("API Message of type {message_type}", extra={'message_type': message['type']})
    if message['type'] == 'api_create_kid_config':
      node = self._node_by_id[message['internal_node_id']]
      logger.debug(
          "API is creating kid config {node_name} for output node {internal_node_id}",
          extra={
              'node_name': message['new_node_name'],
              'internal_node_id': message['internal_node_id']
          })
      return {
          'status': 'ok',
          'data': node.create_kid_config(message['new_node_name'], message['machine_controller_handle']),
      }
    elif message['type'] == 'api_new_transport':
      node = self._node_by_id[message['receiver']['id']]
      logger.info(
          "API getting new transport for sending from node {sender_id} node {receiver_id}",
          extra={
              'sender': message['sender'],
              'sender_id': message['sender']['id'],
              'receiver': message['receiver'],
              'receiver_id': message['receiver']['id'],
          })
      return {
          'status': 'ok',
          'data': node.new_transport_for(message['sender']['id']),
      }
    elif message['type'] == 'api_get_output_state':
      return {
          'status': 'ok',
          'data': self.get_output_state(message['node']['id']),
      }
    else:
      logger.error("Unrecognized API message type {message_type}", extra={'message_type': message['type']})
      return {
          'status': 'failure',
          'reason': 'Unrecognized message type {}'.format(message['type']),
      }

  def get_output_state(self, node_id):
    return self._output_node_state_by_id[node_id]

  def handle(self):
    return {'type': 'MachineController', 'id': self.id}

  def _format_handle_for_logs(self, handle):
    if handle is None:
      return 'None'
    else:
      return "{}:{}".format(handle['type'], handle['id'][:8])

  def handle_message(self, message):
    '''
    Handle an arbitrary machine message for this `MachineController` instance.

    :param message: A machine :ref:`message` for this `MachineController` instance.
    :type message: :ref:`message`
    '''
    if message['type'] == 'machine_start_node':
      self.start_node(message['node_config'])
    elif message['type'] == 'machine_deliver_to_node':
      node_handle = message['node']
      logger.info(
          "Delivering message of type {message_type} to node {to_node_pretty} from node {from_node_pretty}",
          extra={
              'message_type': message['message']['type'],
              'to_node': node_handle,
              'from_node': message['sending_node'],
              'to_node_pretty': self._format_handle_for_logs(node_handle),
              'from_node_pretty': self._format_handle_for_logs(message['sending_node']),
          })
      node = self._get_node_by_handle(node_handle)
      node.receive(message=message['message'], sender=message['sending_node'])
    else:
      logger.error("Unrecognized message type {unrecognized_type}", extra={'unrecognized_type': message['type']})

  def elapse_nodes(self, ms):
    '''
    Elapse ms milliseconds of time on all nodes managed by self.

    :param int ms: The number of milliseconds to elapse.
    '''
    # Elapsing time on nodes can create new nodes, thus changing the list of nodes.
    # Therefore, to avoid updating a dictionary while iterating over it, we make a copy
    for node in list(self._node_by_id.values()):
      node.elapse(ms)
