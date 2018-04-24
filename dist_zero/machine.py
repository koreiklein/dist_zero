import logging

from dist_zero import errors, messages

from .node import io
from .node.sum import SumNode

logger = logging.getLogger(__name__)


def node_output_file(node_id):
  '''
  :param str node_id: The id of an output leaf node
  :return: The filename of the file to which we might write the output state of that node.
  '''
  return '{}.state.json'.format(node_id)


def _node_from_config(node_config, controller):
  '''
  :param JSON node_config: A node config message
  :return: The node specified in that config.
  '''
  if node_config['type'] == 'input_leaf':
    return io.InputLeafNode.from_config(node_config, controller=controller)
  elif node_config['type'] == 'output_leaf':
    return io.OutputLeafNode.from_config(node_config, controller=controller)
  elif node_config['type'] == 'start_input':
    return io.InputNode.from_config(node_config, controller=controller)
  elif node_config['type'] == 'start_output':
    return io.OutputNode.from_config(node_config, controller=controller)
  elif node_config['type'] == 'sum':
    return SumNode.from_config(node_config, controller=controller)
  else:
    raise RuntimeError("Unrecognized type {}".format(node_config['type']))


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

  def spawn_node(self, node_config, on_machine):
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
  instance.  Whatever code manages the `NodeManager` is should deliver messages, api messages, and elapse time.
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

  def send(self, node_handle, message, sending_node_handle):
    transport = self._transports.get((sending_node_handle['id'], node_handle['id']), None)
    if transport is None:
      raise errors.NoTransportError(sender=sending_node_handle, receiver=node_handle)

    self._send_to_machine(
        message=messages.machine_deliver_to_node(node=node_handle, message=message, sending_node=sending_node_handle),
        transport=transport)

  def spawn_node(self, node_config, on_machine):
    # TODO(KK): Write tests for spawning a node and implement this method.
    raise RuntimeError("Not Yet Implemented")

  def new_transport_for(self, local_node_id, remote_node_id):
    return messages.ip_transport(self._ip_host)

  def get_node(self, handle):
    return self._node_by_id[handle['id']]

  def _update_output_node_state(self, node_id, f):
    new_state = f(self._output_node_state_by_id[node_id])
    self._output_node_state_by_id[node_id] = new_state

  def start_node(self, node_config):
    logger.info("Starting new '{node_type}' node", extra={'node_type': node_config['type']})
    if node_config['type'] == 'output_leaf':
      self._output_node_state_by_id[node_config['id']] = node_config['initial_state']
      node = io.OutputLeafNode.from_config(
          node_config=node_config,
          controller=self,
          update_state=lambda f: self._update_output_node_state(node_config['id'], f))
    else:
      node = _node_from_config(node_config, controller=self)

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
          "Delivering message of type {message_type} to node {to_node}",
          extra={
              'message_type': message['message']['type'],
              'to_node': node_handle,
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
    for node in self._node_by_id.values():
      node.elapse(ms)
