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

  def send(self, node_handle, message, sending_node_id):
    '''
    Send a message to a node either managed by self, or linked to self.
    A transport must be set for the sender to send to the destination node.

    :param handle node_handle: The handle of a node
    :type node_handle: :ref:`handle`
    :param message message: A message for that node
    :type message: :ref:`message`
    :param str sending_node_id: The id of the sending node.
    '''
    raise RuntimeError("Abstract Superclass")

  def spawn_node(self, node_config):
    '''
    Start creating a new node on a linked machine.

    :param json node_config: A JSON serializable message that describes how to run the node.
    :param on_machine: The handle of a :any:`MachineController`.
    :type on_machine: :ref:`handle`
    :return: The id of the newly created node.
    :rtype: str
    '''
    raise RuntimeError("Abstract Superclass")

  def fresh_transport_for(self, local_node, new_node_id):
    raise RuntimeError("Abstract Superclass")

  def convert_transport_for(self, local_node, remote_node_handle):
    raise RuntimeError("Abstract Superclass")

  def new_transport_for(self, local_node, remote_node_handle):
    '''
    Create a transport that a remote node can use to link to a node on this machine.

    :param local_node: A node on this machine.
    :type local_noded: `Node`
    :param remote_node_handle: The :ref:`handle` of the node that will be using the returned handle to talk to local_node.
    :type remote_node_handle: :ref:`handle`

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

  def send(self, node_handle, message, sending_node_id):
    self._send_to_machine(
        message=messages.machine.machine_deliver_to_node(
            node_id=node_handle['id'], message=message, sending_node_id=sending_node_id),
        transport=node_handle['transport'])

  def spawn_node(self, node_config):
    # TODO(KK): Rethink how the machine for each node is chosen.  Always running on the same machine
    #   is easy, but an obviously flawed approach.

    # In general, the config should be serialized and deserialized at some point.
    # Do it here so that simulated tests don't accidentally share data.
    node_config = json.loads(json.dumps(node_config))

    return self.start_node(node_config).id

  def fresh_transport_for(self, local_node, new_node_id):
    return messages.machine.ip_transport(self._ip_host)

  def new_transport_for(self, local_node, remote_node_handle):
    return messages.machine.ip_transport(self._ip_host)

  def convert_transport_for(self, local_node, remote_node_handle):
    return messages.machine.ip_transport(self._ip_host)

  def get_node(self, handle):
    return self._node_by_id[handle['id']]

  def _update_output_node_state(self, node_id, f):
    new_state = f(self._output_node_state_by_id[node_id])
    self._output_node_state_by_id[node_id] = new_state

  def start_node(self, node_config):
    logger.info("Starting new '{node_type}' node", extra={'node_type': node_config['type']})
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
    elif message['type'] == 'api_fresh_handle':
      node = self._node_by_id[message['local_node_id']]
      logger.debug(
          "API is creating a fresh handle for a new node {new_node_id} to talk to the existing local node {local_node_id}",
          extra={
              'local_node_id': message['local_node_id'],
              'new_node_id': message['new_node_id'],
          })
      return {
          'status': 'ok',
          'data': node.fresh_handle(other_node_id=message['new_node_id']),
      }
    elif message['type'] == 'api_get_output_state':
      return {
          'status': 'ok',
          'data': self.get_output_state(message['node_id']),
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
      node_id = message['node_id']
      logger.info(
          "Delivering message of type {message_type} to node {to_node_pretty} from node {from_node_pretty}",
          extra={
              'message_type': message['message']['type'],
              'to_node_id': node_id,
              'from_node_id': message['sending_node_id'],
              'to_node_pretty': self._format_node_id_for_logs(node_id),
              'from_node_pretty': self._format_node_id_for_logs(message['sending_node']),
          })
      node = self._get_node_by_id(node_id)
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
