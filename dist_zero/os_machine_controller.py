import json
import logging
import os
import select
import socket
import sys
import time
import traceback
import uuid

from logstash_async.handler import AsynchronousLogstashHandler

import dist_zero.transport
import dist_zero.logging
from dist_zero import machine, messages, settings
from dist_zero.node import io
from dist_zero.spawners import docker

logger = logging.getLogger(__name__)


class OsMachineController(machine.MachineController):
  STEP_LENGTH_MS = 5 # Target number of milliseconds per iteration of the run loop.

  def __init__(self, id, name, mode):
    '''
    :param str id: The unique identity to use for this `OsMachineController`
    :param str name: A nice human readable name for this `OsMachineController`
    :param str mode: A mode (from `dist_zero.spawners`) (simulated, virtual, or cloud)
    '''
    self.id = id
    self.name = name
    self.mode = mode

    self._node_by_id = {}

    self._output_node_state_by_id = {} # dict from output node id to it's current state

    self._udp_port = settings.MACHINE_CONTROLLER_DEFAULT_UDP_PORT
    self._udp_dst = ('', self._udp_port)

    self._tcp_port = settings.MACHINE_CONTROLLER_DEFAULT_TCP_PORT
    self._tcp_dst = ('', self._tcp_port)

    self._udp_socket = None
    self._tcp_socket = None

    # A dict taking a pair (sender_node_id, receiver_node_id) to the transport to be used to send
    # from that sender to that receiver.
    self._transports = {}

  def handle(self):
    return messages.os_machine_controller_handle(self.id)

  def _update_output_node_state(self, node_id, f):
    new_state = f(self._output_node_state_by_id[node_id])
    self._output_node_state_by_id[node_id] = new_state

  def start_node(self, node_config):
    logger.info("Starting new '%s' node", node_config['type'], extra={'node_type': node_config['type']})
    if node_config['type'] == 'output_leaf':
      self._output_node_state_by_id[node_config['id']] = node_config['initial_state']
      node = io.OutputLeafNode.from_config(
          node_config=node_config,
          controller=self,
          update_state=lambda f: self._update_output_node_state(node_config['id'], f))
    else:
      node = machine.node_from_config(node_config, controller=self)

    self._node_by_id[node.id] = node
    node.initialize()
    return node

  def set_transport(self, sender, receiver, transport):
    self._transports[(sender['id'], receiver['id'])] = transport

  def send(self, node_handle, message, sending_node_handle=None):
    transport = self._transports[(sending_node_handle['id'], node_handle['id'])]
    dst = (transport['host'], settings.MACHINE_CONTROLLER_DEFAULT_UDP_PORT)
    msg = messages.machine_deliver_to_node(node=node_handle, message=message, sending_node=sending_node_handle)

    dist_zero.transport.send_udp(msg, dst)

  def _get_node_by_handle(self, node_handle):
    '''
    :param node_handle: The handle of a node managed by self.
    :type node_handle: :ref:`handle`
    :return: The node instance itself.
    '''
    return self._node_by_id[node_handle['id']]

  def ip_host(self):
    return socket.gethostname()

  def _handle_api_message(self, message):
    '''
    :param object message: A json message for the API
    :return: The API response to the message
    :rtype: object
    '''
    logger.info("API Message of type %s", message['type'], extra={'type': message['type']})
    if message['type'] == 'api_create_kid_config':
      node = self._node_by_id[message['internal_node_id']]
      logger.debug(
          "API is creating kid config %s for output node %s",
          message['new_node_name'],
          message['internal_node_id'],
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
          "API getting new transport for sending from node %s to node %s",
          message['sender']['id'],
          message['receiver']['id'],
          extra={
              'sender': message['sender'],
              'receiver': message['receiver']
          })
      return {
          'status': 'ok',
          'data': node.new_transport_for(message['sender']['id']),
      }
    elif message['type'] == 'api_get_output_state':
      return {
          'status': 'ok',
          'data': self._output_node_state_by_id[message['node']['id']],
      }
    else:
      logger.error("Unrecognized API message type %s", message['type'], extra={'type': message['type']})
      return {
          'status': 'failure',
          'reason': 'Unrecognized message type {}'.format(message['type']),
      }

  def _handle_message(self, message):
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
          "Delivering message of type %s to node %s",
          message['message']['type'],
          node_handle['id'],
          extra={
              'message_type': message['message']['type'],
              'to_node': node_handle,
          })
      node = self._get_node_by_handle(node_handle)
      node.receive(message=message['message'], sender=message['sending_node'])
    else:
      logger.error("Unrecognized message type %s", message['type'], extra={'unrecognized_type': message['type']})

  def _bind_udp(self):
    logger.info("OsMachineController binding UDP port {}".format(self._udp_port), extra={'port': self._udp_port})
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(self._udp_dst)
    self._udp_socket = sock

  def _bind_and_listen_tcp(self):
    logger.info("OsMachineController binding TCP port {}".format(self._tcp_port), extra={'port': self._tcp_port})
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(self._tcp_dst)
    sock.listen()
    logger.info("OsMachineController listening on TCP port {}".format(self._tcp_port), extra={'port': self._tcp_port})
    self._tcp_socket = sock

  def runloop(self):
    logger.info(
        "Starting run loop for machine %s: %s",
        self.name,
        self.id,
        extra={
            'machine_id': self.id,
            'machine_name': self.name,
        })
    self._bind_udp()
    self._bind_and_listen_tcp()

    while True:
      try:
        self._loop_iteration()
      except Exception as exn:
        e_type, e_value, e_tb = sys.exc_info()
        #tb_lines = traceback.format_tb(e_tb)
        exn_lines = traceback.format_exception(e_type, e_value, e_tb)
        logger.error("Exception in run loop: %s", ''.join(exn_lines), extra={'e_type': str(e_type)})
        # log the exception and go on.  These exceptions should not stop the run loop.
        continue

    for sock in [self._udp_socket, self._tcp_socket]:
      sock.close()

  def _loop_iteration(self):
    '''Run a single iterator of the run loop, raising any errors that come up.'''

    current_time_s = time.time()
    remaining_ms = OsMachineController.STEP_LENGTH_MS

    # First, elapse the whole time interval on all the nodes.
    self._elapse_nodes(remaining_ms)
    after_elapse_s = time.time()
    time_running_nodes_ms = (after_elapse_s - current_time_s) * 1000

    # Then, spend the remaining time waiting on messages from the network
    network_ms = remaining_ms - time_running_nodes_ms
    self._elapse_network(network_ms)

  def _elapse_nodes(self, ms):
    for node in self._node_by_id.values():
      node.elapse(ms)

  def _elapse_zero_or_one_network_messages(self, max_s):
    '''
    Wait on the network for not more than max_ms milleseconds, and process 0 or 1 messages.
    :param number max_s: The maximum number of seconds to wait for.
    '''
    readers, writers, errs = select.select(
        [self._udp_socket, self._tcp_socket],
        [],
        [],
        max_s,
    )
    for sock in readers:
      if sock == self._udp_socket:
        self._read_udp()
      elif sock == self._tcp_socket:
        self._accept_tcp()
      else:
        logger.error("Impossible! Unrecognized socket returned by select() %s", sock)

  def _elapse_network(self, remaining_ms):
    while remaining_ms > 0:
      before_network_s = time.time()
      self._elapse_zero_or_one_network_messages(remaining_ms / 1000)
      after_network_s = time.time()
      remaining_ms -= (after_network_s - before_network_s) * 1000

  def _accept_tcp(self):
    '''
    Process to completion a new tcp conection now available on the self._tcp_socket server socket.
    '''
    logger.debug("Accepting new TCP connection")
    client_sock, client_addr = self._tcp_socket.accept()
    buf = client_sock.recv(settings.MSG_BUFSIZE)
    logger.debug("Received {} bytes from TCP socket".format(len(buf)), extra={'bufsize': len(buf)})
    message = json.loads(buf.decode(messages.ENCODING))
    response = self._handle_api_message(message)
    binary = bytes(json.dumps(response), messages.ENCODING)
    client_sock.send(binary)
    client_sock.close()

  def _read_udp(self):
    '''Call this method whenever there is a datagram ready to read on the UDP socket'''
    buf, sender_address = self._udp_socket.recvfrom(settings.MSG_BUFSIZE)
    message = json.loads(buf.decode(messages.ENCODING))
    self._handle_message(message)

  def configure_logging(self):
    '''
    Configure logging for a `MachineController`
    '''
    # Filters
    str_format_filter = dist_zero.logging.StrFormatFilter()
    context = {
        'env': settings.DIST_ZERO_ENV,
        'mode': self.mode,
        'runner': False,
        'machine_id': self.id,
        'machine_name': self.name,
    }
    if settings.LOGZ_IO_TOKEN:
      context['token'] = settings.LOGZ_IO_TOKEN
    context_filter = dist_zero.logging.ContextFilter(context)

    # Formatters
    human_formatter = dist_zero.logging.HUMAN_FORMATTER
    json_formatter = dist_zero.logging.JsonFormatter('(asctime) (levelname) (name) (message)')

    # Handlers
    stdout_handler = logging.StreamHandler(sys.stdout)
    human_file_handler = logging.FileHandler(os.path.join(docker.DockerSpawner.CONTAINER_LOGS_DIR, 'output.log'))
    json_file_handler = logging.FileHandler(os.path.join(docker.DockerSpawner.CONTAINER_LOGS_DIR, 'output.json.log'))
    logstash_handler = AsynchronousLogstashHandler(
        settings.LOGSTASH_HOST,
        settings.LOGSTASH_PORT,
        database_path='/logs/.logstash.db',
    )

    stdout_handler.setLevel(logging.ERROR)
    human_file_handler.setLevel(logging.DEBUG)
    json_file_handler.setLevel(logging.DEBUG)
    logstash_handler.setLevel(logging.DEBUG)

    stdout_handler.setFormatter(human_formatter)
    human_file_handler.setFormatter(human_formatter)
    json_file_handler.setFormatter(json_formatter)
    logstash_handler.setFormatter(json_formatter)

    stdout_handler.addFilter(str_format_filter)
    human_file_handler.addFilter(str_format_filter)
    json_file_handler.addFilter(str_format_filter)
    json_file_handler.addFilter(context_filter)
    logstash_handler.addFilter(str_format_filter)
    logstash_handler.addFilter(context_filter)

    # Loggers
    dist_zero_logger = logging.getLogger('dist_zero')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    dist_zero.logging.set_handlers(root_logger, [
        json_file_handler,
        human_file_handler,
        logstash_handler,
        stdout_handler,
    ])
