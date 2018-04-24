import json
import logging
import os
import select
import socket
import sys
import time
import traceback

from logstash_async.handler import AsynchronousLogstashHandler

import dist_zero.transport
import dist_zero.logging

from dist_zero import settings, machine, messages
from dist_zero.spawners import docker

logger = logging.getLogger(__name__)


class MachineRunner(object):
  STEP_LENGTH_MS = 5 # Target number of milliseconds per iteration of the run loop.
  '''
  For running A NodeManager on a machine inside a runloop.
  Real time is passed in, and messages are read from os sockets.
  '''

  def __init__(self, machine_id, machine_name, mode, system_id):

    self._udp_port = settings.MACHINE_CONTROLLER_DEFAULT_UDP_PORT
    self._udp_dst = ('', self._udp_port)

    self._tcp_port = settings.MACHINE_CONTROLLER_DEFAULT_TCP_PORT
    self._tcp_dst = ('', self._tcp_port)

    self._udp_socket = None
    self._tcp_socket = None

    self._node_manager = machine.NodeManager(
        machine_id=machine_id,
        machine_name=machine_name,
        mode=mode,
        system_id=system_id,
        ip_host=socket.gethostname(),
        send_to_machine=self._send_to_machine)

  def _send_to_machine(self, message, transport):
    dst = (transport['host'], settings.MACHINE_CONTROLLER_DEFAULT_UDP_PORT)
    dist_zero.transport.send_udp(message, dst)

  def _bind_udp(self):
    logger.info("MachineRunner binding UDP port {}".format(self._udp_port), extra={'port': self._udp_port})
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(self._udp_dst)
    self._udp_socket = sock

  def _bind_and_listen_tcp(self):
    logger.info("MachineRunner binding TCP port {}".format(self._tcp_port), extra={'port': self._tcp_port})
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(self._tcp_dst)
    sock.listen()
    logger.info("MachineRunner listening on TCP port {}".format(self._tcp_port), extra={'port': self._tcp_port})
    self._tcp_socket = sock

  def runloop(self):
    logger.info(
        "Starting run loop for machine {machine_name}: {machine_id}",
        extra={
            'machine_id': self._node_manager.id,
            'machine_name': self._node_manager.name,
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
        logger.error("Exception in run loop: {e_lines}", extra={'e_type': str(e_type), 'e_lines': ''.join(exn_lines)})
        # log the exception and go on.  These exceptions should not stop the run loop.
        continue

    for sock in [self._udp_socket, self._tcp_socket]:
      sock.close()

  def _loop_iteration(self):
    '''Run a single iterator of the run loop, raising any errors that come up.'''

    current_time_s = time.time()
    remaining_ms = MachineRunner.STEP_LENGTH_MS

    # First, elapse the whole time interval on all the nodes.
    self._node_manager.elapse_nodes(remaining_ms)
    after_elapse_s = time.time()
    time_running_nodes_ms = (after_elapse_s - current_time_s) * 1000

    # Then, spend the remaining time waiting on messages from the network
    network_ms = remaining_ms - time_running_nodes_ms
    self._elapse_network(network_ms)

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
        logger.error(
            "Impossible! Unrecognized socket returned by select() {bad_socket}", extra={'bad_socket': str(sock)})

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
    response = self._node_manager.handle_api_message(message)
    binary = bytes(json.dumps(response), messages.ENCODING)
    client_sock.send(binary)
    client_sock.close()

  def _read_udp(self):
    '''Call this method whenever there is a datagram ready to read on the UDP socket'''
    buf, sender_address = self._udp_socket.recvfrom(settings.MSG_BUFSIZE)
    message = json.loads(buf.decode(messages.ENCODING))
    self._node_manager.handle_message(message)

  def configure_logging(self):
    '''
    Configure logging for a `MachineController`
    '''
    # Filters
    str_format_filter = dist_zero.logging.StrFormatFilter()
    context = {
        'env': settings.DIST_ZERO_ENV,
        'mode': self._node_manager.mode,
        'runner': False,
        'machine_id': self._node_manager.id,
        'machine_name': self._node_manager.name,
        'system_id': self._node_manager.system_id,
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
