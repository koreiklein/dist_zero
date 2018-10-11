import asyncio
import json
import logging
import os
import socket
import sys
import time
import traceback

from logstash_async.handler import AsynchronousLogstashHandler

import dist_zero.transport
import dist_zero.logging

import dist_zero.spawners.parse
import dist_zero.load_balancer
from dist_zero import settings, machine, messages, web_servers, errors
from dist_zero.spawners import docker

logger = logging.getLogger(__name__)


class MachineRunner(object):
  '''
  For running A `NodeManager` on a machine inside a runloop.
  Real time is passed in, and messages are read from os sockets.
  '''

  STEP_LENGTH_MS = 5 # Target number of milliseconds per iteration of the run loop.

  def __init__(self, machine_config):

    self._udp_port = settings.MACHINE_CONTROLLER_DEFAULT_UDP_PORT
    self._udp_dst = ('0.0.0.0', self._udp_port)

    self._tcp_port = settings.MACHINE_CONTROLLER_DEFAULT_TCP_PORT
    self._tcp_dst = ('', self._tcp_port)

    start, stop = self._parse_port_range()

    self._available_server_ports = set(range(start, stop))
    # When we create and listen on new sockets, this dict will always map the socket
    # object to the associated web server.
    self._server_by_socket = {}

    self._udp_socket = None
    self._tcp_socket = None

    self._ip_host = machine_config['ip_address']
    if self._ip_host is None:
      logger.warning("An ip_address was not provided in the machine config.")
      self._ip_host = socket.gethostbyname(socket.gethostname())

    self._load_balancer = None

    self.node_manager = machine.NodeManager(
        machine_config=machine_config,
        spawner=dist_zero.spawners.parse.from_config(machine_config['spawner']),
        ip_host=self._ip_host,
        send_to_machine=self._send_to_machine,
        machine_runner=self)
    '''The `NodeManager` underlying this `MachineRunner`'''

  def _parse_port_range(self):
    port_range = settings.MACHINE_CONTROLLER_ROUTING_PORT_RANGE
    if isinstance(port_range, str):
      start_s, stop_s = port_range[1:-1].split(', ')
      return int(start_s), int(stop_s)
    else:
      return port_range

  def _new_server_port(self):
    if self._available_server_ports:
      return self._available_server_ports.pop()
    else:
      raise errors.InternalError("Ran out of available server ports.")

  def _new_address(self, domain):
    port = self._new_server_port()
    return messages.machine.server_address(domain=domain, ip=self._ip_host, port=port)

  def new_http_server(self, domain, f):
    address = self._new_address(domain)
    server = web_servers.HttpServer(address=address, on_request=f)
    self._server_by_socket[server.socket()] = server
    return server

  def new_socket_server(self, f):
    # FIXME(KK): Implement this!
    raise RuntimeError("Not Yet Implemented")

  def _create_load_balancer(self):
    load_balancer = dist_zero.load_balancer.LoadBalancer()
    return load_balancer

  def _get_load_balancer(self):
    if self._load_balancer is None:
      self._load_balancer = self._create_load_balancer()

    return self._load_balancer

  def new_load_balancer_frontend(self, domain_name, height):
    load_balancer = self._get_load_balancer()
    server_address = self._new_address(domain_name)
    return load_balancer.new_frontend(server_address=server_address, height=height)

  def _send_to_machine(self, message, transport):
    dst = (transport['host'], settings.MACHINE_CONTROLLER_DEFAULT_UDP_PORT)
    dist_zero.transport.send_udp(message, dst)

  async def _bind_udp(self):
    logger.info("MachineRunner binding UDP port {}".format(self._udp_port), extra={'port': self._udp_port})

    runner = self

    class handler(asyncio.DatagramProtocol):
      def datagram_received(self, data, addr):
        message = json.loads(data.decode(messages.ENCODING))
        runner.node_manager.handle_message(message)

      def error_received(self, exc):
        logger.error(f"UDP error: {exc}")

    result = await asyncio.get_event_loop().create_datagram_endpoint(
        handler,
        local_addr=self._udp_dst,
        # Deal with TIME_WAIT issues by reusing the address.
        # WARNING(KK): This risks that if a machine_runner is run twice, datagrams destined for the first
        #   instance could be accidentally received by the second instance.
        #   It is possible that appropriate cryptography could protect against that problem.
        reuse_address=True,
    )

    logger.info("MachineRunner listening on UDP port {port}", extra={'port': self._udp_port})

    return result

  async def _bind_and_listen_tcp(self):
    logger.info("MachineRunner binding TCP port {}".format(self._tcp_port), extra={'port': self._tcp_port})

    async def handler(reader, writer):
      buf = await reader.read(settings.MSG_BUFSIZE)
      message = json.loads(buf.decode(messages.ENCODING))
      response = self.node_manager.handle_api_message(message)
      binary = bytes(json.dumps(response), messages.ENCODING)
      writer.write(binary)
      await writer.drain()
      writer.close()

    result = await asyncio.start_server(
        handler,
        host='',
        port=self._tcp_port,
        # Deal with TIME_WAIT issues by reusing the address.
        # WARNING(KK): This risks that if a machine_runner is run twice, datagrams destined for the first
        #   instance could be accidentally received by the second instance.
        #   It is possible that appropriate cryptography could protect against that problem.
        reuse_address=True,
    )
    logger.info("MachineRunner listening on TCP port {port}", extra={'port': self._tcp_port})
    return result

  def runloop(self):
    '''
    Enter an asyncio runloop for the contained `NodeManager`.
    '''
    logger.info(
        "Starting run loop for machine {machine_name}: {machine_id}",
        extra={
            'machine_id': self.node_manager.id,
            'machine_name': self.node_manager.name,
        })

    asyncio.get_event_loop().create_task(self._bind_udp())
    asyncio.get_event_loop().create_task(self._bind_and_listen_tcp())

    asyncio.get_event_loop().run_forever()

  def configure_logging(self):
    '''
    Configure logging for a `MachineController`
    '''
    # Filters
    str_format_filter = dist_zero.logging.StrFormatFilter()
    context = {
        'env': settings.DIST_ZERO_ENV,
        'mode': self.node_manager.mode,
        'runner': False,
        'machine_id': self.node_manager.id,
        'machine_name': self.node_manager.name,
        'system_id': self.node_manager.system_id,
    }
    if settings.LOGZ_IO_TOKEN:
      context['token'] = settings.LOGZ_IO_TOKEN
    context_filter = dist_zero.logging.ContextFilter(context)

    # Formatters
    human_formatter = dist_zero.logging.HUMAN_FORMATTER
    json_formatter = dist_zero.logging.JsonFormatter('(asctime) (levelname) (name) (message)')

    # Handlers
    stderr_handler = logging.StreamHandler(sys.stderr)
    human_file_handler = logging.FileHandler(os.path.join(docker.DockerSpawner.CONTAINER_LOGS_DIR, 'output.log'))
    json_file_handler = logging.FileHandler(os.path.join(docker.DockerSpawner.CONTAINER_LOGS_DIR, 'output.json.log'))
    logstash_handler = AsynchronousLogstashHandler(
        settings.LOGSTASH_HOST,
        settings.LOGSTASH_PORT,
        database_path='/logs/.logstash.db',
    )

    stderr_handler.setLevel(logging.ERROR)
    human_file_handler.setLevel(logging.DEBUG)
    json_file_handler.setLevel(logging.DEBUG)
    logstash_handler.setLevel(logging.DEBUG)

    stderr_handler.setFormatter(human_formatter)
    human_file_handler.setFormatter(human_formatter)
    json_file_handler.setFormatter(json_formatter)
    logstash_handler.setFormatter(json_formatter)

    stderr_handler.addFilter(str_format_filter)
    human_file_handler.addFilter(str_format_filter)
    json_file_handler.addFilter(str_format_filter)
    json_file_handler.addFilter(context_filter)
    logstash_handler.addFilter(str_format_filter)
    logstash_handler.addFilter(context_filter)

    # Loggers
    dist_zero_logger = logging.getLogger('dist_zero')
    root_logger = logging.getLogger()
    root_logger.setLevel(max(settings.MIN_LOG_LEVEL, logging.DEBUG))

    main_handlers = [
        json_file_handler,
        human_file_handler,
        stderr_handler,
    ]

    if settings.LOGSTASH_HOST:
      main_handlers.append(logstash_handler)

    dist_zero.logging.set_handlers(root_logger, main_handlers)
