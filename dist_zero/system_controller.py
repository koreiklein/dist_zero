import logging
import sys
import time

from logstash_async.handler import AsynchronousLogstashHandler

import dist_zero.logging

from dist_zero import machine, settings, errors, messages, spawners, transport


class SystemController(object):
  '''
  Class to manage the entire distributed system for tests.
  It can be used to send messages to various nodes in the system, spawn links and spawn datasets.
  It was originially intended for use in tests.

  This class must be initialized with a `Spawner` instance that determine the mode in which to run.
  '''

  # NOTE(KK): This class is entirely for tests at the moment.
  # The current plan is that in production, these kind of features will
  # be available on authorized MachineController instances.

  def __init__(self, system_id, spawner):
    '''
    :param str system_id: The id to use for this running system.
    :param spawner: The underlying Spawner subclass that spawns new machines.
    :type spawner: `Spawner`
    '''
    self.id = system_id
    self._spawner = spawner

    self._node_id_to_machine_id = {}
    '''For nodes spawned by this instance, map the node id to the id of the machine it was spawned on.'''

  def sleep_ms(self, ms):
    return self._spawner.sleep_ms(ms)

  @property
  def spawner(self):
    '''The underlying spawner of this `SystemController`'''
    return self._spawner

  def route_dns(self, node_id, domain_name):
    '''
    :param str node_id: The id of a root input `DataNode` instance.
    :param str domain_name: The domain name to map.
    '''
    self.send_api_message(node_id, messages.io.route_dns(domain_name=domain_name))

  def get_adjacent(self, node_id):
    adjacent_handle = self.send_api_message(node_id, messages.machine.get_adjacent_handle())
    if adjacent_handle is None:
      return None
    else:
      self._add_node_machine_mapping(adjacent_handle)
      return adjacent_handle['id']

  def get_kids(self, node_id):
    result = self.send_api_message(node_id, messages.machine.get_kids())
    for handle in result.values():
      self._add_node_machine_mapping(handle)

    return list(result.keys())

  def get_senders(self, node_id):
    result = self.send_api_message(node_id, messages.machine.get_senders())
    for handle in result.values():
      self._add_node_machine_mapping(handle)

    return list(result.keys())

  def get_receivers(self, node_id):
    result = self.send_api_message(node_id, messages.machine.get_receivers())
    for handle in result.values():
      self._add_node_machine_mapping(handle)

    return list(result.keys())

  def get_capacity(self, node_id):
    result = self.send_api_message(node_id, messages.machine.get_capacity())
    highest_capacity_kid = result['highest_capacity_kid']
    if highest_capacity_kid is not None:
      self._add_node_machine_mapping(highest_capacity_kid)
      result['highest_capacity_kid'] = highest_capacity_kid['id']

    return result

  def _add_node_machine_mapping(self, handle):
    if handle['id'] not in self._node_id_to_machine_id:
      self._node_id_to_machine_id[handle['id']] = handle['controller_id']

  def create_kid_config(self, data_node_id, new_node_name, machine_id):
    '''
    :param data_node_id: The id of the parent `DataNode`.
    :param str new_node_name: The name to use for the new node.
    :param str machine_id: The id of the machine on which the new node will run.

    :return: A node_config for creating the new kid node.
    :rtype: :ref:`message`
    '''
    return self.send_api_message(
        data_node_id, messages.machine.create_kid_config(
            new_node_name=new_node_name,
            machine_id=machine_id,
        ))

  def create_descendant(self, data_node_id, new_node_name, machine_id, recorded_user=None):
    '''
    Create a new descendant of an `DataNode` in this system.
    The kid will be added at a height 0 `DataNode` with capacity, that is descended from the node
    identified by ``data_node_id``.

    :param str data_node_id: The id of the ancestor node.
    :param str new_node_name: The name to use for the new node.
    :param str machine_id: The id of the `MachineController` that should run the new node.
    :param recorded_user: An optional recording of a user to be played back on the new node.
    :type recorded_user: :RecordedUser`

    :return: The id of the newly created node

    One of the nodes involved will raise a `NoCapacityError` if the subtree identified by ``data_node_id`` has no
      capacity to accomodate new nodes.
    '''
    capacity = self.get_capacity(data_node_id)
    kid_id = capacity['highest_capacity_kid']
    if kid_id is None:
      return self.create_kid(data_node_id, new_node_name, machine_id, recorded_user=recorded_user)
    else:
      return self.create_descendant(kid_id, new_node_name, machine_id, recorded_user=recorded_user)

  def create_kid(self, parent_node_id, new_node_name, machine_id, recorded_user=None):
    '''
    Create a new kid of a height 0 `DataNode` in this system.

    :param str parent_node_id: The id of the parent node.
    :param str new_node_name: The name to use for the new node.
    :param str machine_id: The id of the `MachineController` that should run the new node.
    :param recorded_user: An optional recording of a user to be played back on the new node.
    :type recorded_user: :RecordedUser`

    :return: The id of the newly created node
    '''
    node_config = self.create_kid_config(
        data_node_id=parent_node_id,
        new_node_name=new_node_name,
        machine_id=machine_id,
    )
    if recorded_user is not None:
      node_config['recorded_user_json'] = recorded_user.to_json()
    return self.spawn_node(on_machine=machine_id, node_config=node_config)

  def spawn_node(self, node_config, on_machine):
    '''
    Start a node on a particular machine's container.

    :param node_config: A node config for a new node.
    :type node_config: :ref:`message`
    :param str on_machine: The id of a `MachineController`

    :return: The node id of the spawned node.
    '''
    node_id = node_config['id']
    self._send_to_machine(machine_id=on_machine, message=messages.machine.machine_start_node(node_config))
    self._node_id_to_machine_id[node_id] = on_machine
    return node_id

  def kill_node(self, node_id):
    self.send_api_message(node_id, messages.machine.kill_node())

  def _send_to_machine(self, machine_id, message, sock_type='udp'):
    '''
    Send a message to the identified `MachineController` using whatever method is appropriate
    for the current environment.

    :param str machine_id: The id of the `MachineController` for one of the managed machines.
    :param message: Some json serializable message to send to that machine.
    :type message: :ref:`message`
    :param str sock_type: Either 'udp' or 'tcp'.  Indicating the type of connection.

    :return: None if sock_type == 'udp'.
      If sock_type == 'tcp', then return the response from the `MachineController` tcp API.
    :rtype: object
    '''
    if self._spawner.mode() == spawners.MODE_SIMULATED:
      return self._spawner.simulate_send_to_machine(machine_id=machine_id, message=message, sock_type=sock_type)
    elif self._spawner.mode() == spawners.MODE_VIRTUAL:
      return self._spawner.send_to_container_from_host(machine_id=machine_id, message=message, sock_type=sock_type)
    elif self._spawner.mode() == spawners.MODE_CLOUD:
      return transport.send(
          message=message,
          ip_address=self._spawner.aws_instance_by_id[machine_id].public_ip_address,
          sock_type=sock_type)
    else:
      raise errors.InternalError('Unrecognized mode "{}"'.format(self._spawner.mode()))

  def mode(self):
    return self._spawner.mode()

  async def create_machine(self, machine_config):
    '''
    Start up a new machine and run a `MachineController` instance on it.

    :param object machine_config: A machine configuration object.

    :return: The id of the new `MachineController`
    :rtype: str
    '''
    return await self.create_machines([machine_config])[0]

  async def create_machines(self, machine_configs):
    '''
    Start up new machines and run `MachineController` instances on them.

    :param list machine_configs: A list of machine configuration objects.

    :return: The list of ids of the new `MachineController` in the same order as the matching 
    :rtype: list[str]
    '''
    return await self._spawner.create_machines(machine_configs)

  def get_output_state(self, output_node_id):
    '''
    Get the state associated with an output node.

    :param str output_node: The id of a output node.

    :return: The state of that node at about the current time.
    '''
    return self.send_api_message(output_node_id, messages.machine.get_output_state())

  def get_stats(self, node_id):
    '''
    Get the stats associated with a node that collects stats.

    :param str node_id: The id of a `Node` in the system that is collecting stats.

    :return: The stats of that node at about the current time.
    '''
    return self.send_api_message(node_id, messages.machine.get_stats())

  def send_api_message(self, node_id, message):
    return self._send_to_machine(
        machine_id=self._node_id_to_machine_id[node_id],
        message=messages.machine.api_node_message(node_id=node_id, message=message),
        sock_type='tcp')

  def generate_new_handle(self, new_node_id, existing_node_id):
    '''
    Generate a new handle to fill a config for a new node to send to an existing node.

    :param str new_node_id: The id of a `Node` that has not yet been spawned.
    :param str existing_node_id: The id of an exsiting `Node`

    :return: A :ref:`handle` that the new `Node` (once spawned) can use to send to the existing `Node`.
    :rtype: :ref:`handle`
    '''
    return self.send_api_message(existing_node_id, messages.machine.new_handle(new_node_id=new_node_id))

  def send_to_node(self, node_id, message):
    '''
    Send a message to a node.

    :param str node_id: The id of some node.

    :param message: A message for that node.
    :type message: :ref:`message`
    '''
    machine_id = self._node_id_to_machine_id[node_id]
    machine_message = messages.machine.machine_deliver_to_node(node_id=node_id, message=message, sending_node_id=None)
    self._send_to_machine(machine_id=machine_id, message=machine_message)

  def _node_handle_to_machine_id(self, node_handle):
    return self._node_id_to_machine_id[node_handle['id']]

  def get_simulated_spawner(self):
    if self._spawner.mode() != spawners.MODE_SIMULATED:
      raise errors.InternalError("System is not using a simulated spawner")
    return self._spawner

  def configure_logging(self):
    # Filters
    str_format_filter = dist_zero.logging.StrFormatFilter()
    context = {
        'env': settings.DIST_ZERO_ENV,
        'mode': self._spawner.mode(),
        'runner': True,
        'system_id': self.id,
    }
    if settings.LOGZ_IO_TOKEN:
      context['token'] = settings.LOGZ_IO_TOKEN
    context_filter = dist_zero.logging.ContextFilter(context, self._spawner)

    # Formatters
    human_formatter = dist_zero.logging.HUMAN_FORMATTER
    json_formatter = dist_zero.logging.JsonFormatter('(asctime) (levelname) (name) (message)')

    # Handlers
    stdout_handler = logging.StreamHandler(sys.stdout)
    human_file_handler = logging.FileHandler('./.tmp/system.log')
    json_file_handler = logging.FileHandler('./.tmp/system.json.log')
    logstash_handler = AsynchronousLogstashHandler(
        settings.LOGSTASH_HOST,
        settings.LOGSTASH_PORT,
        database_path='./.tmp/logstash.db',
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

    main_handlers = [
        json_file_handler,
        human_file_handler,
        stdout_handler,
    ]
    if settings.LOGSTASH_HOST:
      main_handlers.append(logstash_handler)

    # Loggers
    for noisy_logger_name in ['botocore', 'boto3', 'paramiko.transport']:
      noisy_logger = logging.getLogger(noisy_logger_name)
      noisy_logger.propagate = False
      noisy_logger.setLevel(max(settings.MIN_LOG_LEVEL, logging.INFO))
      dist_zero.logging.set_handlers(noisy_logger, main_handlers)

    dist_zero_logger = logging.getLogger('dist_zero')
    root_logger = logging.getLogger()
    root_logger.setLevel(max(settings.MIN_LOG_LEVEL, logging.DEBUG))

    dist_zero.logging.set_handlers(root_logger, main_handlers)
