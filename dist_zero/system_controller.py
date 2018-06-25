import logging
import sys
import time

from logstash_async.handler import AsynchronousLogstashHandler

import dist_zero.logging

from dist_zero import machine, settings, errors, messages


class SystemController(object):
  '''
  Class to manage the entire distributed system for tests.
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

  @property
  def spawner(self):
    '''The underlying spawner of this `SystemController`'''
    return self._spawner

  def create_kid_config(self, internal_node_id, new_node_name, machine_controller_handle):
    '''
    :param internal_node_id: The id of the parent `InternalNode`.
    :param str new_node_name: The name to use for the new node.
    :param machine_controller_handle: The :ref:`handle` of the machine on which the new node will run.
    :type machine_controller_handle: :ref:`handle`

    :return: A node_config for creating the new kid node.
    :rtype: :ref:`message`
    '''
    machine_id = self._node_id_to_machine_id[internal_node_id]
    return self._spawner.send_to_machine(
        machine_id=machine_id,
        message=messages.machine.api_create_kid_config(
            internal_node_id=internal_node_id,
            new_node_name=new_node_name,
            machine_controller_handle=machine_controller_handle,
        ),
        sock_type='tcp')

  def create_kid(self, parent_node_id, new_node_name, machine_controller_handle, recorded_user=None):
    '''
    Create a new kid of an `InternalNode` in this system.

    :param str parent_node_id: The id of the parent node.
    :param str new_node_name: The name to use for the new node.
    :param machine_controller_handle: The :ref:`handle` of the `MachineController` that should run the new node.
    :type machine_controller_handle: :ref:`handle`
    :param recorded_user: An optional recording of a user to be played back on the new node.
    :type recorded_user: :RecordedUser`

    :return: The id of the newly created node
    '''
    node_config = self.create_kid_config(
        internal_node_id=parent_node_id,
        new_node_name=new_node_name,
        machine_controller_handle=machine_controller_handle,
    )
    if recorded_user is not None:
      node_config['recorded_user_json'] = recorded_user.to_json()
    return self.spawn_node(on_machine=machine_controller_handle, node_config=node_config)

  def spawn_node(self, node_config, on_machine):
    '''
    Start a node on a particular machine's container.

    :param node_config: A node config for a new node.
    :type node_config: :ref:`message`
    :param str on_machine: The id of a `MachineController`

    :return: The node id of the spawned node.
    '''
    node_id = node_config['id']
    self._spawner.send_to_machine(machine_id=on_machine, message=messages.machine.machine_start_node(node_config))
    self._node_id_to_machine_id[node_id] = on_machine
    return node_id

  def create_machine(self, machine_config):
    '''
    Start up a new machine and run a `MachineController` instance on it.

    :param object machine_config: A machine configuration object.

    :return: The id of the new `MachineController`
    :rtype: str
    '''
    return self.create_machines([machine_config])[0]

  def create_machines(self, machine_configs):
    '''
    Start up new machines and run `MachineController` instances on them.

    :param list machine_configs: A list of machine configuration objects.

    :return: The list of ids of the new `MachineController` in the same order as the matching 
    :rtype: list[str]
    '''
    return self._spawner.create_machines(machine_configs)

  def get_output_state(self, output_node_id):
    '''
    Get the state associated with an output node.

    :param str output_node: The id of a output node.

    :return: The state of that node at about the current time.
    '''
    machine_id = self._node_id_to_machine_id[output_node_id]
    return self._spawner.send_to_machine(
        machine_id=machine_id, message=messages.machine.api_get_output_state(node_id=output_node_id), sock_type='tcp')

  def get_stats(self, node_id):
    '''
    Get the stats associated with a node that collects stats.

    :param str node_id: The id of a `Node` in the system that is collecting stats.

    :return: The stats of that node at about the current time.
    '''
    machine_id = self._node_id_to_machine_id[node_id]
    return self._spawner.send_to_machine(
        machine_id=machine_id, message=messages.machine.api_get_stats(node_id=node_id), sock_type='tcp')

  def get_adjacent_id(self, node_id):
    '''
    Get the id of an adjacent `Node`.

    :param str node_id: The id of a `Node` in the system that has an adjacent `Node`.
      It must be either a `LeafNode` or an `InternalNode`

    :return: The id of the adjacent `Node`, or `None` if no such `Node` exists.
    '''
    machine_id = self._node_id_to_machine_id[node_id]
    result = self._spawner.send_to_machine(
        machine_id=machine_id, message=messages.machine.api_get_adjacent(node_id=node_id), sock_type='tcp')
    if result is None:
      return None
    else:
      self._node_id_to_machine_id[result['id']] = result['controller_id']
      return result['id']

  def generate_new_handle(self, new_node_id, existing_node_id):
    '''
    Generate a new handle to fill a config for a new node to send to an existing node.

    :param str new_node_id: The id of a `Node` that has not yet been spawned.
    :param str existing_node_id: The id of an exsiting `Node`

    :return: A :ref:`handle` that the new `Node` (once spawned) can use to send to the existing `Node`.
    :rtype: :ref:`handle`
    '''
    machine_id = self._node_id_to_machine_id[existing_node_id]
    return self._spawner.send_to_machine(
        machine_id=machine_id,
        message=messages.machine.api_new_handle(local_node_id=existing_node_id, new_node_id=new_node_id),
        sock_type='tcp')

  def send_to_node(self, node_id, message):
    '''
    Send a message to a node.

    :param str node_id: The id of some node.

    :param message: A message for that node.
    :type message: :ref:`message`
    '''
    machine_id = self._node_id_to_machine_id[node_id]
    machine_message = messages.machine.machine_deliver_to_node(node_id=node_id, message=message, sending_node_id=None)
    self._spawner.send_to_machine(machine_id=machine_id, message=machine_message)

  def _node_handle_to_machine_id(self, node_handle):
    return self._node_id_to_machine_id[node_handle['id']]

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
    context_filter = dist_zero.logging.ContextFilter(context)

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
