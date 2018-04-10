import json
import logging
import os
import uuid

import docker
import docker.errors

from dist_zero import machine, settings, errors, messages

logger = logging.getLogger(__name__)


class DockerSimulatedHardware(object):
  '''
  A class for simulating new hardware by spinning up docker containers on a host and
  running a 'production' `OsMachineController` on each.

  Tests should typically create a single instance of this factory, and use it to generate
  all their `machine.MachineController` instances.  User input in such tests
  will tend to come from `RecordedUser` instances running in each container.
  '''

  DOCKERFILE = 'dist_zero/runners/docker/Dockerfile'
  CONTAINER_WORKING_DIR = '/machine' # Must match the working directory in DOCKERFILE

  CONTAINER_MESSAGE_DIR = '/messages'
  CONTAINER_LOGS_DIR = '/logs'

  LABEL_DOCKER_SIMULATED_HARDWARE = 'DockerSimulatedHarware'
  LABEL_TRUE = 'true'
  LABEL_INSTANCE = '{}_instance'.format(LABEL_DOCKER_SIMULATED_HARDWARE)
  LABEL_CONTAINER_UUID = '{}_container_uuid'.format(LABEL_DOCKER_SIMULATED_HARDWARE)

  CONTAINER_STATUS_RUNNING = 'running'

  def __init__(self):
    self._started = False
    self._docker_client = None
    self.id = str(uuid.uuid4())

    self._dir = os.path.dirname(os.path.realpath(__file__))
    self._root_dir = os.path.realpath(os.path.join(self._dir, '../../..'))

    self._network = None

    self._image = None
    self._build_logs = None
    self._handle_by_id = {}
    self._container_by_id = {}
    self._node_id_to_machine_handle = {}
    '''For nodes spawned by this instance, map the node id to the handle of the machine it was spawned on.'''

    self._all_containers_msg_dir = os.path.join(self._root_dir, '.tmp', 'containers')
    '''Directory in which all the container msg directories are stored.'''

    self._n_sent_messages = 0
    '''
    The total number of messages sent to all `MachineController`s.
    '''

  def virtual_get_state(self, output_node):
    '''
    Get the state associated with an output node.

    :param output_node: The :ref:`handle` of a virtual output node managed as part of this virtual hardware.
    :type output_node: :ref:`handle`

    :return: The state of that node at about the current time.
    '''
    machine_handle = self._node_id_to_machine_handle[output_node['id']]
    return self._virtual_send_machine(
        machine=machine_handle, message=messages.api_get_output_state(node=output_node), sock_type='tcp')

  def create_kid_config(self, internal_node, new_node_name, machine_controller_handle):
    '''
    :param internal_node: The :ref:`handle` of the parent internalnode.
    :type internal_node: :ref:`handle`
    :param str new_node_name: The name to use for the new node.
    :param machine_controller_handle: The :ref:`handle` of the machine on which the new node will run.
    :type machine_controller_handle: :ref:`handle`

    :return: A node_config for creating the new kid node.
    :rtype: :ref:`message`
    '''
    machine_handle = self._node_id_to_machine_handle[internal_node['id']]
    return self._virtual_send_machine(
        machine=machine_handle,
        message=messages.api_create_kid_config(
            internal_node=internal_node,
            new_node_name=new_node_name,
            machine_controller_handle=machine_controller_handle,
        ),
        sock_type='tcp')

  def virtual_new_transport_for(self, sender, receiver):
    '''
    Get and return a transport instance allowing sender to send to receiver.

    :param sender: The :ref:`handle` of a sending node.
    :type sender: :ref:`handle`
    :param receiver: The :ref:`handle` of a sending node.
    :type receiver: :ref:`handle`

    :return: A :ref:`transport` authorizing sender to send to receiver.
    :rtype: :ref:`transport`
    '''
    # Must get the transport from the intended receiver.
    return self._virtual_send_machine(
        machine=self._node_id_to_machine_handle[receiver['id']],
        sock_type='tcp',
        message=messages.api_new_transport(sender, receiver))

  def create_kid(self, parent_node, new_node_name, machine_controller_handle, recorded_user=None):
    node_config = self.create_kid_config(
        internal_node=parent_node,
        new_node_name=new_node_name,
        machine_controller_handle=machine_controller_handle,
    )
    if recorded_user is not None:
      node_config['recorded_user_json'] = recorded_user.to_json()
    return self.virtual_spawn_node(on_machine=machine_controller_handle, node_config=node_config)

  def virtual_spawn_node(self, node_config, on_machine):
    '''
    Start a node on a particular machine's container.

    :param node_config: A node config for a new node.
    :type node_config: :ref:`message`
    :param on_machine: The handle for a `MachineController`
    :type on_machine: :ref:`handle`

    :return: The node :ref:`handle` of the spawned node.
    '''
    node_id = node_config['id']
    self._virtual_send_machine(machine=on_machine, message=messages.machine_start_node(node_config))
    self._node_id_to_machine_handle[node_id] = on_machine
    return {'type': node_config['type'], 'id': node_id, 'controller_id': on_machine['id']}

  def _node_handle_to_machine_handle(self, node_handle):
    return self._node_id_to_machine_handle[node_handle['id']]

  def _virtual_send_machine(self, machine, message, sock_type='udp'):
    '''
    Send a message from the process running the DockerSimulatedHardware instance to `MachineController' listening
    on a port on a container.

    :param machine: The :ref:`handle` of the `MachineController` for one of the managed containers.
    :type machine: :ref:`handle`
    :param message: Some json serializable message to send to that machine.
    :type message: :ref:`message`
    :param str sock_type: Either 'udp' or 'tcp'.  Indicating the type of connection.

    :return: None if sock_type == 'udp'. If sock_type == 'tcp', then return the response from the API.
    :rtype: object
    '''
    host_msg_dir = self._container_msg_dir_on_host(machine['id'])
    filename = "message_{}.json".format(self._n_sent_messages)
    self._n_sent_messages += 1

    logger.info("attempting to exec send_local_msg_from_file on docker container")

    with open(os.path.join(host_msg_dir, filename), 'w') as f:
      json.dump(message, f)

    exit_code, output = self._container_by_id[machine['id']].exec_run([
        'python',
        '-m',
        'dist_zero.runners.docker.send_local_msg_from_file',
        filename,
        sock_type,
    ])

    if exit_code != 0:
      msg = "docker exec of send_local_msg_from_file on container failed with code {}. output: {}".format(
          exit_code, output)
      logger.error(msg, extra={'exit_code': exit_code, 'output': output})
      raise errors.InternalError(msg)
    else:
      logger.info("send_local_msg_from_file successfully exec'd on docker container")

    if sock_type == 'tcp':
      msg = json.loads(output)
      if msg['status'] == 'ok':
        return msg['data']
      else:
        raise errors.InternalError("Failed to communicate over TCP api to MachineController. reason: {}".format(
            msg.get('reason', '')))
    else:
      return None

  def virtual_send(self, node_handle, message, sending_node_handle=None):
    '''
    Send a message to a node.

    :param node_handle: The handle of some node.
    :type node_handle: :ref:`handle`

    :param message: A message for that node.
    :type message: :ref:`message`

    :param sending_node_handle: The :ref:`handle` of the sending node, or None if no node sent the message.
    :type sending_node_handle: :ref:`handle`
    '''
    machine_handle = self._node_handle_to_machine_handle(node_handle)
    machine_message = messages.machine_deliver_to_node(
        node=node_handle, message=message, sending_node=sending_node_handle)
    self._virtual_send_machine(machine=machine_handle, message=machine_message)

  def _clean_msg_directories(self):
    '''
    Completely remove all the container message directories and their parent directory on the host.
    '''
    for root, dirs, files in os.walk(self._all_containers_msg_dir, topdown=False):
      for name in files:
        os.remove(os.path.join(root, name))
      for name in dirs:
        os.rmdir(os.path.join(root, name))

  def start(self):
    if self._started:
      raise errors.InternalError("DockerSimulatedHardware has already been started.")

    self._clean_msg_directories()
    self._docker_client = docker.DockerClient(base_url=settings.DOCKER_BASE_URL)
    self._network = self._docker_client.networks.create('virtual_hardware_default_network', driver='bridge')
    self._started = True

  @property
  def _docker(self):
    if not self._started:
      raise errors.InternalError("DockerSimulatedHardware has not yet been started")

    return self._docker_client

  @property
  def image(self):
    if self._image is None:
      logger.info('building docker image with context %s', self._root_dir)
      image, build_logs = self._docker.images.build(
          path=self._root_dir,
          dockerfile=DockerSimulatedHardware.DOCKERFILE,
          rm=True, # Remove intermediate containers
          labels={
              DockerSimulatedHardware.LABEL_DOCKER_SIMULATED_HARDWARE: DockerSimulatedHardware.LABEL_TRUE,
              DockerSimulatedHardware.LABEL_INSTANCE: self.id,
          },
      )
      self._image = image
      self._build_logs = build_logs

    return self._image

  @property
  def started(self):
    '''True iff this simulation has started running'''
    return self._started

  def _container_msg_dir_on_host(self, machine_controller_id):
    '''
    Directory for copying files to `CONTAINER_MESSAGE_DIR` in the container for a given machine_controller_id
    '''
    return os.path.join(self._all_containers_msg_dir, machine_controller_id)

  def new_container(self, machine_name):
    '''
    Start up a new docker container and run a `MachineController` instance on it.

    :param str machine_name: A user-friendly name to use for the new machine.

    :return: The :ref:`handle` of the new `MachineController`
    :rtype: :ref:`handle`
    '''
    machine_controller_id = str(uuid.uuid4())

    host_msg_dir = self._container_msg_dir_on_host(machine_controller_id)
    log_dir = os.path.join(host_msg_dir, 'logs')

    os.makedirs(host_msg_dir)
    os.makedirs(log_dir)

    container = self._docker.containers.run(
        image=self.image,
        command=[
            'python',
            '-m',
            'dist_zero.machine_init',
            machine_controller_id,
            machine_name,
        ],
        detach=True,
        labels={
            DockerSimulatedHardware.LABEL_DOCKER_SIMULATED_HARDWARE: DockerSimulatedHardware.LABEL_TRUE,
            DockerSimulatedHardware.LABEL_INSTANCE: self.id,
            DockerSimulatedHardware.LABEL_CONTAINER_UUID: machine_controller_id,
        },
        auto_remove=False,
        volumes={
            self._root_dir: {
                'bind': DockerSimulatedHardware.CONTAINER_WORKING_DIR,
                'mode': 'ro',
            },
            host_msg_dir: {
                'bind': DockerSimulatedHardware.CONTAINER_MESSAGE_DIR,
                'mode': 'ro',
            },
            log_dir: {
                'bind': DockerSimulatedHardware.CONTAINER_LOGS_DIR,
                'mode': 'rw',
            },
        },
    )
    self._network.connect(container)

    handle = messages.os_machine_controller_handle(machine_controller_id)
    self._handle_by_id[machine_controller_id] = handle
    self._container_by_id[machine_controller_id] = container
    return handle

  def _get_containers_from_docker(self):
    '''
    Get all containers associated with this instance from the docker daemon.

    :return: The list of all docker container objects associated with
      this particular instance of `DockerSimulatedHardware`
    '''
    labels_query = "{}={}".format(DockerSimulatedHardware.LABEL_INSTANCE, self.id)
    return self._docker.containers.list(all=True, filters={'label': labels_query})

  def get_running_containers(self):
    '''
    Get the list of this `DockerSimulatedHardware` instance's running containers from the docker daemon.

    :return: A list of container handles for all running containers spawned by this `DockerSimulatedHardware`
    '''
    return [
        self._handle_by_id[container.labels[DockerSimulatedHardware.LABEL_CONTAINER_UUID]]
        for container in self._get_containers_from_docker()
        if container.status == DockerSimulatedHardware.CONTAINER_STATUS_RUNNING
    ]

  def get_stopped_containers(self):
    '''
    Get the list of this `DockerSimulatedHardware` instance's non-running containers from the docker daemon.

    :return: A list of container handles for all running containers spawned by this `DockerSimulatedHardware`
    '''
    return [
        self._handle_by_id[container.labels[DockerSimulatedHardware.LABEL_CONTAINER_UUID]]
        for container in self._get_containers_from_docker()
        if container.status == DockerSimulatedHardware.CONTAINER_STATUS_RUNNING
    ]

  def all_spawned_containers(self):
    '''
    :return: The list of all container handles for containers this `DockerSimulatedHardware` has ever spawned.
    '''
    return list(self._handle_by_id.values())

  def clean_all(self):
    '''
    Remove all the docker resources associated with any instance of `DockerSimulatedHardware`
    (not just the current instance).
    '''
    labels_query = "{}={}".format(DockerSimulatedHardware.LABEL_DOCKER_SIMULATED_HARDWARE,
                                  DockerSimulatedHardware.LABEL_TRUE)

    containers = self._docker.containers.list(all=True, filters={'label': labels_query})
    logger.debug("Removing containers {}".format(containers), extra={'n_containers_to_remove': len(containers)})
    for container in containers:
      if container.status == DockerSimulatedHardware.CONTAINER_STATUS_RUNNING:
        try:
          container.kill()
        except docker.errors.APIError as err:
          logger.warning("Failed to kill a container. It may have died on its own")
      container.remove()
    if self._network is not None:
      self._network.remove()
