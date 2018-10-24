import asyncio
import json
import logging
import os

import docker
import docker.errors

from dist_zero import machine, settings, errors, messages, spawners, transport
from .. import spawner

logger = logging.getLogger(__name__)


class DockerSpawner(spawner.Spawner):
  '''
  A class for simulating new hardware by spinning up docker containers on a host and
  running a 'production' `MachineRunner` on each.
  '''

  DOCKERFILE = 'dist_zero/spawners/docker/Dockerfile'
  CONTAINER_WORKING_DIR = '/machine' # Must match the working directory in DOCKERFILE

  CONTAINER_LOGS_DIR = '/logs'
  CONTAINER_MESSAGE_DIR = '/messages'

  CONTAINER_MESSAGE_RESPONSE_TEMPLATE = '{}.response.json'

  LABEL_DOCKER_SIMULATED_HARDWARE = 'DockerSimulatedHardware'
  LABEL_TRUE = 'true'
  LABEL_INSTANCE = '{}_instance'.format(LABEL_DOCKER_SIMULATED_HARDWARE)
  LABEL_CONTAINER_UUID = '{}_container_uuid'.format(LABEL_DOCKER_SIMULATED_HARDWARE)

  CONTAINER_STATUS_RUNNING = 'running'

  STD_DOCKER_IMAGE_TAG = 'dist_zero_std_docker_image'

  def __init__(self, system_id, inside_container):
    '''
    :param str system_id: The id of the overall distributed system.
    :param bool inside_container: True iff this spawner is running from within one of the docker containers.
      False iff running on the host.
    '''
    self._started = False
    self._docker_client = None
    self._system_id = system_id
    self._inside_container = inside_container

    self._host_tcp_port = {} # machine_controller_id to the bound tcp port on the host
    self._host_udp_port = {} # machine_controller_id to the bound udp port on the host
    self._current_port = 12334 # Try not to interfere with other processes that might bind ports on this host

    self._dir = os.path.dirname(os.path.realpath(__file__))
    self._root_dir = os.path.realpath(os.path.join(self._dir, '../../..'))

    self._network = None

    self._image = None
    self._image_tag = DockerSpawner.STD_DOCKER_IMAGE_TAG
    self._build_logs = None
    self._container_by_id = {}

    self._all_containers_msg_dir = os.path.join(self._root_dir, '.tmp', 'containers')
    '''Directory in which all the container msg directories are stored.'''

    self._n_sent_messages = 0
    '''
    The total number of messages sent to all `MachineController`s.
    '''

  def _remote_spawner_json(self):
    '''Generate a `DockerSpawner` config for a new container.'''
    return {
        'system_id': self._system_id,
        'inside_container': True,
    }

  @staticmethod
  def from_spawner_json(spawner_config):
    logger.info("Creating {parsed_spawner_type} from spawner_config", extra={'parsed_spawner_type': 'DockerSpawner'})
    return DockerSpawner(system_id=spawner_config['system_id'], inside_container=spawner_config['inside_container'])

  def mode(self):
    return spawners.MODE_VIRTUAL

  def sleep_ms(self, ms):
    return asyncio.sleep(ms / 1000)

  def send_to_container_from_host(self, machine_id, message, sock_type='udp'):
    '''
    Simulate a virtual send of a message to the container running the identified `MachineController`

    :param str machine_id: The id of the `MachineController` for one of the managed machines.
    :param message: Some json serializable message to send to that machine.
    :type message: :ref:`message`
    :param str sock_type: Either 'udp' or 'tcp'.  Indicating the type of connection.

    :return: None if sock_type == 'udp'.
      If sock_type == 'tcp', then return the response from the `MachineController` tcp API.
    :rtype: object
    '''
    return self._send_to_container_from_host_by_writing_message_to_volume(
        machine_id=machine_id, message=message, sock_type=sock_type)
    # self._send_to_container_from_host_on_the_mapped_host_port(machine_id=machine_id, message=message, sock_type=sock_type)

  def _send_to_container_from_host_on_the_mapped_host_port(self, machine_id, message, sock_type='udp'):
    if sock_type == 'udp':
      dst = ('host.docker.internal', self._host_udp_port[machine_id])
      return transport.send_udp(message, dst)
    elif sock_type == 'tcp':
      dst = ('host.docker.internal', self._host_tcp_port[machine_id])
      return transport.send_tcp(message, dst)
    else:
      raise errors.InternalError("Unrecognized sock_type {}".format(sock_type))

  def _send_to_container_from_host_by_writing_message_to_volume(self, machine_id, message, sock_type='udp'):
    host_msg_dir = self._container_msg_dir_on_host(machine_id)
    filename = "message_{}.json".format(self._n_sent_messages)
    self._n_sent_messages += 1

    logger.info("attempting to exec send_local_msg_from_file on docker container")

    full_path = os.path.join(host_msg_dir, filename)
    with open(full_path, 'w') as f:
      json.dump(message, f)

    exit_code, output = self._container_by_id[machine_id].exec_run([
        'pipenv',
        'run',
        'python',
        '-m',
        'dist_zero.spawners.docker.send_local_msg_from_file',
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
      with open(DockerSpawner.CONTAINER_MESSAGE_RESPONSE_TEMPLATE.format(full_path), 'r') as f_out:
        return json.load(f_out)
    else:
      return None

  def _clean_msg_directories(self):
    '''
    Completely remove all the container message directories and their parent directory on the host.
    '''
    for root, dirs, files in os.walk(self._all_containers_msg_dir, topdown=False):
      for name in files:
        os.remove(os.path.join(root, name))
      for name in dirs:
        os.rmdir(os.path.join(root, name))

    os.rmdir(self._all_containers_msg_dir)

  def start(self):
    if self._started:
      raise errors.InternalError("DockerSpawner has already been started.")

    self._clean_msg_directories()
    self._docker_client = docker.DockerClient(base_url=settings.DOCKER_BASE_URL)
    self._network = self._docker_client.networks.create('virtual_hardware_default_network', driver='bridge')
    self._started = True

  @property
  def _docker(self):
    if not self._started:
      raise errors.InternalError("DockerSpawner has not yet been started")

    return self._docker_client

  def _build_image(self):
    logger.info('building docker image with context {docker_root_dir}', extra={'docker_root_dir': self._root_dir})
    image, build_logs = self._docker.images.build(
        path=self._root_dir,
        tag=self._image_tag,
        dockerfile=DockerSpawner.DOCKERFILE,
        rm=True, # Remove intermediate containers
        labels={
            DockerSpawner.LABEL_DOCKER_SIMULATED_HARDWARE: DockerSpawner.LABEL_TRUE,
            DockerSpawner.LABEL_INSTANCE: self._system_id,
        },
    )
    self._image = image
    self._build_logs = build_logs

  @property
  def image(self):
    if self._image is None:
      if settings.ALWAYS_REBUILD_DOCKER_IMAGES:
        self._build_image()
      else:
        try:
          self._image = self._docker.images.get(self._image_tag)
          logger.warning(
              "Reusing existing docker image '{docker_image_tag}' without performing a new build",
              extra={'docker_image_tag': self._image_tag})
        except docker.errors.ImageNotFound:
          self._build_image()

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

  async def create_machines(self, machine_configs):
    return [ await self.create_machine(machine_config) for machine_config in machine_configs]

  def _new_port(self):
    self._current_port += 1
    return self._current_port

  async def create_machine(self, machine_config):
    machine_name = machine_config['machine_name']
    machine_controller_id = machine_config['id']

    host_msg_dir = self._container_msg_dir_on_host(machine_controller_id)
    log_dir = os.path.join(host_msg_dir, 'logs')

    os.makedirs(host_msg_dir)
    os.makedirs(log_dir)

    machine_config_with_spawner = dict(machine_config)
    machine_config_with_spawner.update({'spawner': {'type': 'docker', 'value': self._remote_spawner_json()}})

    config_filename = 'machine_config.json'
    with open(os.path.join(host_msg_dir, config_filename), 'w') as f:
      json.dump(machine_config_with_spawner, f)

    host_tcp_port = self._new_port()
    host_udp_port = self._new_port()
    self._host_tcp_port[machine_controller_id] = host_tcp_port
    self._host_udp_port[machine_controller_id] = host_udp_port

    container = self._docker.containers.run(
        image=self.image,
        command=[
            'pipenv',
            'run',
            'python',
            '-m',
            'dist_zero.machine_init',
            os.path.join(DockerSpawner.CONTAINER_MESSAGE_DIR, config_filename),
        ],
        detach=True,
        labels={
            DockerSpawner.LABEL_DOCKER_SIMULATED_HARDWARE: DockerSpawner.LABEL_TRUE,
            DockerSpawner.LABEL_INSTANCE: self._system_id,
            DockerSpawner.LABEL_CONTAINER_UUID: machine_controller_id,
        },
        auto_remove=False,
        ports={
            f'{settings.MACHINE_CONTROLLER_DEFAULT_TCP_PORT}/tcp': host_tcp_port,
            f'{settings.MACHINE_CONTROLLER_DEFAULT_UDP_PORT}/udp': host_udp_port,
        },
        volumes={
            self._root_dir: {
                'bind': DockerSpawner.CONTAINER_WORKING_DIR,
                'mode': 'ro',
            },
            host_msg_dir: {
                'bind': DockerSpawner.CONTAINER_MESSAGE_DIR,
                'mode': 'rw',
            },
            log_dir: {
                'bind': DockerSpawner.CONTAINER_LOGS_DIR,
                'mode': 'rw',
            },
        },
    )
    self._network.connect(container)

    self._container_by_id[machine_controller_id] = container

    return machine_controller_id

  def _get_containers_from_docker(self):
    '''
    Get all containers associated with this instance from the docker daemon.

    :return: The list of all docker container objects associated with
      this particular instance of `DockerSpawner`
    '''
    labels_query = "{}={}".format(DockerSpawner.LABEL_INSTANCE, self._system_id)
    return self._docker.containers.list(all=True, filters={'label': labels_query})

  def get_running_containers(self):
    '''
    Get the list of this `DockerSpawner` instance's running containers from the docker daemon.

    :return: A list of container ids for all running containers spawned by this `DockerSpawner`
    :rtype: list[str]
    '''
    return [
        container.labels[DockerSpawner.LABEL_CONTAINER_UUID] for container in self._get_containers_from_docker()
        if container.status == DockerSpawner.CONTAINER_STATUS_RUNNING
    ]

  def get_stopped_containers(self):
    '''
    Get the list of this `DockerSpawner` instance's non-running containers from the docker daemon.

    :return: A list of container ids for all running containers spawned by this `DockerSpawner`
    :rtype: list[str]
    '''
    return [
        container.labels[DockerSpawner.LABEL_CONTAINER_UUID] for container in self._get_containers_from_docker()
        if container.status == DockerSpawner.CONTAINER_STATUS_RUNNING
    ]

  def all_spawned_containers(self):
    '''
    :return: The list of all container ids for containers this `DockerSpawner` has ever spawned.
    :rtype: list[str]
    '''
    return list(self._container_by_id.keys())

  def clean_all(self):
    '''
    Remove all the docker resources associated with any instance of `DockerSpawner`
    (not just the current instance).
    '''
    labels_query = "{}={}".format(DockerSpawner.LABEL_DOCKER_SIMULATED_HARDWARE, DockerSpawner.LABEL_TRUE)

    containers = self._docker.containers.list(all=True, filters={'label': labels_query})
    logger.debug("Removing containers {}".format(containers), extra={'n_containers_to_remove': len(containers)})
    for container in containers:
      if container.status == DockerSpawner.CONTAINER_STATUS_RUNNING:
        try:
          container.kill()
        except docker.errors.APIError as err:
          logger.warning("Failed to kill a container. It may have died on its own")
      container.remove()
    if self._network is not None:
      self._network.remove()
