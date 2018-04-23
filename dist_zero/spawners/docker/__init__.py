import json
import logging
import os
import uuid

import docker
import docker.errors

from dist_zero import machine, settings, errors, messages, spawners
from .. import spawner

logger = logging.getLogger(__name__)


class DockerSpawner(spawner.Spawner):
  '''
  A class for simulating new hardware by spinning up docker containers on a host and
  running a 'production' `OsMachineController` on each.
  '''

  DOCKERFILE = 'dist_zero/spawners/docker/Dockerfile'
  CONTAINER_WORKING_DIR = '/machine' # Must match the working directory in DOCKERFILE

  CONTAINER_MESSAGE_DIR = '/messages'
  CONTAINER_LOGS_DIR = '/logs'

  LABEL_DOCKER_SIMULATED_HARDWARE = 'DockerSimulatedHarware'
  LABEL_TRUE = 'true'
  LABEL_INSTANCE = '{}_instance'.format(LABEL_DOCKER_SIMULATED_HARDWARE)
  LABEL_CONTAINER_UUID = '{}_container_uuid'.format(LABEL_DOCKER_SIMULATED_HARDWARE)

  CONTAINER_STATUS_RUNNING = 'running'

  STD_DOCKER_IMAGE_TAG = 'dist_zero_std_docker_image'

  def __init__(self, system_id):
    self._started = False
    self._docker_client = None
    self._system_id = system_id

    self.id = str(uuid.uuid4())

    self._dir = os.path.dirname(os.path.realpath(__file__))
    self._root_dir = os.path.realpath(os.path.join(self._dir, '../../..'))

    self._network = None

    self._image = None
    self._image_tag = DockerSpawner.STD_DOCKER_IMAGE_TAG
    self._build_logs = None
    self._handle_by_id = {}
    self._container_by_id = {}

    self._all_containers_msg_dir = os.path.join(self._root_dir, '.tmp', 'containers')
    '''Directory in which all the container msg directories are stored.'''

    self._n_sent_messages = 0
    '''
    The total number of messages sent to all `MachineController`s.
    '''

  def mode(self):
    return spawners.MODE_VIRTUAL

  def send_to_machine(self, machine, message, sock_type='udp'):
    host_msg_dir = self._container_msg_dir_on_host(machine['id'])
    filename = "message_{}.json".format(self._n_sent_messages)
    self._n_sent_messages += 1

    logger.info("attempting to exec send_local_msg_from_file on docker container")

    with open(os.path.join(host_msg_dir, filename), 'w') as f:
      json.dump(message, f)

    exit_code, output = self._container_by_id[machine['id']].exec_run([
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
      return json.loads(output)
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
            DockerSpawner.LABEL_INSTANCE: self.id,
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

  def create_machines(self, machine_configs):
    return [self.create_machine(machine_config) for machine_config in machine_configs]

  def create_machine(self, machine_config):
    machine_name = machine_config['machine_name']
    machine_controller_id = machine_config['id']

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
            spawners.MODE_VIRTUAL,
            self._system_id,
        ],
        detach=True,
        labels={
            DockerSpawner.LABEL_DOCKER_SIMULATED_HARDWARE: DockerSpawner.LABEL_TRUE,
            DockerSpawner.LABEL_INSTANCE: self.id,
            DockerSpawner.LABEL_CONTAINER_UUID: machine_controller_id,
        },
        auto_remove=False,
        volumes={
            self._root_dir: {
                'bind': DockerSpawner.CONTAINER_WORKING_DIR,
                'mode': 'ro',
            },
            host_msg_dir: {
                'bind': DockerSpawner.CONTAINER_MESSAGE_DIR,
                'mode': 'ro',
            },
            log_dir: {
                'bind': DockerSpawner.CONTAINER_LOGS_DIR,
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
      this particular instance of `DockerSpawner`
    '''
    labels_query = "{}={}".format(DockerSpawner.LABEL_INSTANCE, self.id)
    return self._docker.containers.list(all=True, filters={'label': labels_query})

  def get_running_containers(self):
    '''
    Get the list of this `DockerSpawner` instance's running containers from the docker daemon.

    :return: A list of container handles for all running containers spawned by this `DockerSpawner`
    '''
    return [
        self._handle_by_id[container.labels[DockerSpawner.LABEL_CONTAINER_UUID]]
        for container in self._get_containers_from_docker()
        if container.status == DockerSpawner.CONTAINER_STATUS_RUNNING
    ]

  def get_stopped_containers(self):
    '''
    Get the list of this `DockerSpawner` instance's non-running containers from the docker daemon.

    :return: A list of container handles for all running containers spawned by this `DockerSpawner`
    '''
    return [
        self._handle_by_id[container.labels[DockerSpawner.LABEL_CONTAINER_UUID]]
        for container in self._get_containers_from_docker()
        if container.status == DockerSpawner.CONTAINER_STATUS_RUNNING
    ]

  def all_spawned_containers(self):
    '''
    :return: The list of all container handles for containers this `DockerSpawner` has ever spawned.
    '''
    return list(self._handle_by_id.values())

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
