import os
import uuid
import logging

import docker

from dist_zero import settings, errors, messages

logger = logging.getLogger(__file__)

class DockerSimulatedHardware(object):
  '''
  A class for simulating new hardware by spinning up docker containers on a host and
  running a 'production' `OsMachineController` on each.

  Tests should typically create a single instance of this factory, and use it to generate
  all their `machine.MachineController` instances.  User input in such tests
  will tend to come from `RecordedUser` instances running in each container.
  '''

  DOCKERFILE = 'dist_zero/runners/docker/Dockerfile'

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

    self._image = None
    self._build_logs = None
    self._handle_by_id = {}

  def start(self):
    if self._started:
      raise errors.InternalError("DockerSimulatedHardware has already been started.")

    self._started = True
    self._docker_client = docker.DockerClient(base_url=settings.DOCKER_BASE_URL)

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
          rm=True,  # Remove intermediate containers
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

  def new_container(self):
    image = self.image
    machine_controller_id = str(uuid.uuid4())
    container = self._docker.containers.run(
        image=self.image,
        command=[
          'python',
          '-m',
          'dist_zero.os_machine_controller',
          machine_controller_id,
          ],
        detach=True,
        labels={
          DockerSimulatedHardware.LABEL_DOCKER_SIMULATED_HARDWARE: DockerSimulatedHardware.LABEL_TRUE,
          DockerSimulatedHardware.LABEL_INSTANCE: self.id,
          DockerSimulatedHardware.LABEL_CONTAINER_UUID: machine_controller_id,
          },
        auto_remove=False,
        volumes={
          self._root_dir: { 'bind': '/machine', 'mode': 'ro' },
          },
        )

    handle = messages.os_machine_controller_handle(machine_controller_id)
    self._handle_by_id[machine_controller_id] = handle
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
    labels_query = "{}={}".format(
        DockerSimulatedHardware.LABEL_DOCKER_SIMULATED_HARDWARE,
        DockerSimulatedHardware.LABEL_TRUE)

    containers = self._docker.containers.list(all=True, filters={'label': labels_query})
    logger.debug("Removing containers {}".format(containers), extra={'n_containers_to_remove': len(containers)})
    for container in containers:
      if container.status == DockerSimulatedHardware.CONTAINER_STATUS_RUNNING:
        container.kill()
      container.remove()

