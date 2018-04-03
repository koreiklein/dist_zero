import os
import uuid
import logging

import docker

from dist_zero import settings, errors

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
  LABEL_INSTANCE = 'DockerSimulatedHarware_instance'

  CONTAINER_STATUS_RUNNING = 'running'

  def __init__(self):
    self._started = False
    self._docker_client = None
    self.id = str(uuid.uuid4())

    self._dir = os.path.dirname(os.path.realpath(__file__))
    self._root_dir = os.path.realpath(os.path.join(self._dir, '../../..'))

    self._image = None
    self._build_logs = None

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
    container = self._docker.containers.run(
        image=self.image,
        command=[
          'python',
          '-m',
          'dist_zero.os_machine_controller',
          ],
        detach=True,
        labels={
          DockerSimulatedHardware.LABEL_DOCKER_SIMULATED_HARDWARE: DockerSimulatedHardware.LABEL_TRUE,
          DockerSimulatedHardware.LABEL_INSTANCE: self.id,
          },
        auto_remove=False,
        volumes={
          self._root_dir: { 'bind': '/machine', 'mode': 'ro' },
          },
        )

  def clean_all(self):
    '''
    Remove all the docker resources associated with this instance
    '''
    labels_query = "{}={}".format(
        DockerSimulatedHardware.LABEL_DOCKER_SIMULATED_HARDWARE,
        DockerSimulatedHardware.LABEL_TRUE)

    containers = self._docker.containers.list(all=True, filters={'label': labels_query})
    logger.debug("Removing containers {}".format(containers), extra={'n_containers_to_remove': len(containers)})
    for container in containers:
      if container.status == DockerSimulatedHardware.CONTAINER_STATUS_RUNNING:
        container.stop(timeout=2)
      container.remove()

