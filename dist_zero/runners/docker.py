import docker

from dist_zero import settings, errors

class DockerSimulatedHardware(object):
  '''
  A class for simulating new hardware by spinning up docker containers on a host and
  running a 'production' `OsMachineController` on each.

  Tests should typically create a single instance of this factory, and use it to generate
  all their `machine.MachineController` instances.  User input in such tests
  will tend to come from `RecordedUser` instances running in each container.
  '''
  def __init__(self):
    self._started = False
    self._docker_client = None

  def start(self):
    if self._started:
      raise errors.InternalError("DockerSimulatedHardware has already been started.")

    self._started = True
    self._docker_client = docker.DockerClient(base_url=settings.DOCKER_BASE_URL)
