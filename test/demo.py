import json
import pytest
import time

import dist_zero.ids

from dist_zero import spawners, messages
from dist_zero.system_controller import SystemController
from dist_zero.spawners.simulator import SimulatedSpawner
from dist_zero.spawners.docker import DockerSpawner
from dist_zero.spawners.cloud.aws import Ec2Spawner


@pytest.fixture(params=[
    pytest.param(spawners.MODE_SIMULATED, marks=pytest.mark.simulated),
    pytest.param(spawners.MODE_VIRTUAL, marks=pytest.mark.virtual),
    pytest.param(spawners.MODE_CLOUD, marks=pytest.mark.cloud),
])
def demo(request):
  result = Demo(mode=request.param)
  result.start()
  request.addfinalizer(lambda: result.tear_down())
  return result


@pytest.fixture(params=[
    pytest.param(spawners.MODE_SIMULATED, marks=pytest.mark.simulated),
    pytest.param(spawners.MODE_VIRTUAL, marks=pytest.mark.virtual),
])
def no_cloud_demo(request):
  result = Demo(mode=request.param)
  result.start()
  request.addfinalizer(lambda: result.tear_down())
  return result


@pytest.fixture(params=[
    pytest.param(spawners.MODE_SIMULATED, marks=pytest.mark.simulated),
])
def simulated_demo(request):
  result = Demo(mode=request.param)
  result.start()
  yield result
  result.tear_down()


class Demo(object):
  '''
  For running Demos of a full distributed system.

  Demos may run in simulated, virtual, or cloud mode.
  '''

  def __init__(self, mode=spawners.MODE_SIMULATED, random_seed=None):
    '''
    :param str mode: The mode in which to run the demo.
    '''
    self.mode = mode
    self.nodes = 0

    self.system = None
    self.spawner = None
    self.simulated_spawner = None
    self.virtual_spawner = None
    self.cloud_spawner = None

    self.random_seed = 'TestSimulatedSpawner' if random_seed is None else random_seed

  def start(self):
    '''Start the demo.'''
    self._set_system_by_mode()

    self.system.configure_logging()

    if self.simulated_spawner:
      self.simulated_spawner.start()
    elif self.virtual_spawner:
      self.virtual_spawner.start()

  def tear_down(self):
    '''Remove any resources created as part of the demo.'''
    if self.virtual_spawner and self.virtual_spawner.started:
      self.virtual_spawner.clean_all()

    if self.cloud_spawner:
      self.cloud_spawner.clean_all()

  def _set_system_by_mode(self):
    self.system_id = dist_zero.ids.new_id('System')
    if self.mode == spawners.MODE_SIMULATED:
      self.spawner = self.simulated_spawner = SimulatedSpawner(system_id=self.system_id, random_seed=self.random_seed)
    elif self.mode == spawners.MODE_VIRTUAL:
      self.spawner = self.virtual_spawner = DockerSpawner(system_id=self.system_id, inside_container=False)
    elif self.mode == spawners.MODE_CLOUD:
      self.spawner = self.cloud_spawner = Ec2Spawner(system_id=self.system_id)
    else:
      raise RuntimeError("Unrecognized mode {}".format(self.mode))

    self.system = SystemController(system_id=self.system_id, spawner=self.spawner)

  @property
  def simulated(self):
    '''True iff this demo is simulated'''
    return self.spawner.mode() == 'simulated'

  def now_ms(self):
    ''':return: The current time in milliseconds'''
    if self.simulated:
      return self.spawner.now_ms()
    else:
      return time.time()

  def run_for(self, ms):
    '''Run for ms milliseconds, in either real or simulated time depending on the current mode.'''
    if self.simulated:
      self.spawner.run_for(int(ms))
    else:
      time.sleep(ms / 1000)

  def new_machine_controllers(self, n, base_config=None, random_seed=None):
    '''
    Create n new machine controllers

    :param int n: The number of new `MachineController` instances to create.
    :param dict base_config: A dictionary of extra parameters to add to the configs for all the newly created machines or `None`.
    :return: The list of the new handles.
    :rtype: list
    '''
    configs = []
    for i in range(n):
      name = 'machine {}'.format(self.nodes)
      self.nodes += 1

      machine_config = json.loads(json.dumps(base_config)) if base_config else {}
      machine_config['machine_name'] = name
      machine_config['machine_controller_id'] = dist_zero.ids.new_id('Machine')
      machine_config['mode'] = self.mode
      machine_config['system_id'] = self.system_id
      machine_config['random_seed'] = "{}:{}".format(random_seed if random_seed is not None else self.random_seed, n)

      configs.append(messages.machine.machine_config(**machine_config))

    return self.system.create_machines(configs)

  def new_machine_controller(self):
    '''Like `Demo.new_machine_controllers` but only creates and returns one.'''
    return self.new_machine_controllers(1)[0]
