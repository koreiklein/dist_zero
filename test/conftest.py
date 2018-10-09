import pytest

import dist_zero.ids
from dist_zero.spawners.simulator import SimulatedSpawner
from dist_zero.spawners.docker import DockerSpawner
from dist_zero.spawners.cloud.aws import Ec2Spawner
from dist_zero.system_controller import SystemController

from .demo import demo, cloud_demo


@pytest.fixture
def simulated_system():
  system_id = dist_zero.ids.new_id('System')
  simulated_spawner = SimulatedSpawner(system_id=system_id, random_seed='TestSimulatedSpawner')
  system = SystemController(system_id=system_id, spawner=simulated_spawner)
  system.configure_logging()
  simulated_spawner.start()

  return system


@pytest.fixture
def virtual_system():
  system_id = dist_zero.ids.new_id('System')
  virtual_spawner = DockerSpawner(system_id=system_id, inside_container=False)
  system = SystemController(system_id=system_id, spawner=virtual_spawner)
  system.configure_logging()
  virtual_spawner.start()

  yield system

  virtual_spawner.clean_all()


@pytest.fixture
def cloud_system():
  system_id = dist_zero.ids.new_id('System')
  cloud_spawner = Ec2Spawner(system_id=system_id)
  system = SystemController(system_id=system_id, spawner=cloud_spawner)
  system.configure_logging()

  yield system

  cloud_spawner.clean_all()
