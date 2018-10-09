import pytest

import dist_zero.ids
from dist_zero.spawners.simulator import SimulatedSpawner
from dist_zero.spawners.docker import DockerSpawner
from dist_zero.spawners.cloud.aws import Ec2Spawner
from dist_zero.system_controller import SystemController

from .demo import demo, cloud_demo
