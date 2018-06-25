from dist_zero import errors

from .docker import DockerSpawner
from .cloud.aws import Ec2Spawner


def from_config(spawner_config):
  if spawner_config['type'] == 'docker':
    return DockerSpawner.from_spawner_json(spawner_config['value'])
  elif spawner_config['type'] == 'aws':
    return Ec2Spawner.from_spawner_json(spawner_config['value'])
  else:
    raise errors.InternalError('Unrecognized spawner type "{}"'.format(spawner_config['type']))
