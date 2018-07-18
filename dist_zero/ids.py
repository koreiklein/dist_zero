'''
`Node` and `MachineController` instances (among other things) often need to be referred to by
id.  This module centralized all the functions for creating and working with unique ids.
'''

import random
import uuid

from dist_zero import settings

rand = random.Random('testing seed')

RAND_CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
RAND_LENGTH = 12


def _new_id_random(prefix, random_id=None):
  return '{}_{}'.format(prefix, random_id or ''.join(rand.choice(RAND_CHARS) for i in range(RAND_LENGTH)))


def _new_id_uuid(prefix, random_id=None):
  return '{}_{}'.format(prefix, random_id or str(uuid.uuid4()))


new_id = _new_id_random if settings.IS_TESTING_ENV else _new_id_uuid
'''
Generate unique ids for things.

:param str prefix: A prefix to prepend to the new id.

:return: A unique id.
:rtype: str
'''
