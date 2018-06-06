'''
`Node` and `MachineController` instances (among other things) often need to be referred to by
id.  This module centralized all the functions for creating and working with unique ids.
'''

import uuid


def new_id(prefix):
  '''
  Generate unique ids for things.

  :param str prefix: A prefix to prepend to the new id.

  :return: A unique id.
  :rtype: str
  '''
  return '{}_{}'.format(prefix, str(uuid.uuid4()))
