'''
`Node` and `MachineController` instances (among other things) often need to be referred to by
id.  This module centralized all the functions for creating and working with unique ids.
'''

import uuid


def new_id():
  '''
  Generate unique ids for things.

  :return: A unique id.
  :rtype: str
  '''
  return str(uuid.uuid4())
