'''
Common code for all messages modules.
Certain common messages go here as well.
'''

ENCODING = 'utf-8'
'''
The encoding to use for messages.
This should be a string understood by the python internals that operate on encodings.
'''


def node_config(node_id, first_function_name, first_function_kwargs=None):
  '''
  A configuration for running a new node.
  These are sent to inform `MachineController` instances to add a new node.

  :param str node_id: The id to use for new `Node` instance.
  :param str first_function_name: The name of the initial coroutine function to run on the new node.
  :param dict first_function_kwargs: Optional kwargs parameters to pass to the initial coroutine function.
    These parameters must be JSON serializable.
  '''
  return {
      'type': 'node_config',
      'id': node_id,
      'first_function_name': first_function_name,
      'first_function_kwargs': first_function_kwargs if first_function_kwargs is not None else {},
  }
