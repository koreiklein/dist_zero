import itertools

from dist_zero import errors, messages


class Publisher(object):
  '''
  Each `DataNode` instance of height > 0 will have a single `Publisher` instance variable.
  That `Publisher` will be responsible for

    - keeping track of which nodes are subscribed to which input/output link keys.
    - publishing changes to the structure of the kids of the `DataNode` to all the subscribed nodes.
  '''

  def __init__(self, dataset_program_config):
    '''
    :param dataset_program_config: A configuration object describing what kind of program
      the associated dataset is running.  It can be used to determine which link keys may be subscribed to.
    '''

    if dataset_program_config['type'] == 'demo_dataset_program_config':
      self._init_from_demo_dataset_program_config(dataset_program_config)
    else:
      raise errors.InternalError(f"Unrecognized leaf type '{dataset_program_config['type']}'.")

  def _init_from_demo_dataset_program_config(self, demo_dataset_program_config):
    # Map each link_key to either None, or the handle of the linked node
    self._inputs = {key: None for key in demo_dataset_program_config['input_link_keys']}
    self._outputs = {key: None for key in demo_dataset_program_config['output_link_keys']}

  def subscribe_input(self, handle):
    key = handle['id']
    if key not in self._inputs:
      raise errors.InternalError(
          "subscribe_input: Key \"{key}\" not found in input keys \"{list(self._inputs.keys())}\"")
    existing = self._inputs[key]
    if existing is not None:
      raise errors.InternalError("subscribe_input: Key \"{key}\" was already subscribed to by \"{existing['id']}\"")
    self._inputs[key] = handle

  def subscribe_output(self, handle):
    key = handle['id']
    if key not in self._outputs:
      raise errors.InternalError(
          "subscribe_output: Key \"{key}\" not found in output keys \"{list(self._outputs.keys())}\"")
    existing = self._outputs[key]
    if existing is not None:
      raise errors.InternalError("subscribe_output: Key \"{key}\" was already subscribed to by \"{existing['id']}\"")
    self._outputs[key] = handle
