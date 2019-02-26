import itertools

from dist_zero import errors, messages


class Publisher(object):
  '''
  Each `DataNode` instance will have a single `Publisher` instance variable.
  That `Publisher` will be responsible for

    - keeping track of which nodes are subscribed to which input/output link keys.
    - When the height is > 0,
      publishing changes to the structure of the kids of the `DataNode` to all the subscribed nodes.
    - When the height is == 0,
      running the reactive Net object in inputs from senders, and sending outputs to receivers

  '''

  def __init__(self, is_leaf, dataset_program_config):
    '''
    :param bool is_leaf: True iff the node is of height 0.
    :param dataset_program_config: A configuration object describing what kind of program
      the associated dataset is running.  It can be used to determine which link keys may be subscribed to.
    '''
    self._is_leaf = is_leaf
    # FIXME(KK): We should make sure to actually run the reactive graph in the event that this is a leaf node

    if dataset_program_config['type'] == 'demo_dataset_program_config':
      self._init_from_demo_dataset_program_config(dataset_program_config)
    else:
      raise errors.InternalError(f"Unrecognized leaf type '{dataset_program_config['type']}'.")

  def _init_from_demo_dataset_program_config(self, demo_dataset_program_config):
    # Map each link_key to either None, or the handle of the linked node
    self._inputs = {key: None for key in demo_dataset_program_config['input_link_keys']}
    self._outputs = {key: None for key in demo_dataset_program_config['output_link_keys']}

  def get_linked_handle(self, link_key, key_type):
    if key_type == 'input':
      return self._inputs.get(link_key, None)
    elif key_type == 'output':
      return self._outputs.get(link_key, None)
    else:
      raise errors.InternalError(f"Unrecognized link key \"{link_key}\"")

  def inputs(self):
    return (node for node in self._inputs.values() if node is not None)

  def outputs(self):
    return (node for node in self._outputs.values() if node is not None)

  def subscribe_input(self, link_key, handle):
    if link_key not in self._inputs:
      raise errors.InternalError(
          f"subscribe_input: Key \"{link_key}\" not found in input keys \"{list(self._inputs.keys())}\"")
    existing = self._inputs[link_key]
    if existing is not None:
      raise errors.InternalError(
          f"subscribe_input: Key \"{link_key}\" was already subscribed to by \"{existing['id']}\"")
    self._inputs[link_key] = handle

  def subscribe_output(self, link_key, handle):
    if link_key not in self._outputs:
      raise errors.InternalError(
          f"subscribe_output: Key \"{link_key}\" not found in output keys \"{list(self._outputs.keys())}\"")
    existing = self._outputs[link_key]
    if existing is not None:
      raise errors.InternalError(
          f"subscribe_output: Key \"{link_key}\" was already subscribed to by \"{existing['id']}\"")
    self._outputs[link_key] = handle
