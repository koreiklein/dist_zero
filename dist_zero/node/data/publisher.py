import itertools

from dist_zero import errors, messages

from dist_zero.reactive.compiler import ReactiveCompiler
from dist_zero.reactive.serialization import ConcreteExpressionDeserializer


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
    self._spy_key_to_capnp_state_builder = {} # Map each spy key to the pycapnp builder for its state
    # When this is a leaf node, self._net should be set to a running network (see `ReactiveCompiler.compile`)
    self._net = None

    if dataset_program_config['type'] == 'demo_dataset_program_config':
      self._init_from_demo_dataset_program_config(dataset_program_config)
    elif dataset_program_config['type'] == 'reactive_dataset_program_config':
      self._init_from_reactive_dataset_program_config(dataset_program_config)
    else:
      raise errors.InternalError(f"Unrecognized leaf type '{dataset_program_config['type']}'.")

  def _init_from_demo_dataset_program_config(self, demo_dataset_program_config):
    # Map each link_key to either None, or the handle of the linked node
    self._inputs = {key: None for key in demo_dataset_program_config['input_link_keys']}
    self._outputs = {key: None for key in demo_dataset_program_config['output_link_keys']}

  def _init_from_reactive_dataset_program_config(self, dataset_program_config):
    self._outputs = {key: None for key in dataset_program_config['output_key_to_expr_id'].keys()}
    self._inputs = {}
    for expr_json in dataset_program_config['concrete_exprs']:
      if expr_json['type'] == 'Input':
        key = expr_json['value']['name']
        self._inputs[key] = None

    if self._is_leaf:
      self._start_reactive_graph(dataset_program_config)

  def _start_reactive_graph(self, dataset_program_config):
    deserializer = ConcreteExpressionDeserializer()
    deserializer.deserialize_types(dataset_program_config['type_jsons'])
    exprs = deserializer.deserialize(dataset_program_config['concrete_exprs'])

    compiler = ReactiveCompiler(name=dataset_program_config['program_name'])
    # FIXME(KK): Re-use compiled programs modules when they already exist on the same physical machine.
    mod = compiler.compile(
        output_key_to_norm_expr={
            output_key: deserializer.get_by_id(expr_id)
            for output_key, expr_id in dataset_program_config['output_key_to_expr_id'].items()
        },
        other_concrete_exprs=exprs)
    self._net = mod.Net()
    self._spy_key_to_capnp_state_builder = {
        spy_key: compiler.capnp_state_builder(expr)
        for expr in exprs for spy_key in expr.spy_keys
    }

  def spy(self, spy_key):
    if not self._is_leaf:
      raise errors.InternalError("Only leaf nodes can be spied on.")
    method = getattr(self._net, f"Spy_{spy_key}")
    result_buffer = method()
    capnp_builder = self._spy_key_to_capnp_state_builder[spy_key]
    parsed_state = capnp_builder.from_bytes(result_buffer)
    result = parsed_state.to_dict()
    return result

  def elapse(self, ms):
    if self._is_leaf and self._net is not None:
      self._net.Elapse(ms)

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
