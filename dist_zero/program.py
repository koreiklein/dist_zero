from dist_zero import ids, errors, messages, transaction

from dist_zero.reactive.serialization import ConcreteExpressionSerializer


class DistributedProgram(object):
  '''
  A static, in memory description of a distributed program.
  It must contain all the information necessary to start up a running instance of the program.
  '''

  def __init__(self, name):
    '''
    :param str name: A name to use for the program.  It should be safe to use in c variables and filenames.
    '''
    self._id = ids.new_id(f"ProgramNode_{name}" if name is not None else "ProgramNode")
    self._name = name
    self._datasets = []
    self._links = []

    self._spy_key_to_dataset = {} # Maps each spy key to the dataset responsible for it.

  @property
  def id(self):
    return self._id

  def localize_spy_key(self, spy_key, ds):
    if spy_key in self._spy_key_to_dataset:
      raise errors.InternalError(f"Spy key \"{spy_key}\" was already assigned to a separate dataset.")
    self._spy_key_to_dataset[spy_key] = ds

  def GetDatasetId(self, spy_key):
    return self._spy_key_to_dataset[spy_key]._id

  def new_dataset(self, singleton, name=None):
    result = Dataset(
        root_node_id=ids.new_id(name if name is not None else 'DataNode'),
        singleton=singleton,
        program_name=self._name,
    )
    self._datasets.append(result)
    return result

  def new_link(self, link_key, src, tgt, name=None):
    result = Link(node_id=ids.new_id(name if name is not None else 'LinkNode'), link_key=link_key, src=src, tgt=tgt)
    self._links.append(result)
    return result

  def to_program_node_config(self):
    '''Generate and return the node config for a `ProgramNode` that runs this `DistributedProgram`.'''
    return transaction.add_participant_role_to_node_config(
        node_config=messages.program.program_node_config(node_id=self._id),
        transaction_id=ids.new_id('StartProgram'),
        participant_typename='StartProgram',
        args=dict(
            dataset_configs=[dataset.to_config() for dataset in self._datasets],
            link_configs=[link.to_config() for link in self._links],
        ))


class Dataset(object):
  '''A static, in memory description of a single dataset in a distributed program.'''

  def __init__(self, root_node_id, program_name, singleton):
    '''
    :param str root_node_id: The id to use for the root `DataNode`
    '''
    self._id = root_node_id
    self._program_name = program_name
    self.singleton = singleton
    self.concrete_exprs = set()
    self.output_key_map = {}

  @property
  def id(self):
    return self._id

  def to_config(self):
    # Generate json for all exprs and types.
    serializer = ConcreteExpressionSerializer()
    concrete_exprs = [serializer.get(expr) for expr in self.concrete_exprs]

    # Now that the serializer is populated with all the json it needs, generate the config message.
    return messages.program.dataset_config(
        node_id=self._id,
        singleton=self.singleton,
        dataset_program_config=messages.data.reactive_dataset_program_config(
            program_name=self._program_name,
            concrete_exprs=concrete_exprs,
            output_key_to_expr_id={key: serializer.get_id(expr)
                                   for key, expr in self.output_key_map.items()},
            type_jsons=list(serializer.type_jsons()),
        ))


class Link(object):
  '''A static, in memory description of a single link between datasets in a distributed program.'''

  def __init__(self, node_id, link_key, src, tgt):
    '''
    :param str root_node_id: The id to use for the root `LinkNode`
    :param Dataset src: The source dataset of this link.
    :param Dataset tgt: The target dataset of this link.
    '''
    self._id = node_id
    self._link_key = link_key
    self._src = src
    self._tgt = tgt

  @property
  def id(self):
    return self._id

  def to_config(self):
    return messages.program.link_config(
        node_id=self._id, link_key=self._link_key, src_dataset_id=self._src.id, tgt_dataset_id=self._tgt.id)
