from dist_zero import ids, errors


class DistributedProgram(object):
  '''
  A static, in memory description of a distributed program.
  It must contain all the information necessary to start up a running instance of the program.
  '''

  def __init__(self):
    self._datasets = []
    self._links = []

    self._spy_key_to_dataset = {} # Maps each spy key to the dataset responsible for it.

  def localize_spy_key(self, spy_key, ds):
    if spy_key in self._spy_key_to_dataset:
      raise errors.InternalError(f"Spy key \"{spy_key}\" was already assigned to a separate dataset.")
    self._spy_key_to_dataset[spy_key] = ds

  def GetDatasetId(self, spy_key):
    return self._spy_key_to_dataset[spy_key]._id

  def new_dataset(self, name=None):
    result = Dataset(root_node_id=ids.new_id(name if name is not None else 'DataNode'))
    self._datasets.append(result)
    return result


class Dataset(object):
  '''A static, in memory description of a single dataset in a distributed program.'''

  def __init__(self, root_node_id):
    '''
    :param str root_node_id: The id to use for the root `DataNode`
    '''
    self._id = root_node_id


class Link(object):
  '''A static, in memory description of a single link between datasets in a distributed program.'''

  def __init__(self, src, tgt):
    '''
    :param str root_node_id: The id to use for the root `LinkNode`
    :param Dataset src: The source dataset of this link.
    :param Dataset tgt: The target dataset of this link.
    '''
    self._id = root_node_id
    self._src = src
    self._tgt = tgt
