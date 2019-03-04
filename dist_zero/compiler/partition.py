from dist_zero import program
from . import cardinality


class Partitioner(object):
  def __init__(self):
    self._cardinality_to_ds = {} # map each Cardinality in the input program to its Dataset instance

    self._compiler = None

  def _new_dataset(self, name=None):
    return self._compiler._program.new_dataset(name=name)

  def _partition_subtrie(self, subtrie: cardinality.CardinalityTrie, ds: program.Dataset):
    '''
    Assign ``ds`` to any cardinality at ``subtrie``, and find suitable datasets for all cardinalities within ``subtrie``

    :param CardinalityTrie subtrie: A subtrie of the main `CardinalityTrie` of the input program.
    :param Dataset ds: A default dataset.
    '''
    if subtrie.cardinality is not None:
      self._cardinality_to_ds[subtrie.cardinality] = ds

    for list_exprs, kid_trie in subtrie.items():
      if self._is_large(list_exprs):
        kid_ds = self._new_dataset(name=self._name_cardinality_dataset(kid_trie.cardinality))
      else:
        kid_ds = ds

      self._partition_subtrie(kid_trie, kid_ds)

  def _is_large(self, list_exprs):
    return any(self._compiler.list_is_large(expr) for expr in list_exprs)

  def _name_cardinality_dataset(self, cardinality):
    # NOTE(KK): It may at some point be possible to assign nicer and more informative names to dataset.
    return "DataNode"

  def partition(self, compiler):
    self._compiler = compiler

    global_dataset = self._new_dataset(name='Globals')
    self._partition_subtrie(compiler._cardinality_trie, global_dataset)

    result = {}
    for expr, cardinality in self._compiler._cardinality.items():
      result[expr] = self._cardinality_to_ds[cardinality]

    return result
