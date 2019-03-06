from dist_zero import program, errors
from . import cardinality


class Partitioner(object):
  def __init__(self, compiler):
    self._cardinality_to_ds = {} # map each Cardinality in the input program to its Dataset instance

    self._compiler = compiler

  def _new_dataset(self, name=None):
    return self._compiler.new_dataset(name=name)

  def _partition_subtrie(self, subtrie: cardinality.CardinalityTrie, ds: program.Dataset):
    '''
    Assign ``ds`` to any cardinality at ``subtrie``, and find suitable datasets for all cardinalities within ``subtrie``

    :param CardinalityTrie subtrie: A subtrie of the main `CardinalityTrie` of the input program.
    :param Dataset ds: A default dataset.
    '''
    if subtrie.cardinality is not None:
      self._cardinality_to_ds[subtrie.cardinality] = ds

    for key, (list_exprs, kid_trie) in subtrie.items():
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

  def partition(self, trie):
    '''
    :return: A map from each `NormExpr` to the `Dataset` on which its values should live.
    :rtype: map[`NormExpr`, `Dataset`]
    '''
    global_dataset = self._new_dataset(name='Globals')
    self._partition_subtrie(trie, global_dataset)

    result = {}
    for expr, cardinality in self._compiler._cardinality.items():
      if cardinality not in self._cardinality_to_ds:
        raise errors.InternalError("Found a cardinality in main compiler that did not exist in the trie.")
      result[expr] = self._cardinality_to_ds[cardinality]

    return result
