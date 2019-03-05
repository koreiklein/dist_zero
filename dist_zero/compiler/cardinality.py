import itertools

from collections import defaultdict

from dist_zero import errors

from . import normalize


class Cardinalizer(object):
  '''
  For determining the `Cardinality` of a set of `NormExprs <NormExpr>`.
  '''

  def __init__(self):
    self._cardinality = {}

  def cardinality(self):
    return dict(self._cardinality)

  def cardinalize(self, expr: normalize.NormExpr):
    '''
    Determine and return the `Cardinality` of a `NormExpr`

    :param NormExpr expr: The normalized expression

    :return: The `Cardinality` of ``expr``
    :rtype: `Cardinality`
    '''
    if expr not in self._cardinality:
      self._cardinality[expr] = self._compute_cardinalize(expr)
    return self._cardinality[expr]

  def _compute_cardinalize(self, expr: normalize.NormExpr):
    if expr.__class__ == normalize.ElementOf:
      return self.cardinalize(expr.base) + expr.base
    elif expr.__class__ == normalize.CaseOf:
      return self.cardinalize(expr.base)
    elif expr.__class__ == normalize.NormRecord:
      return max(itertools.chain((Global, ), (self.cardinalize(value) for key, value in expr.items)))
    elif expr.__class__ == normalize.NormCase:
      return max(
          itertools.chain((self.cardinalize(expr.base), ), (self.cardinalize(value) for key, value in expr.items)))
    elif expr.__class__ == normalize.NormListOp:
      # Despite the fact that the result does not depend on the cardinality of expr.element_expr, we should
      # still compute it to ensure all subexpressions are assigned a cardinality
      self.cardinalize(expr.element_expr)
      # NOTE(KK): It cases where a filtered list is small when the unfiltered list is large,
      # it might make sense to assign the two lists different cardinalities.
      return self.cardinalize(expr.base)
    elif expr.__class__ == normalize.Applied:
      return self.cardinalize(expr.arg)
    elif expr.__class__ in [normalize.NormConstant, normalize.NormRecordedUser, normalize.NormWebInput]:
      return Global
    else:
      raise errors.InternalError(f"Unrecognized type of NormExpr: \"{expr.__class__}\"")


class Cardinality(object):
  '''
  Each instance of `Cardinality` describes the number of times a given `NormExpr` will exist at each point in
  time while a distributed program is running.
  '''

  def __init__(self, list_exprs):
    '''
    :param list list_exprs: A list of `NormExpr` instances.
    '''
    self._list_exprs = list_exprs
    self._exprs = []

  def append_expr(self, expr):
    self._exprs.append(expr)

  def __add__(self, other):
    if not isinstance(other, normalize.NormExpr):
      raise errors.InternalError("Only a NormExpr may be added to a Cardinality")

    result = list(self._list_exprs)
    result.append(other)
    return Cardinality(list_exprs=result)

  def equal(self, other):
    return self._list_exprs == other._list_exprs

  def __lt__(self, other):
    return len(self._list_exprs) <= len(other._list_exprs) and \
        all(a == b for a, b in zip(self._list_exprs, other._list_exprs))

  def __gt__(self, other):
    return other < self

  def __len__(self):
    return len(self._list_exprs)

  def __getitem__(self, index):
    return self._list_exprs[index]


Global = Cardinality(list_exprs=[])


class CardinalityTrie(object):
  def __init__(self, cardinality, d):
    self._cardinality = cardinality
    # self._d must be a dictionary mapping each first key in cardinalities to a pair (list_of_exprs, next_trie)
    self._d = d

  @property
  def cardinality(self):
    return self._cardinality

  def items(self):
    return self._d.items()

  def cardinalities(self):
    yield self._cardinality
    for trie in self._d.values():
      yield from trie.cardinalities()

  @staticmethod
  def build_trie(cardinalities):
    '''
    :param list cardinalities: A list of `Cardinality` instances.
      No two cardinalities may have equal lists of keys and one must be Global.
      Also, the set of cardinalities must be "join closed" in the sense that if two cardinalities a, b
      occur in ``cardinalities``, then the cardinality formed from their longest common prefix occurs 
      in ``cardinalities`` as well.
    '''

    def loop(cs, i):
      # cs is any iterable of cardinalities of length >= i
      # no two cs may be equal at coordinates >= i
      # return a pair (list of keys, trie)
      cardinality = None
      first = defaultdict(list)
      for c in cs:
        if len(c) < i:
          raise errors.InternalError("Impossible! cs contained Cardinalities of length < i")
        elif len(c) == i:
          if cardinality is not None:
            raise errors.InternalError(f"Two cardinalities were equal: \"{cardinality}\" == \"{c}\"")
          cardinality = c
        else:
          first[c[i]].append(c)

      if cardinality is None:
        raise errors.InternalError("Cardinalities were not join closed.")

      d = {key: loop(values, i + 1) for key, values in first.items()}

      if cardinality is None and len(d) == 1:
        key, (rest_keys, rest_trie) = d.popitem()
        return [key] + rest_keys, rest_trie
      else:
        return [], CardinalityTrie(cardinality, d)

    keys, trie = loop(cardinalities, 0)
    if len(keys) != 0 and trie._cardinality != Global:
      raise RuntimeError("Did not find the Global cardinality in the list of cardinalities")
    return trie
