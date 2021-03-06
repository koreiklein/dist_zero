from . import normalize, cardinality, partition, localizer

from dist_zero import program, errors, expression


class DistributedCompiler(object):
  '''
  The main distributed compiler for DistZero distributed programs.
  This compiler pulls together all the different compilation phases to produce
  a `DistributedProgram` from a high-level semantic description of how the program
  should behave.
  '''

  def __init__(self, program_name):
    '''
    :param str program_name: A name for the overall program.  It should be safe to use in
      c variables and filenames.
    '''
    self._program_name = program_name

    self._mainExpr = None # The input program
    self._normMainExpr = None # The input program after being normalized
    self._cardinality = None # Map from NormExpr to Cardinality
    self._cardinality_trie = None # A CardinalityTrie formed from all cardinalities in the program
    self._expr_to_ds = None # Map from NormExpr to Dataset

    # Embedded helper classes responsible for the distinct functions/phases of the compiler
    self._normalizer = normalize.Normalizer()
    self._cardinalizer = cardinality.Cardinalizer()
    self._partitioner = partition.Partitioner(self)
    self._localizer = localizer.Localizer(self)

    # The final result program
    self._program = program.DistributedProgram(self._program_name)

  def new_dataset(self, name, singleton):
    return self._program.new_dataset(name=name, singleton=singleton)

  def list_is_large(self, list_expr):
    '''
    For ``list_expr`` a key in a `Cardinality`, determine whether it should be treated as a large
    list, requiring multiple leaves in a `dataset <dist_zero.program.Dataset>` to reprerent it.
    .'''
    if list_expr.__class__ == normalize.NormWebInput:
      return True
    elif list_expr.__class__ == normalize.NormCase:
      return any(self.list_is_large(v) for k, v in list_expr.items)
    elif list_expr.__class__ in [normalize.ElementOf, normalize.CaseOf]:
      if not self.list_is_large(list_expr.base):
        return False

      if list_expr.base.__class__ == normalize.NormWebInput:
        return False # Each element of a NormWebInput is assumed to be not a large list

      # Note that there may be other cases in which the expression really shouldn't be large here.
      # We can deal with those as they come up
      return True
    elif list_expr.__class__ in [normalize.NormConstant, normalize.NormRecordedUser]:
      return False
    elif list_expr.__class__ == normalize.Applied:
      return self.list_is_large(list_expr.arg)
    elif list_expr.__class__ == normalize.NormListOp:
      return self.list_is_large(list_expr.base)
    elif list_expr.__class__ == normalize.NormRecord:
      return any(self.list_is_large(v) for k, v in list_expr.items)
    else:
      # note that if we add any other kinds of lists, we should
      raise errors.InternalError(
          f'We should not be determining the largeness of a list expression of class "{list_expr.__class__}"')

  def compile(self, expr: expression.Expression):
    '''
    Compile a single `Expression` into a `DistributedProgram`

    :param Expression expr: The main expression, describing the behavior of a distributed program.
    :return: The distributed program that implements the behavior described by ``expr``
    :rtype: `DistributedProgram`
    '''
    if self._mainExpr is not None:
      # Users of DistributedCompiler should create a new instance of the compiler for each Expression they'd like to compile
      raise errors.InternalError("DistributedCompiler has already been used to compile a separate expression.")

    self._mainExpr = expr
    self._normMainExpr = self._normalizer.normalize(expr)

    # Assign a Cardinality to each NormExpr
    self._cardinalizer.cardinalize(self._normMainExpr)
    self._cardinality = self._cardinalizer.cardinality()

    # Assign its NormExprs to each Cardinality
    for expr, c in self._cardinality.items():
      c.append_expr(expr)

    self._cardinality_trie = cardinality.CardinalityTrie.build_trie(set(self._cardinality.values()))

    self._expr_to_ds = self._partitioner.partition(self._cardinality_trie)

    self._localizer.localize(self._normMainExpr)

    return self._program
