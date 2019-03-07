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

  def new_dataset(self, name):
    return self._program.new_dataset(name=name)

  def list_is_large(self, list_expr):
    # FIXME(KK): Look into whether there are cases where lists could genuinely be small.
    return True

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
