from . import normalize, cardinality, partition, localizer

from dist_zero import program, errors, expression


class MainCompiler(object):
  '''The main distributed compiler for DistZero programs.'''

  def __init__(self):
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
    self._program = program.DistributedProgram()

  def list_is_large(self, list_expr):
    # FIXME(KK): Look into whether there are cases where lists could genuinely be small.
    return True

  def compile(self, expr: expression.Expression):
    if self._mainExpr is not None:
      # Users of MainCompiler should create a new instance of the compiler for each Expression they'd like to compile
      raise errors.InternalError("MainCompiler has already been used to compile a separate expression.")

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
