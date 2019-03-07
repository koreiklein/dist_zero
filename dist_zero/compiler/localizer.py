from collections import defaultdict

from dist_zero import errors, messages

from . import normalize


class Localizer(object):
  '''
  Compiler phase to "localize" each `NormExpr` into a `ConcreteExpression` running in each leaf on a specific
  `Dataset <dist_zero.program.Dataset>`
  '''

  def __init__(self, compiler):
    self._compiler = compiler

    # Map the pair (dataset, normExpr) to the ConcreteExpression instance that represents
    # normExpr on dataset.
    self._concrete_expression = {}
    # Map each dataset to a the dict d giving the ConcreteExpression to use for each output key.
    self._ds_to_output_key_map = {}

  def _expr_to_ds(self, normExpr):
    return self._compiler._expr_to_ds[normExpr]

  def _compute_concrete_expression(self, ds, normExpr):
    '''Actually calculate the ConcreteExpression without memoization.'''
    # FIXME(KK): Test and implement all of these!
    if normExpr.__class__ == normalize.NormRecordedUser:
      return normExpr.concrete_recorded_user
    elif normExpr.__class__ == normalize.ElementOf:
      raise RuntimeError("Not Yet Implemented")
    elif normExpr.__class__ == normalize.CaseOf:
      raise RuntimeError("Not Yet Implemented")
    elif normExpr.__class__ == normalize.NormRecord:
      raise RuntimeError("Not Yet Implemented")
    elif normExpr.__class__ == normalize.NormCase:
      raise RuntimeError("Not Yet Implemented")
    elif normExpr.__class__ == normalize.NormListOp:
      raise RuntimeError("Not Yet Implemented")
    elif normExpr.__class__ == normalize.Applied:
      raise RuntimeError("Not Yet Implemented")
    elif normExpr.__class__ == normalize.NormWebInput:
      raise RuntimeError("Not Yet Implemented")
    elif normExpr.__class__ == normalize.NormConstant:
      raise RuntimeError("Not Yet Implemented")
    else:
      raise errors.InternalError(f"Unrecognized class of normalized expression: \"{normExpr.__class__}\"")

  def localize(self, normExpr):
    '''
    Given a normalized expression ``normExpr``, assign to each of its subexpressions
      - one `ConcreteExpression` instance living on the `dataset <dist_zero.program.Dataset>`
        assigned to ``normExpr`` by `Partitioner.partition`.
      - as many other `ConcreteExpression` instances as are necessary in order to properly link the overall program
        together.

    Create `links <dist_zero.program.Link>` between `datasets <dist_zero.program.Dataset>` to allow each
    expression to receive any necessary input expressions that live on distinct `datasets <dist_zero.program.Dataset>`.

    Each `ConcreteExpression` produced by `localize` will be assigned to exactly one
    `Dataset <dist_zero.program.Dataset>`.

    :param NormExpr normExpr: An expression to localize.

    :return: A `ConcreteExpression` for ``normExpr``
    :rtype: `ConcreteExpression`
    '''
    ds = self._expr_to_ds(normExpr)
    key = (ds, normExpr)

    result = self._concrete_expression.get(key, None)
    if result is None:
      result = self._compute_concrete_expression(ds, normExpr)

      for spy_key in normExpr.spy_keys:
        self._compiler._program.localize_spy_key(spy_key, ds)
        result.spy(spy_key)

      ds.concrete_exprs.add(result)
      self._concrete_expression[key] = result

    return result
