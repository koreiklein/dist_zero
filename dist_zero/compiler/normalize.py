'''For normalizing `dist_zero.expression.Expression` instances'''

from dist_zero import errors
from dist_zero.types import Type
from dist_zero import expression
from dist_zero import primitive


class Normalizer(object):
  '''Normalizes `Expression` objects into `NormExpr` objects'''

  def __init__(self):
    # Map expression to normalized expression.  For memoization so we don't normalize the same expression twice
    self._norm_expr = {}

  def normalize(self, expr):
    '''
    Normalize an expression and all its subexpressions.  Repeated calls to this method will cache their results.

    :param Expression expr: An expression to normalize.
    :return: The normalized expression corresponding to ``expr``.  raises `NotNormalizableError` if ``expr`` can
      not be fully normalized.
    :rtype: `NormExpr`
    '''
    return self._eval(expr, full=True)

  def _eval(self, expr, full=False):
    result = self._norm_expr.get(expr, None)
    if result is None:
      result = self._compute_eval(expr, full=full)
      result.spy_keys = expr.spy_keys
      self._norm_expr[expr] = result
    return result

  def _apply_primitive(self, arg_norm_expr, p):
    # Sometimes, it's actually possible to calculate the result of applying p in advance.
    # Do that if possible, otherwise return the generic `Applied`
    if p.__class__ == primitive.Project and arg_norm_expr.__class__ == NormRecord:
      return arg_norm_expr[p.key]
    else:
      return Applied(arg=arg_norm_expr, p=p)

  def _apply_case(self, arg_norm_expr, f_expr):
    if arg_norm_expr.__class__ == Applied and arg_norm_expr.p.__class__ == primitive.Inject:
      key = arg_norm_expr.p.key
      return self._apply(f_expr.dict()[key], self._eval(arg_norm_expr.arg))
    else:
      return NormCase(
          base=arg_norm_expr, items=[(key, self._apply(f, arg_norm_expr.case_of(key))) for key, f in f_expr.items])

  def _apply(self, f_expr, arg_expr):
    # This _eval call should not fully normalize f_expr, as it must be of a function type.
    # We expect to get an unnormalized result and apply it to arg_expr
    f_expr = self._eval(f_expr)

    arg_norm_expr = self._eval(arg_expr)

    if f_expr.__class__ == expression.Lambda:
      return self._eval(f_expr.f(arg_norm_expr))
    elif f_expr.__class__ == expression.PrimitiveExpression:
      return self._apply_primitive(arg_norm_expr=arg_norm_expr, p=f_expr.primitive)
    elif f_expr.__class__ == expression.ListOp:
      return NormListOp(
          base=arg_norm_expr, opVariant=f_expr.opVariant, element_expr=self._apply(f_expr.f, arg_norm_expr.element_of))
    elif f_expr.__class__ == expression.Case:
      return self._apply_case(arg_norm_expr=arg_norm_expr, f_expr=f_expr)
    else:
      # f_expr was not a function
      raise errors.InternalError(f"Normalizer should never be asked to apply an expr of class \"{f_expr.__class__}\"")

  def _compute_eval(self, expr, full=False):
    if isinstance(expr, NormExpr):
      return expr
    elif expr.__class__ == expression.Apply:
      return self._eval(self._apply(expr.f, expr.arg), full=full)
    elif expr.__class__ == expression.Record:
      return NormRecord(items=[(key, self._eval(value, full=full)) for key, value in expr.items])
    elif expr.__class__ == expression.Constant:
      return NormConstant(value=expr.value)
    elif expr.__class__ == expression.WebInput:
      return NormWebInput(domain_name=expr.domain_name)
    elif expr.__class__ == expression.RecordedUser:
      return NormRecordedUser(expr.concrete_recorded_user)
    else:
      # It can't be normalized any more
      if full:
        raise errors.InternalError(f"Unable to fully normalize expression \"{expr}\"")
      else:
        return expr


class NormExpr(expression.Expression):
  '''
  Abstract base class for the normalized expression objects.
  These are created by `dist_zero.compiler.normalize` and are consumed by the code
  that assigns `NormExpr` instances to datasets.

  A note about scope:

  Each normalized expression has a notion of a "scope".
  The scope refers to the set of variables and expressions that expression is allowed to depend on.
  Certain expressions (e.g. `CaseOf` and `ElementOf`) only come into scope under special circumstances, and generally
  can not otherwise be used.  When they are permitted, the subclasses of `NormExpr` will explain how.
  '''

  def __init__(self):
    self._element_of = None
    self._case_of = {}

  def equal(self, other):
    raise RuntimeError("Abstract Superclass")

  def __str__(self):
    raise RuntimeError("Abstract Superclass")

  def __repr__(self):
    return str(self)

  @property
  def element_of(self):
    if self._element_of is None:
      self._element_of = ElementOf(self)
    return self._element_of

  def case_of(self, key):
    if key not in self._case_of:
      self._case_of[key] = CaseOf(self, key)

    return self._case_of[key]


class ElementOf(NormExpr):
  '''Represents the typical element of a list.'''

  def __init__(self, base):
    self.base = base
    super(ElementOf, self).__init__()

  def equal(self, other):
    return other.__class__ == ElementOf and self.base.equal(other.base)

  def __str__(self):
    return f"element_of({self.base})"


class CaseOf(NormExpr):
  '''Represents the typical element of one branch of a `Sum` typed value.'''

  def __init__(self, base, key):
    self.base = base
    self.key = key
    super(CaseOf, self).__init__()

  def equal(self, other):
    return other.__class__ == CaseOf and self.key == other.key and self.base.equal(other.base)

  def __str__(self):
    return f"{self.base}.element_of({self.key})"


class NormRecord(NormExpr):
  '''
  A normalized record expression creating universal compound data from components.  Like a tuple or record type in other languages.
  '''

  def __init__(self, items):
    '''
    :param items: The named components of the record.
    :type items: list[tuple[str, `NormExpr`]]
    '''
    self.items = items
    self._d = None
    super(NormRecord, self).__init__()

  def __str__(self):
    inner = ', '.join(f'{key}: {value}' for key, value in self.items)
    return f'Record({inner})'

  def keys(self):
    return [k for k, v in self.items]

  def equal(self, other):
    return other.__class__ == NormRecord and _equal_expr_dicts(self._dict(), other._dict())

  def _dict(self):
    if self._d is None:
      self._d = dict(self.items)

    return self._d

  def __getitem__(self, key):

    return self._dict()[key]


class NormCase(NormExpr):
  '''
  A normalized case expression whose value depends on the state of a `Sum` type argument.
  '''

  def __init__(self, base, items):
    '''
    :param NormExpr base: The base expression of a `Sum` type.
    :param  items: A list of pairs (key, expr) where expr is a `NormExpr`.
      ``CaseOf(base, key)`` is included in the scope of ``expr``.
    :type items: list[tuple[str, `NormExpr`]]
    '''
    self.base = base
    self.items = items
    self._d = None
    super(NormCase, self).__init__()

  def __str__(self):
    inner = ', '.join(f'{key}: {value}' for key, value in self.items)
    return f"(case {self.base} of {inner})"

  def _dict(self):
    if self._d is None:
      self._d = dict(self.items)
    return self._d

  def equal(self, other):
    return other.__class__ == NormCase and self.base.equal(other.base) and _equal_expr_dicts(
        self._dict(), other._dict())


class NormListOp(NormExpr):
  '''
  A normalized pointwise operation on lists.  Examples include map, filter and sort.
  Since these variants are all treated so similarly, they are represented in a common class and distinguished by
  ``opVariant``
  '''

  def __init__(self, base: NormExpr, opVariant, element_expr: NormExpr):
    '''
    :param str opVariant: Identifies which kind of pointwise operation this is.
    :param NormExpr element_expr: An expression involved in this `ListOp`.  How it is used depends on ``opVariant``
      ``ElementOf(base)`` is included in the scope of ``expr``.
    '''
    self.base = base
    self.opVariant = opVariant
    self.element_expr = element_expr
    super(NormListOp, self).__init__()

  def __str__(self):
    return f'{self.opVariant}_{{{self.element_expr}}}'

  def equal(self, other):
    return other.__class__ == NormListOp and self.base.equal(other.base) and self.opVariant == other.opVariant and \
        self.element_expr.equal(other.element_expr)


class Applied(NormExpr):
  '''
  Normalized expression class for a function that has been applied to a fully normalized argument.
  '''

  def __init__(self, arg: NormExpr, p: primitive.PrimitiveOp):
    '''
    :param NormExpr arg: A normalized expression. The argument to the application.
    :param PrimitiveOp p: A primitive operation that is applied to arg.
    '''
    self.arg = arg
    self.p = p
    super(Applied, self).__init__()

  def __str__(self):
    return f'{self.p}({self.arg})'

  def equal(self, other):
    return other.__class__ == Applied and self.arg.equal(other.arg) and self.p == other.p


class NormRecordedUser(NormExpr):
  def __init__(self, concrete_recorded_user):
    self.concrete_recorded_user = concrete_recorded_user
    super(NormRecordedUser, self).__init__()

  def __str__(self):
    return str(self.concrete_recorded_user)

  def equal(self, other):
    return other.__class__ == NormRecordedUser and self.concrete_recorded_user == other.concrete_recorded_user


class NormWebInput(NormExpr):
  '''
  A fundamential input corresponding to a web endpoint identified by a domain name.
  '''

  def __init__(self, domain_name: str):
    '''
    :param str domain_name: A domain name identifying the web endpoint.
    '''
    self.domain_name = domain_name
    super(NormWebInput, self).__init__()

  def __str__(self):
    return f"WebInput({self.domain_name})"

  def equal(self, other):
    return other.__class__ == NormWebInput and self.domain_name == other.domain_name


class NormConstant(NormExpr):
  '''A constant value expression.'''

  def __init__(self, value):
    self.value = value

  def equal(self, other):
    return other.__class__ == NormConstant and self.value == other.value

  def __str__(self):
    return str(self.value)


def _equal_expr_dicts(d0, d1):
  return set(d0.keys()) == set(d1.keys()) and all(value.equal(d1[key]) for key, value in d0.items())
