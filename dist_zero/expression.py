from dist_zero.types import Type


class Expression(object):
  '''
  Abstract base class for the core expression objects.  These form the starting point for the DistZero compiler.
  '''

  def __call__(self, other):
    return Apply(f=self, arg=other)


class Apply(Expression):
  '''
  Expression class for a function to be applied to an argument.
  '''

  def __init__(self, arg: Expression, f: Expression):
    '''
    :param Expression arg: An expression object.
    :param Expression f: An expression of a function type with a source matching the type of ``arg``.
    '''
    self.arg = arg
    self.f = f


class Lambda(Expression):
  '''
  Create a DistZero function expression from a python function.
  '''

  def __init__(self, srcType: Type, tgtType: Type, f):
    '''
    :param Type srcType: The source type of the function
    :param Type tgtType: The target type of the function
    :param function f: A python function to represent this underlying `Lambda` instance.
      It will be called once each time this lambda is applied during normalization.
    '''
    self.srcType = srcType
    self.tgtType = tgtType
    self.f = f


class Record(Expression):
  '''
  A record expression creating universal compound data from components.  Like a tuple or record type in other languages.
  '''

  def __init__(self, items):
    '''
    :param items: The named components of the record.
    :type items: list[tuple[str, `Expression`]]
    '''
    self.items = items


class Case(Expression):
  '''
  A case expression, building a function on a `Sum` type out of functions on the components.
  '''

  def __init__(self, items):
    '''
    :param  items: A list of pairs (key, expr) where expr is an `Expression` of a function type.
      Each ``expr`` should define the behavior of the case expression when its
      source in the state identified by ``key``.
    :type items: list[tuple[str, `Expression`]]
    '''
    self.items = items


class Constant(Expression):
  '''A constant value expression.'''

  def __init__(self, value):
    self.value = value


class ListOp(Expression):
  '''
  A pointwise operation on lists.  Examples include map, filter and sort.
  Since these variants are all treated so similarly, they are represented in a common class and distinguished by
  ``opVariant``
  '''

  def __init__(self, opVariant, f: Expression):
    '''
    :param str opVariant: Identifies which kind of pointwise operation this is.
    :param Expression f: A function expression involved in this `ListOp`.  How it is used depends on ``opVariant``
    '''
    self.opVariant = opVariant
    self.f = f


class PrimitiveExpression(Expression):
  '''Each instance of `PrimitiveOp` can be treated as an expression by passing it to `PrimitiveExpression`'''

  def __init__(self, primitive):
    '''
    :param PrimitiveOp primitive: A primitive operation
    '''
    self.primitive = primitive


# Fundamental input types


class WebInput(Expression):
  '''
  A fundamential input corresponding to a web endpoint identified by a domain name.
  '''

  def __init__(self, domain_name: str):
    '''
    :param str domain_name: A domain name identifying the web endpoint.
    '''
    self.domain_name
