from dist_zero import expression, primitive


class DistZero(object):
  '''
  Each instance of the `DistZero` class serves as a frontend to all of DistZero.

  It can be used to create `Expression` objects, and compile them into distributed programs to be
  run by the DistZero runtime.
  '''

  def Record(self, **kwargs):
    return expression.Record(items=list(kwargs.items()))

  def Case(self, base, **kwargs):
    items = []
    for key, value in kwargs.items():
      if not isinstance(value, expression.Expression):
        value = expression.Lambda(f=value, srcType=None, tgtType=None)
      items.append((key, value))
    return expression.Case(items=items)(base)

  def Project(self, key):
    return expression.PrimitiveExpression(primitive.Project(key))

  def Inject(self, key):
    return expression.PrimitiveExpression(primitive.Inject(key))

  def Constant(self, value):
    return expression.Constant(value)

  def Lambda(self, f, srcType=None, tgtType=None):
    return expression.Lambda(srcType=srcType, tgtType=tgtType, f=f)

  def WebInput(self, *args, **kwargs):
    return expression.WebInput(*args, **kwargs)

  def Map(self, base, f):
    if not isinstance(f, expression.Expression):
      f = expression.Lambda(f=f, srcType=None, tgtType=None)
    return expression.ListOp(opVariant='map', f=f)(base)
