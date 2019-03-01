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
    return expression.Case(items=list(kwargs.items()))(base)

  def Project(self, key):
    return expression.PrimitiveExpression(primitive.Project(key))

  def Inject(self, key):
    return expression.PrimitiveExpression(primitive.Inject(key))

  def Constant(self, value):
    return expression.Constant(value)

  def Lambda(self, f, srcType=None, tgtType=None):
    return expression.Lambda(srcType=srcType, tgtType=tgtType, f=f)
