from dist_zero import expression


class DistZero(object):
  '''
  Each instance of the `DistZero` class serves as a frontend to all of DistZero.

  It can be used to create `Expression` objects, and compile them into distributed programs to be
  run by the DistZero runtime.
  '''

  def Record(self, **kwargs):
    return expression.Record(items=list(kwargs.items()))

  def Constant(self, value):
    return expression.Constant(value)

  def Lambda(self, f, srcType=None, tgtType=None):
    return expression.Lambda(srcType=srcType, tgtType=tgtType, f=f)
