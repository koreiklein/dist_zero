'''
Primitive operators for the DistZero language.
'''

from dist_zero import errors, types


class PrimitiveOp(object):
  def get_type(self):
    raise errors.AbstractSuperclass(self.__class__)


class BinOp(PrimitiveOp):
  def __init__(self, s, type):
    self.s = s
    self.output_type = type
    self.input_type = types.Product(items=[
        ('left', type),
        ('right', type),
    ])
    self.type = types.FunctionType(src=self.input_type, tgt=self.output_type)

  def __str__(self):
    return self.s

  def get_input_type(self):
    return self.input_type

  def get_output_type(self):
    return self.output_type

  def get_type(self):
    return self.type


Plus = lambda t: BinOp('+', t)
Minus = lambda t: BinOp('-', t)
Times = lambda t: BinOp('*', t)
Div = lambda t: BinOp('/', t)
Mod = lambda t: BinOp('%', t)
