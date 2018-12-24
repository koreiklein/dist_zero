'''
Primitive operators for the DistZero language.
'''

from dist_zero import errors, types, cgen


class PrimitiveOp(object):
  def get_type(self):
    raise errors.AbstractSuperclass(self.__class__)

  def generate_primitive_initialize_state(self, cFunction, arg, lvalue):
    raise errors.AbstractSuperclass(self.__class__)


class BinOp(PrimitiveOp):
  def __init__(self, s, type, c_operation):
    self.s = s
    self.output_type = type
    self.input_type = types.Product(items=[
        ('left', type),
        ('right', type),
    ])
    self.type = types.FunctionType(src=self.input_type, tgt=self.output_type)
    self.c_operation = c_operation

  def __str__(self):
    return self.s

  def generate_primitive_initialize_state(self, cFunction, argRvalue, resultLvalue):
    cFunction.AddAssignment(resultLvalue,
                            cgen.BinOp(self.c_operation,
                                       argRvalue.Dot('left').Deref(),
                                       argRvalue.Dot('right').Deref()))

  def get_input_type(self):
    return self.input_type

  def get_output_type(self):
    return self.output_type

  def get_type(self):
    return self.type


Plus = lambda t: BinOp('+', t, c_operation=cgen.Plus)
Minus = lambda t: BinOp('-', t, c_operation=cgen.Minus)
Times = lambda t: BinOp('*', t, c_operation=cgen.Times)
Div = lambda t: BinOp('/', t, c_operation=cgen.Div)
Mod = lambda t: BinOp('%', t, c_operation=cgen.Mod)
