'''
Primitive operators for the DistZero language.
'''

from dist_zero import errors, types, cgen


class PrimitiveOp(object):
  '''
  A reactive function that can not be decomposed into simpler functions.
  '''

  def get_type(self):
    raise errors.AbstractSuperclass(self.__class__)

  def generate_primitive_initialize_state(self, cFunction, arg, lvalue):
    raise errors.AbstractSuperclass(self.__class__)

  def generate_react_to_transitions(self, compiler, block, vGraph, arg, expr):
    raise errors.AbstractSuperclass(self.__class__)


class BinOp(PrimitiveOp):
  '''A binary operation'''

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

  def generate_react_to_transitions(self, compiler, block, vGraph, arg, expr):
    raise RuntimeError(f'BinOp of type "{self.s}" have not (yet) been programmed to react to transitions.')

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


class PlusBinOp(BinOp):
  def generate_react_to_transitions(self, compiler, block, vGraph, arg, expr):
    outputTransitions = compiler.transitions_rvalue(vGraph, expr)
    output_transition_ctype = compiler.get_concrete_type(expr.type).c_transitions_type

    arg_c_transition_type = compiler.get_concrete_type(arg.type).c_transitions_type
    arg_enum = arg_c_transition_type.field_by_id['type']
    arg_union = arg_c_transition_type.field_by_id['value']

    argTransitions = compiler.transitions_rvalue(vGraph, arg)

    vStart = compiler.vProcessedTransitions(vGraph, expr)
    block.logf(f"  Plus operation is reacting to transitions [%d, %zu).\n", vStart, cgen.kv_size(argTransitions))

    with block.ForInt(cgen.kv_size(argTransitions), vStart=vStart) as (loop, argTransitionIndex):
      transition = cgen.kv_A(argTransitions, argTransitionIndex)
      switch = loop.AddSwitch(transition.Dot('type'))

      # NOTE: Not all cases are product_on_{key} cases.  The others should also be handled.
      for key, _value in arg.type.items:
        product_on_key = f"product_on_{key}"
        case = switch.AddCase(arg_enum.literal(product_on_key))
        nextValue = transition.Dot('value').Dot(product_on_key).Deref()
        case.AddAssignment(None, cgen.kv_push(output_transition_ctype, outputTransitions, nextValue))
        case.logf(f"  Plus operation in product_on_{key} case. Input value: %d\n", nextValue)
        case.AddBreak()

      default = switch.AddDefault()
      default.AddAssignment(None, compiler.pyerr_from_string("Unrecognized input transition to operation.")).AddReturn(
          cgen.true)


Plus = lambda t: PlusBinOp('+', t, c_operation=cgen.Plus)
Minus = lambda t: BinOp('-', t, c_operation=cgen.Minus)
Times = lambda t: BinOp('*', t, c_operation=cgen.Times)
Div = lambda t: BinOp('/', t, c_operation=cgen.Div)
Mod = lambda t: BinOp('%', t, c_operation=cgen.Mod)
