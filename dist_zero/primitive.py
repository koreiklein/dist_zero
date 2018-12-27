'''
Primitive operators for the DistZero language.
'''

from dist_zero import errors, types, cgen


class PrimitiveOp(object):
  def get_type(self):
    raise errors.AbstractSuperclass(self.__class__)

  def generate_primitive_initialize_state(self, cFunction, arg, lvalue):
    raise errors.AbstractSuperclass(self.__class__)

  def generate_react_to_transitions(self, compiler, block, vGraph, maintainState, arg, expr):
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

  def generate_react_to_transitions(self, compiler, block, vGraph, maintainState, arg, expr):
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
  def generate_react_to_transitions(self, compiler, block, vGraph, maintainState, arg, expr):
    outputTransitions = compiler.transitions_rvalue(vGraph, expr)
    output_transition_ctype = compiler.get_c_transition_type(expr)

    arg_type = compiler.get_type_for_expr(arg)
    arg_c_transition_type = compiler.get_c_transition_type(arg)
    arg_enum = arg_c_transition_type.field_by_id['type']
    arg_union = arg_c_transition_type.field_by_id['value']

    argTransitions = compiler.transitions_rvalue(vGraph, arg)

    argTransitionIndex = cgen.Var('arg_transition_index', cgen.MachineInt)
    block.AddAssignment(cgen.CreateVar(argTransitionIndex), cgen.Zero)
    loop = block.AddWhile(argTransitionIndex < cgen.kv_size(argTransitions))

    transition = cgen.kv_A(argTransitions, argTransitionIndex)
    switch = loop.AddSwitch(transition.Dot('type'))

    # NOTE: Not all cases are product_on_{key} cases.  The others should also be handled.
    for key, _value in arg_type.items:
      product_on_key = f"product_on_{key}"
      case = switch.AddCase(arg_enum.literal(product_on_key))
      nextValue = transition.Dot('value').Dot(product_on_key).Deref()
      case.AddAssignment(None, cgen.kv_push(output_transition_ctype, outputTransitions, nextValue))
      case.AddBreak()

    default = switch.AddDefault()
    default.AddAssignment(None, compiler.pyerr_from_string("Unrecognized input transition to operation.")).AddReturn(
        cgen.true)

    loop.AddAssignment(cgen.UpdateVar(argTransitionIndex), argTransitionIndex + cgen.One)


Plus = lambda t: PlusBinOp('+', t, c_operation=cgen.Plus)
Minus = lambda t: BinOp('-', t, c_operation=cgen.Minus)
Times = lambda t: BinOp('*', t, c_operation=cgen.Times)
Div = lambda t: BinOp('/', t, c_operation=cgen.Div)
Mod = lambda t: BinOp('%', t, c_operation=cgen.Mod)
