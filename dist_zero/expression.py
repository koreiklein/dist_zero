from dist_zero import cgen, errors


class Expression(object):
  def __repr__(self):
    return str(self)

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    '''
    Generate c code in ``stateInitFunction`` to initialize the state of this expression in ``vGraph``.
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def generate_react_to_transitions(self, compiler, block, vGraph, maintainState):
    '''
    Generate c code in ``block`` to:
      - Append the output transitions for self to compiler.transitions_rvalue(self)
      - If the c expression ``maintainsState`` evaluates to true, update compiler.state_lvalue(self) as well.

    This function may assume that transitions and states have been written for all expressions 'prior' to self.
    It may also also that the kvec given by compiler.transitions_rvalue(self) has been initialized, and is empty.
    If this is an input expression, it may assume that compiler.transitions_rvalue(self) already has the transitions.

    :param compiler: The reactive compiler
    :type compiler: `ReactiveCompiler`

    :param block: A c block
    :type block: `Block`

    :param vGraph: A c variable for a graph pointer
    :type vGraph: `cgen.Var`

    :param maintainsState: A c expression indicating whether we should maintain the state of this expression.
    :type maintainsState: `dist_zero.cgen.Expression`
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def _standard_update_state(self, compiler, block, vGraph, maintainState):
    '''
    For use in implementing generate_react_to_transitions

    Assuming the transitions have already been written, update the state if necessary by applying all the transitions to it.
    '''
    whenMaintainsState = block.AddIf(maintainState).consequent

    vIndex = cgen.Var('index', cgen.MachineInt)
    whenMaintainsState.AddAssignment(cgen.CreateVar(vIndex), cgen.Zero)

    transitions = compiler.transitions_rvalue(vGraph, self)

    loop = whenMaintainsState.AddWhile(vIndex < cgen.kv_size(transitions))
    transition = cgen.kv_A(transitions, vIndex)
    compiler.get_type_for_expr(self).generate_apply_transition(loop, compiler.state_lvalue(vGraph, self),
                                                               compiler.state_rvalue(vGraph, self), transition)
    loop.AddAssignment(cgen.UpdateVar(vIndex), vIndex + cgen.One)


class Project(Expression):
  def __init__(self, key, base):
    self.key = key
    self.base = base

  def __str__(self):
    return f"{self.base}.'{self.key}'"

  def generate_react_to_transitions(self, compiler, block, vGraph, maintainState):
    outputTransitions = compiler.transitions_rvalue(vGraph, self)
    baseTransitionsRvalue = compiler.transitions_rvalue(vGraph, self.base)

    vIndex = cgen.Var('base_index', cgen.MachineInt)
    block.AddAssignment(cgen.CreateVar(vIndex), cgen.Zero)
    loop = block.AddWhile(vIndex < cgen.kv_size(baseTransitionsRvalue))

    curTransition = cgen.kv_A(baseTransitionsRvalue, vIndex)
    switch = loop.AddSwitch(curTransition.Dot('type'))

    base_transitions_ctype = compiler.get_concrete_type_for_expr(self.base).c_transitions_type
    output_transitions_ctype = compiler.get_concrete_type_for_expr(self).c_transitions_type
    c_enum = base_transitions_ctype.field_by_id['type']
    product_on_key = f"product_on_{self.key}"

    onMe = switch.AddCase(c_enum.literal(product_on_key))
    onMe.AddAssignment(
        None,
        cgen.kv_push(output_transitions_ctype, outputTransitions,
                     curTransition.Dot('value').Dot(product_on_key).Deref()))

    onMe.AddBreak()

    default = switch.AddDefault()
    default.AddBreak()

    loop.AddAssignment(cgen.UpdateVar(vIndex), vIndex + cgen.One)

    self._standard_update_state(compiler, block, vGraph, maintainState)

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    stateLvalue = compiler.state_lvalue(vGraph, self)
    stateInitFunction.AddAssignment(stateLvalue, compiler.state_rvalue(vGraph, self.base).Dot(self.key).Deref())


class Applied(Expression):
  def __init__(self, func, arg):
    '''
    :param Primitive func: The operation to apply to the argument.
    :param Expression arg: The input to this function.  Multi-argument functions will take a Product expression as input.
    '''
    self.func = func
    self.arg = arg

  def generate_react_to_transitions(self, compiler, block, vGraph, maintainState):
    self.func.generate_react_to_transitions(compiler, block, vGraph, maintainState, self.arg, self)
    self._standard_update_state(compiler, block, vGraph, maintainState)

  def __str__(self):
    return f"{self.func}({self.arg})"

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    self.func.generate_primitive_initialize_state(
        stateInitFunction,
        argRvalue=compiler.state_rvalue(vGraph, self.arg),
        resultLvalue=compiler.state_lvalue(vGraph, self))


class Product(Expression):
  def __init__(self, items):
    self.items = items

  def generate_react_to_transitions(self, compiler, block, vGraph, maintainState):
    product_type = compiler.get_type_for_expr(self)
    transition_ctype = compiler.get_concrete_type_for_expr(self).c_transitions_type
    if 'standard' not in product_type.transition_identifiers and 'individual' not in product_type.transition_identifiers:
      raise errors.InternalError("Have not implemented the action of Product on transitions "
                                 "when the output type doesn't have individual transitions.")

    vIndex = cgen.Var('component_transitions_index', cgen.MachineInt)
    block.AddDeclaration(cgen.CreateVar(vIndex))
    outputTransitions = compiler.transitions_rvalue(vGraph, self)
    block.logf(f"Running product {product_type.name} react to transitions.\n")

    for key, expr in self.items:
      transitions = compiler.transitions_rvalue(vGraph, expr)

      block.AddAssignment(cgen.UpdateVar(vIndex), cgen.Zero)
      loop = block.Newline().AddWhile(vIndex < cgen.kv_size(transitions))
      product_on_key = f"product_on_{key}"
      innerValue = cgen.kv_A(transitions, vIndex)

      loop.AddAssignment(
          None,
          cgen.kv_push(
              transition_ctype, outputTransitions,
              transition_ctype.literal(
                  type=transition_ctype.field_by_id['type'].literal(product_on_key),
                  value=transition_ctype.field_by_id['value'].literal(
                      key=product_on_key,
                      value=innerValue.Address(),
                  ))))
      loop.AddAssignment(cgen.UpdateVar(vIndex), vIndex + cgen.One)

    self._standard_update_state(compiler, block, vGraph, maintainState)

  def __str__(self):
    items = ', '.join(f"{key}: {value}" for key, value in self.items)
    return f"{{{items}}}"

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    my_c_type = compiler.get_concrete_type_for_expr(self).c_state_type

    stateLvalue = compiler.state_lvalue(vGraph, self)

    for key, expr in self.items:
      stateInitFunction.AddAssignment(stateLvalue.Dot(key), compiler.state_rvalue(vGraph, expr).Address())


class Input(Expression):
  def __init__(self, name, type):
    self.name = name
    self.type = type

  def generate_react_to_transitions(self, compiler, block, vGraph, maintainState):
    self._standard_update_state(compiler, block, vGraph, maintainState)

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    raise errors.InternalError("Input expressions should never generate c code to initialize from prior inputs.")

  def __str__(self):
    return f"Input_{self.name}"
