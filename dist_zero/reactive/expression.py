from dist_zero import cgen, errors, types, concrete_types


class ConcreteExpression(object):
  '''
  Abstract base class for DistZero reactive expressions.
  Instances of `ConcreteExpression` represent the expressions in the reactive program.

  Each subclass defines a different type of expression.

  Each `ConcreteExpression` instance has its representation totally determined.  This means that while it
  can involve several `ConcreteType` objects, it should not involve any `Type` objects.
  This way, importantly, it makes sense to define for each `ConcreteExpression` a single way to compile the code
  to convert from its input data to its output data.
  '''

  @property
  def type(self):
    '''
    :return: The type of the expression.
    :rtype: `dist_zero.types.Type`
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    '''
    Generate c code in ``stateInitFunction`` to initialize the state of this expression in ``vGraph``.
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def generate_free_state(self, compiler, block, stateRvalue):
    '''
    Generate c code in ``block`` to free all memory associated with this expr that is not stored
    directly in the ``vGraph`` struct.
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def generate_react_to_transitions(self, compiler, block, vGraph):
    '''
    Generate c code in ``block`` to:
      - Append the output transitions for self to compiler.transitions_rvalue(self)

    This function may assume that transitions and states have been written for all expressions 'prior' to self.
    It may also also that the kvec given by compiler.transitions_rvalue(self) has been initialized, and is empty.
    If this is an input expression, it may assume that compiler.transitions_rvalue(self) already has the transitions.

    :param compiler: The reactive compiler
    :type compiler: `ReactiveCompiler`

    :param block: A c block
    :type block: `Block`

    :param vGraph: A c variable for a graph pointer
    :type vGraph: `cgen.expression.Var`
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def __repr__(self):
    return str(self)

  def __init__(self):
    self.spy_keys = set()

  def spy(self, key):
    self.spy_keys.add(key)
    return self


class Constant(ConcreteExpression):
  def __init__(self, value, type):
    self._value = value
    self._type = type
    super(Constant, self).__init__()

  @property
  def type(self):
    return self._type

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    type = compiler.get_concrete_type(self.type)
    stateLvalue = compiler.state_lvalue(vGraph, self)
    type.generate_set_state(compiler, stateInitFunction, stateLvalue, self._value)

  def generate_free_state(self, compiler, block, stateRvalue):
    type = compiler.get_concrete_type(self.type)
    type.generate_free_state(compiler, block, stateRvalue)

  def generate_react_to_transitions(self, compiler, block, vGraph):
    # No transitions should ever occur
    block.AddAssignment(None, compiler.pyerr_from_string("Constants do not react to transitions"))
    block.AddReturn(cgen.One)


class Project(ConcreteExpression):
  def __init__(self, key, base):
    self.key = key
    self.base = base
    super(Project, self).__init__()

  @property
  def type(self):
    return self.base.type.d[self.key]

  def __str__(self):
    return f"{self.base}.'{self.key}'"

  def generate_react_to_transitions(self, compiler, block, vGraph):
    outputTransitions = compiler.transitions_rvalue(vGraph, self)
    baseTransitionsRvalue = compiler.transitions_rvalue(vGraph, self.base)

    with block.ForInt(
        cgen.kv_size(baseTransitionsRvalue), vStart=compiler.vProcessedTransitions(vGraph, self)) as (loop, vIndex):
      curTransition = cgen.kv_A(baseTransitionsRvalue, vIndex)
      switch = loop.AddSwitch(curTransition.Dot('type'))

      base_transitions_ctype = compiler.get_concrete_type(self.base.type).c_transitions_type
      output_transitions_ctype = compiler.get_concrete_type(self.type).c_transitions_type
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

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    stateLvalue = compiler.state_lvalue(vGraph, self)
    stateInitFunction.AddAssignment(stateLvalue, compiler.state_rvalue(vGraph, self.base).Dot(self.key).Deref())

  def generate_free_state(self, compiler, block, stateRvalue):
    pass


class Applied(ConcreteExpression):
  '''
  A fully normalized application of a function to its argument.  The function must have
  no more decomposable structure and must be represented by a `PrimitiveOp`
  '''

  def __init__(self, func, arg):
    '''
    :param func: The operation to apply to the argument.
    :type func: `PrimitiveOp`
    :param arg: The input to this function.  Multi-argument functions will take a Product expression as input.
    :type arg: `ConcreteExpression`
    '''
    self.func = func
    self.arg = arg
    self._type = func.get_output_type()
    super(Applied, self).__init__()

  @property
  def type(self):
    return self._type

  def generate_react_to_transitions(self, compiler, block, vGraph):
    self.func.generate_react_to_transitions(compiler, block, vGraph, self.arg, self)

  def __str__(self):
    return f"{self.func}({self.arg})"

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    self.func.generate_primitive_initialize_state(
        stateInitFunction,
        argRvalue=compiler.state_rvalue(vGraph, self.arg),
        resultLvalue=compiler.state_lvalue(vGraph, self))

  def generate_free_state(self, compiler, block, stateRvalue):
    pass


class Product(ConcreteExpression):
  def __init__(self, items):
    '''
    :param list[tuple[str,`Expression`]] items: List of pairs (key, expr).
    '''
    self.items = items
    self._type = types.Product(items=[(k, v.type) for k, v in self.items])
    super(Product, self).__init__()

  @property
  def type(self):
    return self._type

  def generate_react_to_transitions(self, compiler, block, vGraph):
    transition_ctype = compiler.get_concrete_type(self.type).c_transitions_type
    if 'standard' not in self._type.transition_identifiers and 'individual' not in self._type.transition_identifiers:
      raise errors.InternalError("Have not implemented the action of Product on transitions "
                                 "when the output type doesn't have individual transitions.")

    outputTransitions = compiler.transitions_rvalue(vGraph, self)
    block.logf(f"Running product {self._type.name} react to transitions.\n")

    for key, expr in self.items:
      transitions = compiler.transitions_rvalue(vGraph, expr)

      with block.ForInt(
          cgen.kv_size(transitions), vStart=compiler.vProcessedTransitions(vGraph, expr)) as (loop, vIndex):
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

  def __str__(self):
    items = ', '.join(f"{key}: {value}" for key, value in self.items)
    return f"{{{items}}}"

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    my_c_type = compiler.get_concrete_type(self.type).c_state_type

    stateLvalue = compiler.state_lvalue(vGraph, self)

    for key, expr in self.items:
      stateInitFunction.AddAssignment(stateLvalue.Dot(key), compiler.state_rvalue(vGraph, expr).Address())

  def generate_free_state(self, compiler, block, stateRvalue):
    pass


class Input(ConcreteExpression):
  def __init__(self, name, type):
    self.name = name
    self._type = type
    super(Input, self).__init__()

  @property
  def type(self):
    return self._type

  def generate_react_to_transitions(self, compiler, block, vGraph):
    pass

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    raise errors.InternalError("Input expressions should never generate c code to initialize from prior inputs.")

  def generate_free_state(self, compiler, block, stateRvalue):
    t = compiler.get_concrete_type(self._type)
    if t.__class__ == concrete_types.ConcreteProductType:
      t.generate_free_state(compiler, block, stateRvalue)

  def __str__(self):
    return f"Input_{self.name}"
