from dist_zero import cgen, errors


class Expression(object):
  def __repr__(self):
    return str(self)


class Applied(Expression):
  def __init__(self, func, arg):
    '''
    :param Primitive func: The operation to apply to the argument.
    :param Expression arg: The input to this function.  Multi-argument functions will take a Product expression as input.
    '''
    self.func = func
    self.arg = arg

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

  def __str__(self):
    items = ', '.join(f"{key}: {value}" for key, value in self.items)
    return f"{{{items}}}"

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    my_c_type = compiler.get_c_state_type(self)

    stateLvalue = compiler.state_lvalue(vGraph, self)

    for key, expr in self.items:
      stateInitFunction.AddAssignment(stateLvalue.Dot(key), compiler.state_rvalue(vGraph, expr).Address())


class Input(Expression):
  def __init__(self, name, type):
    self.name = name
    self.type = type

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    raise errors.InternalError("Input expressions should never generate c code to initialize from prior inputs.")

  def __str__(self):
    return f"Input_{self.name}"
