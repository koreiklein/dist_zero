from .common import INDENT, escape_c


class Expression(object):
  def add_includes(self, program):
    raise NotImplementedError()

  def to_c_string(self, root=False):
    raise NotImplementedError()

  def __call__(self, *args):
    return Call(self, args)

  def Address(self):
    return UnOp(self, op="&")

  def Negate(self):
    return UnOp(self, op="!")

  def __and__(self, other):
    return BinOp(And, self, other)

  def __or__(self, other):
    return BinOp(Or, self, other)

  def __xor__(self, other):
    return BinOp(Xor, self, other)

  def __add__(self, other):
    return BinOp(Plus, self, other)

  def __sub__(self, other):
    return BinOp(Minus, self, other)

  def __mul__(self, other):
    return BinOp(Times, self, other)

  def __mod__(self, other):
    return BinOp(Mod, self, other)

  def __ne__(self, other):
    return BinOp(NotEqual, self, other)

  def __eq__(self, other):
    return BinOp(Equal, self, other)

  def __ge__(self, other):
    return BinOp(Gte, self, other)

  def __gt__(self, other):
    return BinOp(Gt, self, other)

  def __le__(self, other):
    return BinOp(Lte, self, other)

  def __lt__(self, other):
    return BinOp(Lt, self, other)

  def __truediv__(self, other):
    return BinOp(Div, self, other)

  def __floordiv__(self, other):
    return BinOp(Div, self, other)


class Sizeof(Expression):
  def __init__(self, base_type):
    self.base_type = base_type

  def add_includes(self, program):
    self.base_type.add_includes(program)

  def to_c_string(self, root=False):
    return f"sizeof({self.base_type.to_c_string()})"


class UnOp(Expression):
  def __init__(self, base_expression, op):
    if not isinstance(base_expression, Expression):
      raise RuntimeError(f"Unary operation \"{op.s}\" must be applied to an Expression.  Got {base_expression}.")
    self.base_expression = base_expression
    self.op = op

  def add_includes(self, program):
    self.base_expression.add_includes(program)

  def to_c_string(self, root=False):
    if root:
      return f"{self.op}{self.base_expression.to_c_string()}"
    else:
      return f"({self.op}{self.base_expression.to_c_string()})"


class Constant(Expression):
  def __init__(self, s):
    self.s = str(s)

  def add_includes(self, program):
    pass

  def to_c_string(self, root=False):
    return self.s


true = Constant(1)
false = Constant(0)


class StrConstant(Expression):
  def __init__(self, s):
    self.s = s

  def add_includes(self, program):
    pass

  def to_c_string(self, root=False):
    return f'"{escape_c(self.s)}"'


class Call(Expression):
  def __init__(self, func, args):
    if not isinstance(func, Expression):
      raise RuntimeError(f"Function argument in call was not an expression. Got {func}")
    for i, arg in enumerate(args):
      if not isinstance(arg, Expression):
        raise RuntimeError(f"Argument {i} in call was not an expression. Got {arg}")
    self.func = func
    self.args = args

  def add_includes(self, program):
    self.func.add_includes(program)
    for arg in self.args:
      arg.add_includes(program)

  def to_c_string(self, root=False):
    args = [arg.to_c_string(root=True) for arg in self.args]
    return f"{self.func.to_c_string()}({', '.join(args)})"


class BinOp(Expression):
  def __init__(self, op, left, right):
    self.op = op
    if not isinstance(left, Expression):
      raise RuntimeError(f"Left argument in binary operation \"{op.s}\" was not an expression. Got {left}")
    if not isinstance(right, Expression):
      raise RuntimeError(f"Right argument in binary operation \"{op.s}\" was not an expression. Got {right}")
    self.left = left
    self.right = right

  def add_includes(self, program):
    self.left.add_includes(program)
    self.right.add_includes(program)

  def to_c_string(self, root=False):
    if root:
      return f"{self.left.to_c_string()} {self.op.to_c_string()} {self.right.to_c_string()}"
    else:
      return f"({self.left.to_c_string()} {self.op.to_c_string()} {self.right.to_c_string()})"


class Operation(object):
  def __init__(self, s):
    self.s = s

  def to_c_string(self):
    return self.s


And = Operation("&&")
Or = Operation("||")
Xor = Operation("^")
Plus = Operation("+")
Minus = Operation("-")
Times = Operation("*")
Mod = Operation("%")
Div = Operation("/")
Lt = Operation("<")
Lte = Operation("<=")
Gt = Operation(">")
Gte = Operation(">=")
Equal = Operation("==")
NotEqual = Operation("!=")


class _NULL(Expression):
  def add_includes(self, program):
    pass

  def to_c_string(self, root=False):
    return "NULL"


NULL = _NULL()


class Var(Expression):
  def __init__(self, name, type):
    self.name = name
    self.type = type

  def add_includes(self, program):
    self.type.add_includes(program)

  def to_c_string(self, root=False):
    return self.name


PyArg_ParseTuple = Var("PyArg_ParseTuple", None)
PyLong_FromLong = Var("PyLong_FromLong", None)
