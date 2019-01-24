from dist_zero import errors

from .common import INDENT, escape_c_string
from . import lvalue
from .type import CType


class Expression(object):
  '''A C expression'''

  def add_includes(self, program):
    raise NotImplementedError()

  def to_c_string(self, root=False):
    raise NotImplementedError()

  def toLValue(self):
    '''
    If this expression can be used as an lvalue, return an `Lvalue` instance representing it.
    Otherwise, raise an appropriate error.
    '''
    raise errors.InternalError(f"Expression can't be used in assignment \"{self}\".")

  def __str__(self):
    return self.to_c_string()

  def __repr__(self):
    return str(self)

  def Cast(self, type):
    return Cast(self, type)

  def Deref(self):
    return self.to_component_rvalue().Deref()

  def Dot(self, name):
    return self.to_component_rvalue().Dot(name)

  def Arrow(self, name):
    return self.to_component_rvalue().Arrow(name)

  def Sub(self, i):
    if not isinstance(i, Expression):
      raise errors.InternalError(f"Sub: Expected an Expression, got {i}")
    return self.to_component_rvalue().Sub(i)

  def to_component_rvalue(self):
    return ComponentRvalue(self, [])

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


class UnionLiteral(Expression):
  '''A C union literal expression'''

  def __init__(self, union, key, value):
    self.union = union
    self.key = key
    self.value = value

  def add_includes(self, program):
    self.value.add_includes(program)

  def to_c_string(self, root=False):
    return f"(({self.union.to_c_string()}) {{ .{self.key}={self.value.to_c_string()} }})"


class StructureLiteral(Expression):
  '''A C structure literal expression'''

  def __init__(self, struct, key_to_expr):
    self.struct = struct
    self.key_to_expr = key_to_expr

  def add_includes(self, program):
    for expr in self.key_to_expr.values():
      expr.add_includes(program)

  def to_c_string(self, root=False):
    assignments = ", ".join(f"\n    .{key}={expr.to_c_string()}" for key, expr in self.key_to_expr.items())
    return f"(({self.struct.to_c_string()}) {{ {assignments} }})"


class Sizeof(Expression):
  '''The C sizeof operator'''

  def __init__(self, base_type):
    self.base_type = base_type

  def add_includes(self, program):
    self.base_type.add_includes(program)

  def to_c_string(self, root=False):
    return f"sizeof({self.base_type.to_c_string()})"


class ComponentRvalue(Expression):
  '''A C expression with syntactic accessors (e.g. ".", "->", "[3]", "*") applied to it'''

  def __init__(self, base, accessors):
    self.base = base
    self.accessors = accessors

  def toLValue(self):
    return self.base.toLValue().to_component_lvalue(accessors=self.accessors)

  def _extend(self, accessor):
    accessors = list(self.accessors)
    accessors.append(accessor)
    return ComponentRvalue(base=self.base, accessors=accessors)

  def Deref(self):
    return self._extend(lvalue.Deref)

  def Dot(self, name):
    return self._extend(lvalue.Dot(name))

  def Arrow(self, name):
    return self._extend(lvalue.Arrow(name))

  def Sub(self, i):
    if not isinstance(i, Expression):
      raise errors.InternalError(f"Sub: Expected an Expression, got {i}")
    return self._extend(lvalue.Sub(i))

  def add_includes(self, program):
    self.base.add_includes(program)

  def to_c_string(self, root=False):
    result = self.base.to_c_string(root=True)
    for accessor in self.accessors:
      result = accessor.access_variable(result)

    return result


class UnOp(Expression):
  '''A unary C operator applied to a C expression'''

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
  '''A predefined C constant'''

  def __init__(self, s):
    self.s = str(s)

  def add_includes(self, program):
    pass

  def to_c_string(self, root=False):
    return self.s


true = Constant(1)
false = Constant(0)

Zero, One, Two, Three, Four, Five = [Constant(i) for i in range(6)]
MinusOne = Constant(-1)


class StrConstant(Expression):
  '''A predefined C string constant'''

  def __init__(self, s):
    self.s = s

  def add_includes(self, program):
    pass

  def to_c_string(self, root=False):
    return escape_c_string(self.s)


class Call(Expression):
  '''A C function call expression'''

  def __init__(self, func, args):
    if not isinstance(func, Expression):
      raise RuntimeError(f"Function argument in call was not an expression. Got {func}")
    for i, arg in enumerate(args):
      if not isinstance(arg, Expression) and not isinstance(arg, CType):
        raise RuntimeError(f"Argument {i} in call was not an expression or Type. Got {arg}")
    self.func = func
    self.args = args

  def add_includes(self, program):
    self.func.add_includes(program)
    for arg in self.args:
      arg.add_includes(program)

  def to_c_string(self, root=False):
    args = [arg.to_c_string() for arg in self.args]
    return f"{self.func.to_c_string()}({', '.join(args)})"


class BinOp(Expression):
  '''A C binary operator applied to two argument expressions'''

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
  '''A C binary operator'''

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
  '''The special C NULL expression'''

  def add_includes(self, program):
    pass

  def to_c_string(self, root=False):
    return "NULL"


NULL = _NULL()


class Cast(Expression):
  '''A C typecast expression'''

  def __init__(self, base, type):
    self.base = base
    self.type = type

  def toLValue(self):
    return Cast(base=self.base.toLValue(), type=self.type)

  def add_includes(self, program):
    self.base.add_includes(program)
    self.type.add_includes(program)

  def to_c_string(self, root=False):
    if root:
      return f"({self.type.to_c_string()}) {self.base.to_c_string(root=False)}"
    else:
      return f"(({self.type.to_c_string()}) {self.base.to_c_string(root=False)})"


class Var(Expression):
  '''A C variable'''

  def __init__(self, name, type=None):
    self.name = name
    self.type = type

  def toLValue(self):
    return lvalue.UpdateVar(self)

  def __str__(self):
    return f"Var(\"{self.name}\": {self.type})"

  def add_includes(self, program):
    if self.type is not None:
      self.type.add_includes(program)

  def to_c_string(self, root=False):
    return self.name


class LoopVar(object):
  def __init__(self, block, var, limit):
    self.block = block
    self.var = var
    self.limit = limit
    self.loop = None

  def __enter__(self):
    self.block.AddDeclaration(self.var, Zero)
    self.loop = self.block.AddWhile(self.var < self.limit)
    return self.loop, self.var

  def __exit__(self, type, value, traceback):
    self.loop.AddAssignment(self.var, self.var + One)


Py_DECREF = Var("Py_DECREF", None)
Py_XDECREF = Var("Py_XDECREF", None)
Py_INCREF = Var("Py_INCREF", None)
Py_XINCREF = Var("Py_XINCREF", None)

Py_None = Var("Py_None")

PyArg_ParseTuple = Var("PyArg_ParseTuple", None)
PyLong_FromLong = Var("PyLong_FromLong", None)
PyBool_FromLong = Var("PyBool_FromLong", None)

PyDict_New = Var('PyDict_New', None)
PyDict_SetItemString = Var('PyDict_SetItemString', None)
PyDict_Next = Var('PyDict_Next', None)

PyList_Size = Var('PyList_Size', None)
PyList_GetItem = Var('PyList_GetItem', None)

PyBytes_FromString = Var('PyBytes_FromString', None)
PyBytes_FromStringAndSize = Var('PyBytes_FromStringAndSize', None)
PyBytes_AsStringAndSize = Var('PyBytes_AsStringAndSize', None)

PyUnicode_CompareWithASCIIString = Var('PyUnicode_CompareWithASCIIString', None)

PyMemoryView_FromMemory = Var('PyMemoryView_FromMemory', None)

PyExc_RuntimeError = Var('PyExc_RuntimeError', None)
PyErr_SetString = Var('PyErr_SetString', None)
PyErr_Format = Var('PyErr_Format', None)

calloc = Var("calloc", None)
malloc = Var("malloc", None)
free = Var("free", None)
capn_init_mem = Var("capn_init_mem", None)
capn_write_mem = Var("capn_write_mem", None)
capn_getp = Var("capn_getp", None)
capn_len = Var("capn_len", None)
capn_setp = Var("capn_setp", None)
capn_root = Var("capn_root", None)
capn_resolve = Var("capn_resolve", None)
capn_init_malloc = Var("capn_init_malloc", None)
capn_free = Var("capn_free", None)

printf = Var("printf", None)

kv_init = Var('kv_init', None)
kv_push = Var('kv_push', None)
kv_size = Var('kv_size', None)
kv_A = Var('kv_A', None)
kv_destroy = Var('kv_destroy', None)

queue_push = Var('queue_push', None)
queue_pop = Var('queue_pop', None)

event_queue_init = Var('event_queue_init', None)
event_queue_push = Var('event_queue_push', None)
event_queue_pop = Var('event_queue_pop', None)
