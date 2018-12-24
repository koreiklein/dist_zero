from . import expression


class CType(object):
  def add_includes(self, program):
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  def wrap_variable(self, varname):
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  def parsing_format_string(self):
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  def to_c_string(self):
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  def Sizeof(self):
    return expression.Sizeof(self)

  def Star(self):
    return Star(self)

  def Array(self, n=None):
    return Array(self, n=n)

  def KVec(self):
    return KVec(self)


class KVec(CType):
  def __init__(self, base_type):
    self.base_type = base_type

  def add_includes(self, program):
    program.includes.add('"kvec.h"')

  def wrap_variable(self, varname):
    return f"kvec_t({self.base_type.to_c_string()}) {varname}"

  def parsing_format_string(self):
    raise RuntimeError(f"Unable to produce a PyArg_ParseTuple format string for {self.to_c_string()}")

  def to_c_string(self):
    return f'kvec_t({self.base_type.to_c_string()})'


class Array(CType):
  def __init__(self, base_type, n):
    self.base_type = base_type
    self.n = n

  def add_includes(self, program):
    self.base_type.add_includes(program)

  def _bracket(self):
    if self.n is None:
      return '[]'
    else:
      return f'[{self.n.to_c_string(root=True)}]'

  def wrap_variable(self, varname):
    return self.base_type.wrap_variable(f"({varname}){self._bracket()}")

  def parsing_format_string(self):
    raise RuntimeError(f"Unable to produce a PyArg_ParseTuple format string for {self.to_c_string()}")

  def to_c_string(self):
    return f'{self.base_type.to_c_string()} {self._bracket()}'


class Star(CType):
  def __init__(self, base_type):
    self.base_type = base_type

  def add_includes(self, program):
    self.base_type.add_includes(program)

  def wrap_variable(self, varname):
    return self.base_type.wrap_variable(f"*({varname})")

  def parsing_format_string(self):
    if self.base_type == Char:
      return "w"
    else:
      raise RuntimeError(f"Unable to produce a PyArg_ParseTuple format string for {self.to_c_string()}")

  def to_c_string(self):
    return f'{self.base_type.to_c_string()} *'


class SimpleCType(CType):
  def __init__(self, name, format_string=None):
    self.name = name
    self.format_string = format_string

  def __str__(self):
    return self.name

  def wrap_variable(self, varname):
    return f"{self.name} {varname}"

  def parsing_format_string(self):
    if self.format_string is None:
      raise RuntimeError(f"No PyArg_ParseTuple format string specified for {self.to_c_string}")
    else:
      return self.format_string

  def to_c_string(self):
    return self.name

  def add_includes(self, program):
    pass


Void = SimpleCType('void')
PyObject = SimpleCType('PyObject')
Char = SimpleCType('char', format_string='c')
Capn = SimpleCType('struct capn')
Capn_Ptr = SimpleCType('capn_ptr')
Capn_Segment = SimpleCType('struct capn_segment')


class Int(CType):
  def __init__(self, nbits, format_string=None):
    self.nbits = nbits
    self.format_string = format_string

  def add_includes(self, program):
    program.includes.add("<stdint.h>")

  def parsing_format_string(self):
    if self.format_string is None:
      raise RuntimeError(f"No PyArg_ParseTuple format string specified for {self.to_c_string}")
    else:
      return self.format_string

  def wrap_variable(self, varname):
    return f"{self.to_c_string()} {varname}"

  def to_c_string(self):
    return f"int{self.nbits}_t"


class BasicType(CType):
  def __init__(self, s):
    self.s = s

  def add_includes(self, program):
    pass

  def parsing_format_string(self):
    raise RuntimeError(f"No PyArg_ParseTuple format string specified for {self.to_c_string}")

  def wrap_variable(self, varname):
    return f"{self.to_c_string()} {varname}"

  def to_c_string(self):
    return self.s


Void = BasicType('void')
Bool = BasicType('bool')
MachineInt = BasicType('int')

Int8 = Int(8)
Int16 = Int(16, format_string='h')
Int32 = Int(32, format_string='i')
Int64 = Int(64, format_string='l')

UInt8 = BasicType('uint8_t')
UInt16 = BasicType('uint16_t')
UInt32 = BasicType('uint32_t')
UInt64 = BasicType('uint64_t')