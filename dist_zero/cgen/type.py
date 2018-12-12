class CType(object):
  def add_includes(self, program):
    raise NotImplementedError()

  def wrap_variable(self, varname):
    raise NotImplementedError()

  def parsing_format_string(self):
    raise NotImplementedError()

  def Star(self):
    return Star(self)


class Star(CType):
  def __init__(self, base_type):
    self.base_type = base_type

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

  def wrap_variable(self, varname):
    return f"{self.name} {varname}"

  def parsing_format_string(self):
    if self.format_string is None:
      raise RuntimeError(f"No PyArg_ParseTuple format string specified for {self.to_c_string}")
    else:
      return self.format_string

  def to_c_string(self):
    return self.name


Void = SimpleCType('void')
PyObject = SimpleCType('PyObject')
Char = SimpleCType('char', format_string='c')


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


Int8 = Int(8)
Int16 = Int(16, format_string='h')
Int32 = Int(32, format_string='i')
Int64 = Int(64, format_string='l')
