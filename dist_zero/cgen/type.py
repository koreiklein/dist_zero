class CType(object):
  def add_includes(self, program):
    raise NotImplementedError()

  def wrap_variable(self, varname):
    raise NotImplementedError()

  def Star(self):
    return Star(self)


class Star(CType):
  def __init__(self, base_type):
    self.base_type = base_type

  def wrap_variable(self, varname):
    return self.base_type.wrap_variable(f"*({varname})")

  def to_c_string(self):
    return f'{self.base_type.to_c_string()} *'


class SimpleCType(CType):
  def __init__(self, name):
    self.name = name

  def wrap_variable(self, varname):
    return f"{self.name} {varname}"

  def to_c_string(self):
    return self.name


Void = SimpleCType('void')
PyObject = SimpleCType('PyObject')


class Int(CType):
  def __init__(self, nbits):
    self.nbits = nbits

  def add_includes(self, program):
    program.includes.add("<stdint.h>")

  def wrap_variable(self, varname):
    return f"{self.to_c_string()} {varname}"

  def to_c_string(self):
    return f"int{self.nbits}_t"


Int8 = Int(8)
Int16 = Int(16)
Int32 = Int(32)
Int64 = Int(64)
