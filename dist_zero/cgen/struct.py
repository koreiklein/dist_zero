from .type import CType
from .common import INDENT


class Structure(CType):
  def __init__(self, name, is_pyobject=False):
    self.name = name
    self.fields = []
    self.is_pyobject = is_pyobject

  def add_includes(self, program):
    for name, type in self.fields:
      type.add_includes(program)

  def wrap_variable(self, varname):
    return f"struct {self.name} {varname}"

  def parsing_format_string(self):
    raise RuntimeError("No format string to parse a structure.")

  def to_c_string(self):
    return f"struct {self.name}"

  def to_c_string_definition(self):
    yield f'{self.to_c_string()} {{\n'
    if self.is_pyobject:
      yield f"{' ' * INDENT}PyObject_HEAD\n"
    for name, type in self.fields:
      yield f"{' ' * INDENT}{type.wrap_variable(name)};\n"
    yield '};\n'

  def AddField(self, name, type):
    self.fields.append((name, type))


class Enum(CType):
  def __init__(self, name):
    self.name = name
    self.options = []
    self.removed = False

  def AddOption(self, key):
    self.options.append(key)

  def RemoveIfEmpty(self):
    if not self.options:
      self.removed = True
      return True
    return False

  def add_includes(self, program):
    pass

  def to_c_string(self):
    return f"enum {self.name}"

  def to_c_string_definition(self):
    if self.removed:
      return
    yield f'{self.to_c_string()} {{\n'
    for i, name in enumerate(self.options):
      yield f"{' ' * INDENT}{self.name}_option_{name} = {i},\n"
    yield '};\n'

  def wrap_variable(self, varname):
    return f"enum {self.name} {varname}"

  def parsing_format_string(self):
    raise RuntimeError("No format string to parse an enum.")


class Union(CType):
  def __init__(self, name):
    self.name = name
    self.fields = []
    self.removed = False

  def add_includes(self, program):
    for name, type in self.fields:
      type.add_includes(program)

  def RemoveIfEmpty(self):
    if not self.fields:
      self.removed = True
      return True
    return False

  def to_c_string(self):
    return f"union {self.name}"

  def to_c_string_definition(self):
    if self.removed:
      return

    yield f'{self.to_c_string()} {{\n'
    for name, type in self.fields:
      yield f"{' ' * INDENT}{type.wrap_variable(name)};\n"
    yield '};\n'

  def wrap_variable(self, varname):
    return f"union {self.name} {varname}"

  def parsing_format_string(self):
    raise RuntimeError("No format string to parse a union.")

  def AddField(self, key, field_type):
    self.fields.append((key, field_type))
