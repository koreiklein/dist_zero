'''
This module contains classes for programatically generating a capnproto file,
and obtaining python representations for all the structures and unions within it.
These representations can then be used by compilers operating on a `dist_zero.cgen.program.Program` object
to generate the proper C code to serialize/deserialize to the capnproto structures.

'''
import os
import subprocess

from dist_zero import errors, settings, cgen

INDENT = '  '


class Union(object):
  '''Represents a capnproto union.'''

  def __init__(self, structure, name):
    self.structure = structure
    self.name = name
    self._options = []
    self._removed = False

    self._c_enum_option_by_name = {}

  def c_enum_option_by_name(self, name):
    return self._c_enum_option_by_name[name]

  def AddField(self, name, option_type):
    '''Add a field to the union.'''
    self._c_enum_option_by_name[name] = cgen.Constant(f"{self.structure.name}_{name}")
    self._options.append((name, option_type))

  def RemoveIfTooSmall(self):
    '''Remove the union entirely if it has no fields.'''
    if len(self._options) <= 1:
      self._removed = True
      return True
    else:
      return False

  def lines(self, indent):
    '''yield the `str` lines of the union'''
    if self._removed:
      return

    if self.name is None:
      yield f"{indent}union {{\n"
    else:
      yield f"{indent}{self.name} :union {{\n"

    extra_indent = indent + INDENT
    for name, option_type in self._options:
      yield f"{extra_indent}{name} @{self.structure.next_count()} :{option_type};\n"
    yield f"{indent}}}\n"


class Structure(object):
  '''Represents a capnproto structure.'''

  def __init__(self, name):
    self.name = name
    self._fields = []
    self._field_by_name = {}
    self._unions = []
    self._has_unnamed_union = False

    self.count = 0

    self.c_new_ptr_function = cgen.Var(f"new_{self.name}", None)
    self.c_new_list_function = cgen.Var(f"new_{self.name}_list", None)
    self.c_read_function = cgen.Var(f"read_{self.name}", None)
    self.c_write_function = cgen.Var(f"write_{self.name}", None)

    self.c_ptr_type = cgen.BasicType(f"{self.name}_ptr")
    self.c_list_type = cgen.BasicType(f"{self.name}_list")
    self.c_structure_type = cgen.BasicType(f"struct {self.name}")

  def get_field(self, name):
    '''
    :param str name: The name of a field of this structure.
    :return: The field itself.
    :rtype: `Field`
    '''
    return self._field_by_name[name]

  def __str__(self):
    return self.name

  def next_count(self):
    result = self.count
    self.count += 1
    return result

  def lines(self, indent):
    '''yield the `str` lines of the structure'''
    self.count = 0
    yield f"{indent}struct {self.name} {{\n"

    extra_indent = indent + INDENT
    for field in self._fields:
      yield f"{extra_indent}{field.name} @{self.next_count()} :{field.type};\n"

    if self._fields:
      before_union = "\n"
    else:
      before_union = ""

    for union in self._unions:
      yield before_union
      before_union = "\n"
      for line in union.lines(extra_indent):
        yield line

    yield f"{indent}}}\n\n"

  def AddField(self, name, field_type):
    '''
    Add a new field to the structure.

    :param str name: The name of the field.
    :param str field_type: The type of the field.
    :return: The newly added field.
    :rtype: `Field`
    '''
    result = Field(name=name, type=field_type, base=self)
    self._fields.append(result)
    if name in self._field_by_name:
      raise errors.InternalError(f"Field name \"{name}\" was already added to this structure.")
    self._field_by_name[name] = result
    return result

  def c_set_field(self, field_name):
    '''
    :param str field_name: The name of the field.
    :return: The C variable for the C function that sets that field.
    :rtype: `dist_zero.cgen.expression.Var`
    '''
    field = self._field_by_name[field_name]
    return field.c_set

  def c_get_field(self, field_name):
    '''
    :param str field_name: The name of the field.
    :return: The C variable for the C function that gets that field.
    :rtype: `dist_zero.cgen.expression.Var`
    '''
    field = self._field_by_name[field_name]
    return field.c_get

  def AddUnion(self, name=None):
    '''
    Add a union field to this structure.

    :rtype: `Union`
    '''
    if name is None:
      if self._has_unnamed_union:
        raise errors.CapnpFormatError("This structure already has an unnamed union.")
      else:
        self._has_unnamed_union = True

    result = Union(structure=self, name=name)
    self._unions.append(result)
    return result


class Field(object):
  '''Represents a field of a capnproto structure/union.'''

  def __init__(self, name, type, base):
    self.name = name
    self.type = type
    self._base = base

    self.c_set = cgen.Var(f"{self._base.name}_set_{self.name}", None)
    '''The `cgen.expression.Var` for the C function to set this field.'''
    self.c_get = cgen.Var(f"{self._base.name}_get_{self.name}", None)
    '''The `cgen.expression.Var` for the C function to get this field.'''

    self.c_get_structure_from_list = cgen.Var(f"get_{self._base.name}_{self.name}", None)
    '''For fields that are of a list type, the `cgen.expression.Var` for the C function to get an item in the list at this field.'''


Void = 'Void'
Bool = 'Bool'
Int8 = 'Int8'
Int16 = 'Int16'
Int32 = 'Int32'
Int64 = 'Int64'
UInt8 = 'UInt8'
UInt16 = 'UInt16'
UInt32 = 'UInt32'
UInt64 = 'UInt64'
Float32 = 'Float32'
Float64 = 'Float64'
Text = 'Text'
Data = 'Data'

gen_capn_uid = lambda: subprocess.check_output(['capnpc', '-i']).decode().strip()


class CapnpFile(object):
  '''
  Root object for generating a capnproto file.
  '''

  def __init__(self, capnpid):
    self.id = capnpid
    self._structures = []

  def lines(self):
    '''yield: The `str` lines of the underlying capnproto file.'''
    yield f"{self.id};\n\n"

    imported_file = os.path.join('/', 'c-capnproto', 'compiler', 'c.capnp')
    yield f'using C = import "{imported_file}";\n'
    yield "$C.fieldgetset;\n\n"

    for struct in self._structures:
      for line in struct.lines(''):
        yield line

  def build_in(self, dirname, filename):
    '''
    Write capnproto and c files in dirname.
    The written files will all start with filename and have the appropriate extensions.
    '''
    fullname = os.path.join(dirname, filename)

    with open(fullname, 'w') as f:
      for line in self.lines():
        f.write(line)

    try:
      subprocess.check_output(['capnpc', '-I', settings.CAPNP_DIR, f'-oc', fullname], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
      raise errors.CapnpCompileError(f"Error from capnpc:\n{e.output.decode()}")

  def get_file_contents(self):
    '''
    Return the capnproto file as a string.
    '''
    return ''.join(self.lines())

  def AddStructure(self, name):
    '''
    Add a capnproto structure to this file.

    :return: The newly added structure.
    :rtype: `dist_zero.capnpgen.Structure`
    '''
    result = Structure(name)
    self._structures.append(result)
    return result
