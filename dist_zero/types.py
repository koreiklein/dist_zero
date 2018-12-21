import math
import random

from dist_zero import capnpgen, errors, cgen

rand = random.Random("types")

lowerChars = 'pyfgcrlaoeuidhtnsqjkxbmwvz'
chars = lowerChars + lowerChars.upper()
n_names = [0]


def _gen_name():
  result = f"{''.join(rand.choice(chars) for i in range(6))}{n_names[0]}"
  n_names[0] += 1
  return result


class Type(object):
  def _write_c_state_definition(self, compiler):
    '''Add a definition for the c states of this type.'''
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  def _write_capnp_state_definition(self, compiler):
    '''Add a definition for the capnp states of this type.'''
    raise RuntimeError("Abstract Superclass")

  def _capnp_transitions_structure_name(self):
    '''return the name to use for the capnp structure with this type's transitions..'''
    raise RuntimeError('Abstract Superclass')

  def _c_transitions_structure_name(self):
    return self._capnp_transitions_structure_name()

  def _write_capnp_transition_definition(self, compiler, ident, union):
    '''Add a definition for the capnp transitions of this type matching ``ident`` to the capnp union object.'''
    raise RuntimeError('Abstract Superclass')

  def _write_c_transition_definition(self, compiler, ident, union, enum):
    '''Add a definition for the c transitions of this type matching ``ident`` to the c union and enum objects.'''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def equivalent(self, other):
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def generate_c_state_to_capnp(self, compiler, block, stateRvalue):
    '''
    Generate code in ``block`` to write the capnp data from ``stateRValue``.
    Return the c expression with the capnp bytes data.
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def generate_capnp_to_c_state(self, compiler, block, capn_ptr_input, output_lvalue):
    '''
    Generate code in ``block`` to write to the ``output_lvalue`` state using the ``capn_ptr_input``
    capn_ptr variable containing the input capnp buffer.
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def __init__(self):
    self.transition_identifiers = set(['standard'])

  def With(self, *transition_identifiers):
    for ti in transition_identifiers:
      self.transition_identifiers.add(ti)

    return self

  def Discrete(self):
    self.transition_identifiers = set()
    return self

  def _write_indiscrete_transition_definition(self, compiler, union):
    union.AddField("jump", compiler.capnp_state_ref(self))

  def _write_indiscrete_c_transition_definition(self, compiler, union, enum):
    union.AddField("jump", compiler.c_state_ref(self).Star())
    enum.AddOption('jump')

  def _write_capnp_transition_identifiers(self, compiler, union):
    for ti in self.transition_identifiers:
      if ti == 'indiscrete':
        self._write_indiscrete_transition_definition(compiler, union)
      else:
        self._write_capnp_transition_definition(compiler, ti, union)

  def _write_c_transition_identifiers(self, compiler, union, enum):
    for ti in self.transition_identifiers:
      if ti == 'indiscrete':
        self._write_indiscrete_c_transition_definition(compiler, union, enum)
      else:
        self._write_c_transition_definition(compiler, ti, union, enum)

  def _write_c_transitions_definition(self, compiler):
    if len(self.transition_identifiers) == 0:
      return cgen.Void
    else:
      struct = compiler.cprogram.AddStruct(self._c_transitions_structure_name())
      union = compiler.cprogram.AddUnion(self._c_transitions_structure_name() + '_union')
      enum = compiler.cprogram.AddEnum(self._c_transitions_structure_name() + '_enum')
      self._write_c_transition_identifiers(compiler, union, enum)

      if union.RemoveIfEmpty():
        enum.RemoveIfEmpty()
      return struct

  def _write_capnp_transitions_definition(self, compiler):
    if len(self.transition_identifiers) == 0:
      return capnpgen.Void
    else:
      struct = compiler.capnp.AddStructure(self._capnp_transitions_structure_name())
      union = struct.AddUnion()
      self._write_capnp_transition_identifiers(compiler, union)

      if union.RemoveIfTooSmall():
        self._write_capnp_transition_identifiers(compiler, struct)

      return self._capnp_transitions_structure_name()


class FunctionType(Type):
  def __init__(self, src, tgt):
    self.src = src
    self.tgt = tgt

  def equivalent(self, other):
    return other.__class__ == FunctionType and self.src.equivalent(other.src) and self.tgt.equivalent(other.tgt)

  def _write_c_state_definition(self, compiler):
    raise RuntimeError("FunctionType should be be compiled to a c state.")

  def _write_capnp_transition_definition(self, compiler, ident, union):
    raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _write_c_transition_definition(self, compiler, ident, union, enum):
    raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _capnp_transitions_structure_name(self):
    raise errors.InternalError("We should not be generating transitions from FunctionTypes.")

  def _write_capnp_state_definition(self, compiler):
    raise errors.InternalError("We should not be generating states from FunctionTypes.")


class BasicType(Type):
  def __init__(self, capnp_state, c_state_type, capnp_transition_type, c_transition_type):
    self.name = f"Basic{_gen_name()}"
    self.capnp_state = capnp_state
    self.c_state_type = c_state_type
    self.capnp_transition_type = capnp_transition_type
    self.c_transition_type = c_transition_type

    self._wrote_capnp_transition_definition = False
    self._wrote_capnp_state_definition = False

    super(BasicType, self).__init__()

  def generate_c_state_to_capnp(self, compiler, block, stateRvalue):
    vCapn = cgen.Var('capn', cgen.Capn)
    vCapnPtr = cgen.Var('msg_ptr', cgen.Capn_Ptr)
    vSegment = cgen.Var('seg', cgen.Capn_Segment.Star())

    stateStructureType = compiler.type_to_capnp_state_type(self)
    myStructure = cgen.Var('structure', stateStructureType)

    block.AddDeclaration(cgen.CreateVar(vCapn))
    block.AddAssignment(None, cgen.capn_init_malloc(vCapn.Address()))
    block.AddAssignment(cgen.CreateVar(vCapnPtr), cgen.capn_root(vCapn.Address()))
    block.AddAssignment(cgen.CreateVar(vSegment), vCapnPtr.Dot('seg'))

    block.Newline()

    block.AddDeclaration(cgen.CreateVar(myStructure))
    block.AddAssignment(cgen.UpdateVar(myStructure).Dot('basicState'), stateRvalue)

    writeStructure = compiler.type_to_capnp_state_write_function(self)
    newPtr = compiler.type_to_capnp_state_new_ptr_function(self)
    ptr = cgen.Var('ptr', compiler.type_to_capnp_state_ptr(self))

    block.AddAssignment(cgen.CreateVar(ptr), newPtr(vSegment))
    block.AddAssignment(None, writeStructure(myStructure.Address(), ptr))

    (block.AddIf(cgen.Zero != cgen.capn_setp(vCapnPtr, cgen.Zero, ptr.Dot('p'))).consequent.AddAssignment(
        None, compiler.pyerr_from_string("Failed to capn_setp for root when producing output.")).AddReturn(cgen.NULL))

    block.Newline()

    pythonBytesFromCapn = cgen.Var(compiler._python_bytes_from_capn_function_name(), None)
    return pythonBytesFromCapn(vCapn.Address())

  def generate_capnp_to_c_state(self, compiler, block, capn_ptr_input, output_lvalue):
    ptr = cgen.Var('ptr', compiler.type_to_capnp_state_ptr(self))

    structure = cgen.Var('parsed_structure', compiler.type_to_capnp_state_type(self))
    block.AddDeclaration(cgen.CreateVar(structure))
    block.AddDeclaration(cgen.CreateVar(ptr))
    block.Newline()

    readStructure = compiler.type_to_capnp_state_read_function(self)

    block.AddAssignment(cgen.UpdateVar(ptr).Dot('p'), capn_ptr_input)
    block.AddAssignment(None, readStructure(structure.Address(), ptr))

    block.AddAssignment(output_lvalue, structure.Dot('basicState'))

    block.Newline()

  def _write_c_transitions_definition(self, compiler):
    return self.c_transition_type

  def _write_c_state_definition(self, compiler):
    return self.c_state_type

  def _capnp_transitions_structure_name(self):
    return f"{self.name}Transitions"

  def equivalent(self, other):
    return other.__class__ == BasicType and \
        self.capnp_state == other.capnp_state and \
        self.c_state_type == other.c_state_type and \
        self.capnp_transition_type == other.capnp_transition_type and \
        self.c_transition_type == other.c_transition_type

  def _write_c_transition_definition(self, compiler, ident, union, enum):
    pass

  def _write_capnp_transition_definition(self, compiler, ident, union):
    union.AddField('basicTransition', self.capnp_transition_type)

  def _write_capnp_state_definition(self, compiler):
    if not self._wrote_capnp_state_definition:
      self._wrote_capnp_state_definition = True
      struct = compiler.capnp.AddStructure(self.name)
      struct.AddField('basicState', self.capnp_state)
    return self.name


Int8 = BasicType('Int8', c_state_type=cgen.Int8, capnp_transition_type='Int8', c_transition_type=cgen.Int8)
Int16 = BasicType('Int16', c_state_type=cgen.Int16, capnp_transition_type='Int16', c_transition_type=cgen.Int16)
Int32 = BasicType('Int32', c_state_type=cgen.Int32, capnp_transition_type='Int32', c_transition_type=cgen.Int32)
Int64 = BasicType('Int64', c_state_type=cgen.Int64, capnp_transition_type='Int64', c_transition_type=cgen.Int64)
UInt8 = BasicType('UInt8', c_state_type=cgen.UInt8, capnp_transition_type='UInt8', c_transition_type=cgen.UInt8)
UInt16 = BasicType('UInt16', c_state_type=cgen.UInt16, capnp_transition_type='UInt16', c_transition_type=cgen.UInt16)
UInt32 = BasicType('UInt32', c_state_type=cgen.UInt32, capnp_transition_type='UInt32', c_transition_type=cgen.UInt32)
UInt64 = BasicType('UInt64', c_state_type=cgen.UInt64, capnp_transition_type='UInt64', c_transition_type=cgen.UInt64)


class Product(Type):
  def __init__(self, items, name=None):
    self.items = items
    self.d = dict(items)
    if len(self.items) != len(self.d):
      raise RuntimeError("Duplicate key detected in Product.")
    self.name = name if name is not None else f"Product{_gen_name()}"

    self._wrote_capnp_simulatenous = False
    self._wrote_capnp_individual = False
    self._wrote_c_simultaneous = False
    self._wrote_c_individual = False

    super(Product, self).__init__()

  def equivalent(self, other):
    if other.__class__ != Product or len(self.items) != len(other.items):
      return False

    for k, v in self.items:
      if not v.equivalent(other.d[k]):
        return False

    return True

  def _capnp_transitions_structure_name(self):
    return f"{self.name}Transition"

  def _write_c_simultaneous_components_transition_definitions(self, compiler, union, enum):
    if not self._wrote_c_simultaneous:
      self._wrote_c_simultaneous = True
      struct = compiler.cprogram.AddStruct(f"{self.name}_simultaneous")
      union.AddField('simultaneous', struct.Star())
      enum.AddOption('simultaneous')
      for key, value in self.items:
        struct.AddField(key, compiler.c_transitions_ref(value).Star())

  def _write_capnp_simultaneous_components_transition_definitions(self, compiler, union):
    if not self._wrote_capnp_simulatenous:
      self._wrote_capnp_simulatenous = True
      struct = compiler.capnp.AddStructure(f"{self.name}Simultaneous")
      union.AddField("simultaneous", struct)

      for key, value in self.items:
        struct.AddField(key, compiler.capnp_transitions_ref(value))

  def _write_c_individual_components_transition_definitions(self, compiler, union, enum):
    if not self._wrote_capnp_individual:
      self._wrote_capnp_individual = True
      for key, value in self.items:
        union.AddField(f'product_on_{key}', compiler.c_transitions_ref(value).Star())
        enum.AddOption(f'product_on_{key}')

  def _write_capnp_individual_components_transition_definitions(self, compiler, union):
    if not self._wrote_capnp_individual:
      self._wrote_capnp_individual = True
      for key, value in self.items:
        union.AddField(f"productOn{key}", compiler.capnp_transitions_ref(value))

  def _write_capnp_transition_definition(self, compiler, ident, union):
    if ident == 'standard':
      self._write_capnp_individual_components_transition_definitions(compiler, union)
      self._write_capnp_simultaneous_components_transition_definitions(compiler, union)
    elif ident == 'individual':
      self._write_capnp_individual_components_transition_definitions(compiler, union)
    elif ident == 'simultaneous':
      self._write_capnp_simultaneous_components_transition_definitions(compiler, union)
    else:
      raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _write_c_transition_definition(self, compiler, ident, union, enum):
    if ident == 'standard':
      self._write_c_individual_components_transition_definitions(compiler, union, enum)
      self._write_c_simultaneous_components_transition_definitions(compiler, union, enum)
    elif ident == 'individual':
      self._write_c_individual_components_transition_definitions(compiler, union, enum)
    elif ident == 'simultaneous':
      self._write_c_simultaneous_components_transition_definitions(compiler, union, enum)
    else:
      raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def __abs__(self):
    result = 1
    for k, x in self.items:
      result *= abs(x)
    return result

  def _write_c_state_definition(self, compiler):
    struct = compiler.cprogram.AddStruct(self.name)
    for key, value in self.items:
      struct.AddField(key, compiler.c_state_ref(value).Star())

    return struct

  def _write_capnp_state_definition(self, compiler):
    struct = compiler.capnp.AddStructure(self.name)
    for key, value in self.items:
      struct.AddField(key, compiler.capnp_state_ref(value))

    return self.name


class Sum(Type):
  def __init__(self, items, name=None):
    self.items = items
    self.d = dict(items)
    self.name = name if name is not None else f"Sum{_gen_name()}"
    super(Sum, self).__init__()

  def _capnp_transitions_structure_name(self):
    return f"{self.name}Transition"

  def _write_capnp_individual_components_transition_definitions(self, compiler, union):
    for key, value in self.items:
      union.AddField(f"sumOn{key}", compiler.capnp_transitions_ref(value))

  def _write_c_individual_components_transition_definitions(self, compiler, union, enum):
    for key, value in self.items:
      union.AddField(f"sum_on_{key}", compiler.c_transitions_ref(value).Star())
      enum.AddOption(f"sum_on_{key}")

  def _write_capnp_transition_definition(self, compiler, ident, union):
    if ident == 'standard':
      self._write_capnp_individual_components_transition_definitions(compiler, union)
    else:
      raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _write_c_transition_definition(self, compiler, ident, union, enum):
    if ident == 'standard':
      self._write_c_individual_components_transition_definitions(compiler, union, enum)
    else:
      raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _write_capnp_state_definition(self, compiler):
    struct = compiler.capnp.AddStructure(self.name)
    if len(self.items) == 0:
      pass
    elif len(self.items) == 0:
      for key, value in self.items:
        struct.AddField(key, compiler.capnp_state_ref(value))
    else:
      union = struct.AddUnion()
      for key, value in self.items:
        union.AddField(key, compiler.capnp_state_ref(value))

    return self.name

  def _write_c_state_definition(self, compiler):
    struct = compiler.cprogram.AddStruct(self.name)
    if len(self.items) == 0:
      pass
    else:
      union = compiler.cprogram.AddUnion(self.name + '_union')
      enum = compiler.cprogram.AddEnum(self.name + '_enum')

      struct.AddField('type', enum)
      struct.AddField('value', union)

      for key, value in self.items:
        enum.AddOption(key)
        union.AddField(key, compiler.c_state_ref(value).Star())

    return struct

  def __abs__(self):
    return sum(abs(x) for k, x in self.items)


class List(Type):
  def __init__(self, base, name=None):
    self.name = name if name is not None else f"List{_gen_name()}"
    self.base = base
    super(List, self).__init__()

  def _write_c_state_definition(self, compiler):
    return compiler.c_state_ref(self.base).Star().KVec()

  def _capnp_transitions_structure_name(self):
    return f"{self.name}Transition"

  def _write_c_append_transition_definition(self, compiler, union, enum):
    union.AddField('append', compiler.c_state_ref(self.base).Star())

  def _write_c_insert_transition_definition(self, compiler, union, enum):
    insertStruct = compiler.cprogram.AddStruct(f"insert_in_{self.name}")
    insertStruct.AddField('index', cgen.UInt32)
    insertStruct.AddField('value', compiler.c_state_ref(self.base))
    union.AddField('insert', insertStruct.Star())
    enum.AddOption('insert')

  def _write_append_transition_definition(self, compiler, union):
    union.AddField('append', compiler.capnp_state_ref(self.base))

  def _write_insert_transition_definition(self, compiler, union):
    insertStruct = compiler.capnp.AddStructure(f"InsertIn{self.name}")
    insertStruct.AddField('index', capnpgen.UInt32)
    insertStruct.AddField('value', compiler.capnp_state_ref(self.base))

    union.AddField('insert', insertStruct)

  def _write_c_on_index_transition_definition(self, compiler, union, enum):
    onIndexStruct = compiler.cprogram.AddStruct(f"on_index_{self.name}")
    onIndexStruct.AddField('index', cgen.UInt32)
    onIndexStruct.AddField('transition', compiler.c_transitions_ref(self.base).Star())

    union.AddField('on_index', onIndexStruct.Star())
    enum.AddOption('on_index')

  def _write_on_index_transition_definition(self, compiler, union):
    onIndexStruct = compiler.capnp.AddStructure(f"OnIndex{self.name}")
    onIndexStruct.AddField('index', capnpgen.UInt32)
    onIndexStruct.AddField('transition', compiler.capnp_transitions_ref(self.base))

    union.AddField('onIndex', onIndexStruct)

  def _write_c_remove_transition_definition(self, compiler, union, enum):
    union.AddField('remove', cgen.UInt32)

  def _write_remove_transition_definition(self, compiler, union):
    union.AddField('remove', capnpgen.UInt32)

  def _write_capnp_transition_definition(self, compiler, ident, union):
    if ident == 'standard':
      pass
    elif ident == 'append':
      self._write_append_transition_definition(compiler, union)
    elif ident == 'insert':
      self._write_insert_transition_definition(compiler, union)
    elif ident == 'remove':
      self._write_remove_transition_definition(compiler, union)
    elif ident == 'onIndex':
      self._write_on_index_transition_definition(compiler, union)
    else:
      raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _write_c_transition_definition(self, compiler, ident, union, enum):
    if ident == 'standard':
      pass
    elif ident == 'append':
      self._write_c_append_transition_definition(compiler, union, enum)
    elif ident == 'insert':
      self._write_c_insert_transition_definition(compiler, union, enum)
    elif ident == 'remove':
      self._write_c_remove_transition_definition(compiler, union, enum)
    elif ident == 'onIndex':
      self._write_c_on_index_transition_definition(compiler, union, enum)
    else:
      raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _write_capnp_state_definition(self, compiler):
    return f"List({compiler.capnp_state_ref(self.base)})"

  def __abs__(self):
    if abs(self.base) == 0:
      return 1
    else:
      return math.inf


Unit = Product([], name="Unit")


def sum_i(i, name):
  return Sum([(f"field{x}", Unit) for x in range(i)], name=name)


Zero, One, Two, Three, Four, Five = [
    sum_i(i, name=name) for i, name in enumerate(['Zero', 'One', 'Two', 'Three', 'Four', 'Five'])
]
