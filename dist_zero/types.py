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

  def generate_apply_transition(self, block, stateLvalue, stateRvalue, transition):
    '''
    Generate code in ``block`` to update a state given a c expression for a transition on that state.
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def generate_capnp_to_c_state(self, compiler, block, capn_ptr_input, output_lvalue):
    '''
    Generate code in ``block`` to write to the ``output_lvalue`` state using the ``capn_ptr_input``
    capn_ptr variable containing the input capnp buffer.
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def generate_capnp_to_c_transition(self, compiler, block, capn_ptr_input, output_kvec):
    '''
    Generate code in ``block`` to append to the ``outptu_kvec`` list of transitions using the ``capn_ptr_input``
    capn_ptr variable containing the input capnp buffer of the transitions.
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def _write_c_state_to_capnp_ptr(self, compiler, block, vSegment, stateRvalue, vPtr):
    '''
    Write the state associated with ``stateRvalue`` into the capnp pointer vPtr.

    :param compiler: The compiler working on the current program.
    :type compiler: `ReactiveCompiler`
    :param block: The current code block in which to generate the code.
    :type block: `Block`
    :param stateRvalue: An expression for the rvalue giving a c state of the type of self.
    :type stateRvalue: `dist_zero.cgen.Expression`
    :param vPtr: The c variable defining the capnp pointer to which to write the result.
    :type vPtr: `dist_zero.cgen.Var`
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def _write_c_transitions_to_capnp_ptr(self, compiler, block, vSegment, transitionsRvalue, vPtr):
    '''
    Write the transitions associated with ``transitionsRvalue`` into the capnp pointer vPtr.

    :param compiler: The compiler working on the current program.
    :type compiler: `ReactiveCompiler`
    :param block: The current code block in which to generate the code.
    :type block: `Block`
    :param transitionsRvalue: An expression for the rvalue giving the kvec of c transitions of the c transition type of self.
    :type transitionsRvalue: `dist_zero.cgen.Expression`
    :param vPtr: The c variable defining the capnp pointer to which to write the result.
    :type vPtr: `dist_zero.cgen.Var`
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def __init__(self):
    self.transition_identifiers = set(['standard'])

  def generate_c_transitions_to_capnp(self, compiler, block, transitionsRvalue, result):
    '''
    Generate code in ``block`` to write the capnp data from ``transitionsRValue``.
    :param result: The c lvalue to assign the result to
    '''
    vCapn, vCapnPtr, vSegment = self._init_capn_mem(block)

    vPtr = cgen.Var('ptr', compiler.type_to_capnp_transitions_list_type(self))

    self._write_c_transitions_to_capnp_ptr(compiler, block, vSegment, transitionsRvalue, vPtr)

    self._write_capn_to_python_bytes(compiler, block, vCapn, vCapnPtr, vPtr, result)

  def generate_c_state_to_capnp(self, compiler, block, stateRvalue, result):
    '''
    Generate code in ``block`` to write the capnp data from ``stateRValue``.
    :param result: The c lvalue to assign the result to
    '''
    vCapn, vCapnPtr, vSegment = self._init_capn_mem(block)

    newPtr = compiler.type_to_capnp_state_new_ptr_function(self)
    vPtr = cgen.Var('ptr', compiler.type_to_capnp_state_ptr(self))
    block.AddAssignment(cgen.CreateVar(vPtr), newPtr(vSegment))

    self._write_c_state_to_capnp_ptr(compiler, block, vSegment, stateRvalue, vPtr)

    self._write_capn_to_python_bytes(compiler, block, vCapn, vCapnPtr, vPtr, result)

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
      struct = compiler.cprogram.AddStruct(self._c_transitions_structure_name() + '_c')
      union = compiler.cprogram.AddUnion(self._c_transitions_structure_name() + '_c__union')
      enum = compiler.cprogram.AddEnum(self._c_transitions_structure_name() + '_c__enum')
      self._write_c_transition_identifiers(compiler, union, enum)

      if union.RemoveIfEmpty():
        enum.RemoveIfEmpty()
      else:
        struct.AddField('type', enum)
        struct.AddField('value', union)
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

  def _init_capn_mem(self, block):
    vCapn = cgen.Var('capn', cgen.Capn)
    vCapnPtr = cgen.Var('msg_ptr', cgen.Capn_Ptr)
    vSegment = cgen.Var('seg', cgen.Capn_Segment.Star())

    block.AddDeclaration(cgen.CreateVar(vCapn))
    block.AddAssignment(None, cgen.capn_init_malloc(vCapn.Address()))
    block.AddAssignment(cgen.CreateVar(vCapnPtr), cgen.capn_root(vCapn.Address()))
    block.AddAssignment(cgen.CreateVar(vSegment), vCapnPtr.Dot('seg'))

    block.Newline()

    return vCapn, vCapnPtr, vSegment

  def _write_capn_to_python_bytes(self, compiler, block, vCapn, vCapnPtr, ptr, result):
    (block.AddIf(cgen.Zero != cgen.capn_setp(vCapnPtr, cgen.Zero, ptr.Dot('p'))).consequent.AddAssignment(
        None, compiler.pyerr_from_string("Failed to capn_setp for root when producing output.")).AddAssignment(
            None, cgen.capn_free(vCapn.Address())).AddReturn(cgen.NULL))

    block.Newline()

    pythonBytesFromCapn = cgen.Var(compiler._python_bytes_from_capn_function_name(), None)
    block.AddAssignment(result, pythonBytesFromCapn(vCapn.Address()))
    block.AddAssignment(None, cgen.capn_free(vCapn.Address()))


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
  def __init__(self, capnp_state, c_state_type, capnp_transition_type, c_transition_type, apply_transition,
               nil_transition_c_expression):
    self.name = f"Basic{_gen_name()}"
    self.capnp_state = capnp_state
    self.c_state_type = c_state_type
    self.capnp_transition_type = capnp_transition_type
    self.c_transition_type = c_transition_type
    self.nil_transition_c_expression = nil_transition_c_expression

    self._apply_transition = apply_transition

    super(BasicType, self).__init__()

  def _write_c_transitions_to_capnp_ptr(self, compiler, block, vSegment, transitionsRvalue, vPtr):
    newPtr = compiler.type_to_capnp_transitions_new_ptr_function(self)
    block.AddAssignment(cgen.CreateVar(vPtr), newPtr(vSegment, cgen.One))
    singleTransition = cgen.Var('single_transition', compiler.type_to_capnp_transitions_ptr(self))
    block.AddDeclaration(cgen.CreateVar(singleTransition))
    block.AddAssignment(singleTransition.Dot('p'), cgen.capn_getp(vPtr.Dot('p'), cgen.Zero, cgen.Zero))

    vTotal = cgen.Var('combined_total', self.c_transition_type)
    block.AddAssignment(cgen.CreateVar(vTotal), self.nil_transition_c_expression)
    basicLValue = cgen.UpdateVar(vTotal)
    basicRValue = vTotal

    cTransitionsIndex = cgen.Var('c_transitions_i', cgen.MachineInt)
    block.AddAssignment(cgen.CreateVar(cTransitionsIndex), cgen.Zero)
    loop = block.AddWhile(cTransitionsIndex < cgen.kv_size(transitionsRvalue))
    loop.AddAssignment(basicLValue, self._apply_transition(basicRValue, cgen.kv_A(transitionsRvalue,
                                                                                  cTransitionsIndex)))
    loop.AddAssignment(cgen.UpdateVar(cTransitionsIndex), cTransitionsIndex + cgen.One)

    setBasicTransition = compiler.type_to_capnp_transition_field_set_function(self, 'basicTransition')
    block.AddAssignment(None, setBasicTransition(singleTransition, basicRValue))

  def _write_c_state_to_capnp_ptr(self, compiler, block, vSegment, stateRvalue, vPtr):
    setBasicState = compiler.type_to_capnp_field_set_function(self, 'basicState')
    block.AddAssignment(None, setBasicState(vPtr, stateRvalue))

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

  def generate_capnp_to_c_transition(self, compiler, block, capn_ptr_input, output_kvec):
    ptr = cgen.Var('ptr', compiler.type_to_capnp_transitions_ptr(self))
    structure = cgen.Var('parsed_structure', compiler.type_to_capnp_transitions_type(self))

    block.AddDeclaration(cgen.CreateVar(structure))
    block.AddDeclaration(cgen.CreateVar(ptr))
    block.Newline()

    readStructure = compiler.type_to_capnp_transitions_read_function(self)
    block.AddAssignment(cgen.UpdateVar(ptr).Dot('p'), capn_ptr_input)
    block.AddAssignment(None, readStructure(structure.Address(), ptr))

    t = compiler.c_types.type_to_transitions_ctype[self]
    block.AddAssignment(None, cgen.kv_push(t, output_kvec, structure.Dot('basicTransition')))

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
    struct = compiler.capnp.AddStructure(self.name)
    struct.AddField('basicState', self.capnp_state)
    return self.name

  def generate_apply_transition(self, block, stateLvalue, stateRvalue, transition):
    block.AddAssignment(stateLvalue, self._apply_transition(transition, stateRvalue))


apply_plus = lambda transition, stateRvalue: transition + stateRvalue
Int8 = BasicType(
    'Int8',
    c_state_type=cgen.Int8,
    capnp_transition_type='Int8',
    c_transition_type=cgen.Int8,
    apply_transition=apply_plus,
    nil_transition_c_expression=cgen.Zero)
Int16 = BasicType(
    'Int16',
    c_state_type=cgen.Int16,
    capnp_transition_type='Int16',
    c_transition_type=cgen.Int16,
    apply_transition=apply_plus,
    nil_transition_c_expression=cgen.Zero)
Int32 = BasicType(
    'Int32',
    c_state_type=cgen.Int32,
    capnp_transition_type='Int32',
    c_transition_type=cgen.Int32,
    apply_transition=apply_plus,
    nil_transition_c_expression=cgen.Zero)
Int64 = BasicType(
    'Int64',
    c_state_type=cgen.Int64,
    capnp_transition_type='Int64',
    c_transition_type=cgen.Int64,
    apply_transition=apply_plus,
    nil_transition_c_expression=cgen.Zero)
UInt8 = BasicType(
    'UInt8',
    c_state_type=cgen.UInt8,
    capnp_transition_type='UInt8',
    c_transition_type=cgen.UInt8,
    apply_transition=apply_plus,
    nil_transition_c_expression=cgen.Zero)
UInt16 = BasicType(
    'UInt16',
    c_state_type=cgen.UInt16,
    capnp_transition_type='UInt16',
    c_transition_type=cgen.UInt16,
    apply_transition=apply_plus,
    nil_transition_c_expression=cgen.Zero)
UInt32 = BasicType(
    'UInt32',
    c_state_type=cgen.UInt32,
    capnp_transition_type='UInt32',
    c_transition_type=cgen.UInt32,
    apply_transition=apply_plus,
    nil_transition_c_expression=cgen.Zero)
UInt64 = BasicType(
    'UInt64',
    c_state_type=cgen.UInt64,
    capnp_transition_type='UInt64',
    c_transition_type=cgen.UInt64,
    apply_transition=apply_plus,
    nil_transition_c_expression=cgen.Zero)


class Product(Type):
  def __init__(self, items, name=None):
    self.items = items
    self.d = dict(items)
    if len(self.items) != len(self.d):
      raise RuntimeError("Duplicate key detected in Product.")
    self.name = name if name is not None else f"Product{_gen_name()}"

    super(Product, self).__init__()

  def _write_c_transitions_to_capnp_ptr(self, compiler, block, vSegment, transitionsRvalue, vPtr):
    newPtr = compiler.type_to_capnp_transitions_new_ptr_function(self)
    nTransitions = cgen.kv_size(transitionsRvalue)
    block.AddAssignment(cgen.CreateVar(vPtr), newPtr(vSegment, nTransitions))

    cTransitionsIndex = cgen.Var('c_transitions_i', cgen.MachineInt)
    block.AddAssignment(cgen.CreateVar(cTransitionsIndex), cgen.Zero)
    loop = block.AddWhile(cTransitionsIndex < nTransitions)

    transitionI = cgen.Var('transition_i', compiler.type_to_capnp_transitions_ptr(self))
    loop.AddDeclaration(cgen.CreateVar(transitionI))
    loop.AddAssignment(transitionI.Dot('p'), cgen.capn_getp(vPtr.Dot('p'), cTransitionsIndex, cgen.Zero))

    # FIXME(KK): Finish this!
    #loop.AddAssignment(basicLValue, self._apply_transition(basicRValue, cgen.kv_A(transitionsRvalue, cTransitionsIndex)))

    loop.AddAssignment(cgen.UpdateVar(cTransitionsIndex), cTransitionsIndex + cgen.One)

  def _write_c_state_to_capnp_ptr(self, compiler, block, vSegment, stateRvalue, vPtr):
    for field, t in self.items:
      setField = compiler.type_to_capnp_field_set_function(self, field)

      newPtr = compiler.type_to_capnp_state_new_ptr_function(t)
      itemPtr = cgen.Var(f'{field}_component_ptr', compiler.type_to_capnp_state_ptr(t))
      block.AddAssignment(cgen.CreateVar(itemPtr), newPtr(vSegment))

      itemValue = stateRvalue.Dot(field).Deref()
      t._write_c_state_to_capnp_ptr(compiler, block, vSegment, itemValue, itemPtr)
      block.AddAssignment(None, setField(vPtr, itemPtr))
      block.Newline()

  def generate_apply_transition(self, block, stateLvalue, stateRvalue, transition):
    # NOTE(KK): When we need to maintain the product state, then the components' states
    # are also being maintained.  In that case, since the product shares data with them, it doesn't actually
    # need any updating at all.
    # The generated c code should be specifically designed to treat products differently so as to ensure
    # that when a product is maintaining its state, so are all its components.
    pass

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
    struct = compiler.cprogram.AddStruct(f"{self.name}_simultaneous")
    union.AddField('simultaneous', struct.Star())
    enum.AddOption('simultaneous')
    for key, value in self.items:
      struct.AddField(key, compiler.c_transitions_ref(value).Star())

  def _write_capnp_simultaneous_components_transition_definitions(self, compiler, union):
    struct = compiler.capnp.AddStructure(f"{self.name}Simultaneous")
    union.AddField("simultaneous", struct)

    for key, value in self.items:
      struct.AddField(key, compiler.capnp_transitions_ref(value))

  def _write_c_individual_components_transition_definitions(self, compiler, union, enum):
    for key, value in self.items:
      union.AddField(f'product_on_{key}', compiler.c_transitions_ref(value).Star())
      enum.AddOption(f'product_on_{key}')

  def _write_capnp_individual_components_transition_definitions(self, compiler, union):
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
    struct = compiler.cprogram.AddStruct(self.name + '_c')
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
    struct = compiler.cprogram.AddStruct(self.name + "_c")
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
