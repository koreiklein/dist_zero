from dist_zero import capnpgen, errors, cgen


class ConcreteType(object):
  '''
  A concrete type is like a `dist_zero.types.Type`, but with all relevant physical
  representations already determined.
  In particular, this means that the c structures and network messages to represent
  the type will be determined.
  '''

  def initialize(self, compiler):
    '''
    Initialize this type to produce concrete types inside the given compiler.

    :param compiler: The compiler instance that will generate code using this concrete type.
    :type compiler: `ReactiveCompiler`

    :return: ``self``
    '''
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  def initialize_capnp(self, compiler):
    '''
    Initialize the capnp structures for this type.
    '''
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  def generate_apply_transition(self, block, stateLvalue, stateRvalue, transition):
    '''
    Generate code in ``block`` to update a state given a c expression for a transition on that state.
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def _write_capnp_transition_definition(self, compiler, ident, union):
    '''Add a definition for the capnp transitions of this type matching ``ident`` to the capnp union object.'''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def _write_single_c_transition_to_capnp_ptr(self, compiler, block, vSegment, transitionRvalue, vPtr):
    '''Write a single c transition to a variable of type ``self.capnp_transitions_type.c_ptr_type``'''
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

  def generate_capnp_to_c_state(self, compiler, block, ptr, output_lvalue):
    '''
    Generate code in ``block`` to write to the ``output_lvalue`` state using the ``ptr``
    capn_ptr variable containing the ptr variable for the matching capnproto type.
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def generate_capnp_to_c_transition(self, compiler, block, ptr):
    '''
    Generate code in ``block`` to compute expressions for c transitions from a capnp ptr,
    yield the (block, expression) pairs as they are generated.
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  @property
  def c_state_type(self):
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  @property
  def capnp_state_type(self):
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  @property
  def c_transitions_type(self):
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  @property
  def capnp_transitions_type(self):
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  @property
  def dz_type(self):
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  def _write_c_transition_definition(self, compiler, ident, union, enum):
    raise RuntimeError(f"Unrecognized transition identifier {ident} for {self.__class__}.")

  def _capnp_transitions_structure_name(self):
    '''return the name to use for the capnp structure with this type's transitions..'''
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  def _write_indiscrete_transition_definition(self, compiler, union):
    union.AddField("jump", self.capnp_state_type.name)

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

  def generate_c_transitions_to_capnp(self, compiler, block, transitionsRvalue, result):
    '''
    Generate code in ``block`` to write the capnp data from ``transitionsRValue``.
    :param result: The c lvalue to assign the result to
    '''
    vCapn, vCapnPtr, vSegment = self._init_capn_mem(block)

    vPtr = cgen.Var(f'{self.name}_ptr', self.capnp_transitions_type.c_ptr_type)

    self._write_c_transitions_to_capnp_ptr(compiler, block, vSegment, transitionsRvalue, vPtr)

    self._write_capn_to_python_bytes(compiler, block, vCapn, vCapnPtr, vPtr, result)

  def _write_capn_to_python_bytes(self, compiler, block, vCapn, vCapnPtr, ptr, result):
    (block.AddIf(cgen.Zero != cgen.capn_setp(vCapnPtr, cgen.Zero, ptr.Dot('p'))).consequent.AddAssignment(
        None, compiler.pyerr_from_string("Failed to capn_setp for root when producing output.")).AddAssignment(
            None, cgen.capn_free(vCapn.Address())).AddReturn(cgen.NULL))

    block.Newline()

    pythonBytesFromCapn = cgen.Var(compiler._python_bytes_from_capn_function_name(), None)
    block.AddAssignment(result, pythonBytesFromCapn(vCapn.Address()))
    block.AddAssignment(None, cgen.capn_free(vCapn.Address()))

  def generate_c_state_to_capnp(self, compiler, block, stateRvalue, result):
    '''
    Generate code in ``block`` to write the capnp data from ``stateRValue``.
    :param result: The c lvalue to assign the result to
    '''
    vCapn, vCapnPtr, vSegment = self._init_capn_mem(block)

    newPtr = self.capnp_state_type.c_new_ptr_function
    vPtr = cgen.Var(f'{self.name}_ptr', self.capnp_state_type.c_ptr_type)
    block.AddAssignment(cgen.CreateVar(vPtr), newPtr(vSegment))

    self._write_c_state_to_capnp_ptr(compiler, block, vSegment, stateRvalue, vPtr)

    self._write_capn_to_python_bytes(compiler, block, vCapn, vCapnPtr, vPtr, result)

  def _c_transitions_structure_name(self):
    return f"{self._capnp_transitions_structure_name()}_c_transitions"

  def _write_c_transition_identifiers(self, compiler, union, enum):
    for ti in self.dz_type.transition_identifiers:
      if ti == 'indiscrete':
        self._write_indiscrete_c_transition_definition(compiler, union, enum)
      else:
        self._write_c_transition_definition(compiler, ti, union, enum)

  def _write_capnp_transition_identifiers(self, compiler, union):
    for ti in self.dz_type.transition_identifiers:
      if ti == 'indiscrete':
        self._write_indiscrete_transition_definition(compiler, union)
      else:
        self._write_capnp_transition_definition(compiler, ti, union)

  def _write_indiscrete_c_transition_definition(self, compiler, union, enum):
    union.AddField("jump", self.c_state_type.Star())
    enum.AddOption('jump')

  def _write_c_transitions_definition(self, compiler):
    if len(self.dz_type.transition_identifiers) == 0:
      return cgen.Void
    else:
      name = self._c_transitions_structure_name()

      struct = compiler.program.AddStruct(f"{name}_c")
      union = compiler.program.AddUnion(f'{name}_c__union')
      enum = compiler.program.AddEnum(f'{name}_c__enum')
      self._write_c_transition_identifiers(compiler, union, enum)

      if union.RemoveIfEmpty():
        enum.RemoveIfEmpty()
      else:
        struct.AddField('type', enum)
        struct.AddField('value', union)
      return struct

  def _write_capnp_transitions_definition(self, compiler):
    if len(self.dz_type.transition_identifiers) == 0:
      return capnpgen.Void
    else:
      name = self._capnp_transitions_structure_name()
      struct = compiler.capnp.AddStructure(f"{name}Transition")
      union = struct.AddUnion()
      self._write_capnp_transition_identifiers(compiler, union)

      if union.RemoveIfTooSmall():
        self._write_capnp_transition_identifiers(compiler, struct)

      return struct


global_i = [0]


def inc_i():
  global_i[0] += 1
  return global_i[0]


class ConcreteBasicType(ConcreteType):
  def __init__(self, basic_type):
    self._basic_type = basic_type
    self.name = self._basic_type.name

    self._capnp_state_type = None
    self._basicState_field = None
    self._capnp_transitions_type = None
    self._capnp_transitions_basicTransition_field = None

  def generate_capnp_to_c_transition(self, compiler, block, ptr):
    vStructure = cgen.Var(f'{self.name}_transition_structure', self._capnp_transitions_type.c_structure_type)
    block.AddDeclaration(cgen.CreateVar(vStructure))
    block.AddAssignment(None, self._capnp_transitions_type.c_read_function(vStructure.Address(), ptr))

    yield block, vStructure.Dot('basicTransition')

    block.Newline()

  def generate_capnp_to_c_state(self, compiler, block, ptr, output_lvalue):
    vStructure = cgen.Var(f'{self.name}_structure_{inc_i()}', self._capnp_state_type.c_structure_type)
    block.AddDeclaration(cgen.CreateVar(vStructure))
    block.AddAssignment(None, self._capnp_state_type.c_read_function(vStructure.Address(), ptr))

    block.AddAssignment(output_lvalue, vStructure.Dot('basicState'))

    block.Newline()

  def _write_capnp_transition_definition(self, compiler, ident, union):
    union.AddField('basicTransition', self.capnp_transition_type)

  def _write_single_c_transition_to_capnp_ptr(self, compiler, block, vSegment, transitionRvalue, vPtr):
    block.AddAssignment(None, self._capnp_transitions_basicTransition_field.c_set(vPtr, transitionRvalue))

  def _write_c_transitions_to_capnp_ptr(self, compiler, block, vSegment, transitionsRvalue, vPtr):
    block.AddAssignment(cgen.CreateVar(vPtr), self._capnp_transitions_type.c_new_ptr_function(vSegment))

    vTotal = cgen.Var('combined_total', self._basic_type.c_transition_type)
    block.AddAssignment(cgen.CreateVar(vTotal), self._basic_type.nil_transition_c_expression)
    basicLValue = cgen.UpdateVar(vTotal)
    basicRValue = vTotal

    cTransitionsIndex = cgen.Var('c_transitions_i', cgen.MachineInt)
    block.AddAssignment(cgen.CreateVar(cTransitionsIndex), cgen.Zero)
    loop = block.AddWhile(cTransitionsIndex < cgen.kv_size(transitionsRvalue))
    loop.AddAssignment(basicLValue,
                       self._basic_type._apply_transition(basicRValue, cgen.kv_A(transitionsRvalue, cTransitionsIndex)))
    loop.AddAssignment(cgen.UpdateVar(cTransitionsIndex), cTransitionsIndex + cgen.One)

    block.AddAssignment(None, self._capnp_transitions_basicTransition_field.c_set(vPtr, basicRValue))

  @property
  def capnp_state_type(self):
    return self._capnp_state_type

  def _write_c_state_to_capnp_ptr(self, compiler, block, vSegment, stateRvalue, vPtr):
    block.AddAssignment(None, self._basicState_field.c_set(vPtr, stateRvalue))

  @property
  def capnp_transitions_type(self):
    return self._capnp_transitions_type

  def _capnp_transitions_structure_name(self):
    return f"{self.name}"

  @property
  def c_state_type(self):
    return self._basic_type.c_state_type

  @property
  def c_transitions_type(self):
    return self._basic_type.c_transition_type

  def initialize_capnp(self, compiler):
    if self._capnp_state_type is not None:
      return

    self._capnp_state_type = compiler.capnp.AddStructure(self.name)
    self._basicState_field = self._capnp_state_type.AddField('basicState', self._basic_type.capnp_state)

    self._capnp_transitions_type = compiler.capnp.AddStructure(f"{self.name}Transitions")
    self._capnp_transitions_basicTransition_field = self._capnp_transitions_type.AddField(
        'basicTransition', self._basic_type.capnp_transition_type)

  def initialize(self, compiler):

    return self

  @property
  def dz_type(self):
    return self._basic_type


class ConcreteProductType(ConcreteType):
  def __init__(self, product_type):
    self._product_type = product_type
    self.name = self._product_type.name

    self._c_state_type = None
    self._capnp_state_type = None
    self._capnp_single_transition_type = None
    self._capnp_transitions_type = None
    self._capnp_single_transition_union = None
    self._items = None

    self._c_transition_union = None
    self._c_transition_enum = None

  @property
  def capnp_transitions_type(self):
    return self._capnp_transitions_type

  def _write_c_state_to_capnp_ptr(self, compiler, block, vSegment, stateRvalue, vPtr):
    for field, t in self._items:
      itemPtr = cgen.Var(f'{field}_component_ptr', t.capnp_state_type.c_ptr_type)
      block.AddAssignment(cgen.CreateVar(itemPtr), t.capnp_state_type.c_new_ptr_function(vSegment))

      itemValue = stateRvalue.Dot(field).Deref()
      t._write_c_state_to_capnp_ptr(compiler, block, vSegment, itemValue, itemPtr)
      block.AddAssignment(None, self.capnp_state_type.c_set_field(field)(vPtr, itemPtr))
      block.Newline()

  def _write_capnp_simultaneous_components_transition_definitions(self, compiler, union):
    struct = compiler.capnp.AddStructure(f"{self.name}Simultaneous")
    field = union.AddField("simultaneous", struct)

    for key, value in self._items:
      struct.AddField(key, value.capnp_transitions_type.name)

  def _write_capnp_individual_components_transition_definitions(self, compiler, union):
    for key, value in self._items:
      field = union.AddField(f"productOn{key}", value.capnp_transitions_type.name)

  def _write_capnp_transition_definition(self, compiler, ident, union):
    self._capnp_single_transition_union = union
    if ident == 'standard':
      self._write_capnp_individual_components_transition_definitions(compiler, union)
    elif ident == 'individual':
      self._write_capnp_individual_components_transition_definitions(compiler, union)
    elif ident == 'simultaneous':
      self._write_capnp_simultaneous_components_transition_definitions(compiler, union)
    else:
      raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _write_c_transitions_to_capnp_ptr(self, compiler, block, vSegment, transitionsRvalue, vPtr):
    newPtr = self._capnp_transitions_type.c_new_ptr_function
    nTransitions = cgen.kv_size(transitionsRvalue)
    block.AddAssignment(cgen.CreateVar(vPtr), newPtr(vSegment))

    vTransitions = cgen.Var(f"{self.name}_transitions_list", self._capnp_single_transition_type.c_list_type)
    block.AddAssignment(
        cgen.CreateVar(vTransitions), self._capnp_single_transition_type.c_new_list_function(vSegment, nTransitions))
    block.AddAssignment(None, self._capnp_transitions_type.c_set_field('transitions')(vPtr, vTransitions))

    cTransitionsIndex = cgen.Var('c_transitions_i', cgen.MachineInt)
    block.AddAssignment(cgen.CreateVar(cTransitionsIndex), cgen.Zero)
    loop = block.AddWhile(cTransitionsIndex < nTransitions)

    transitionI = cgen.Var('transition_i', self._capnp_single_transition_type.c_ptr_type)
    loop.AddDeclaration(cgen.CreateVar(transitionI))
    loop.AddAssignment(transitionI.Dot('p'), cgen.capn_getp(vTransitions.Dot('p'), cTransitionsIndex, cgen.Zero))

    vCTransition = cgen.kv_A(transitionsRvalue, cTransitionsIndex)

    switch = loop.AddSwitch(vCTransition.Dot('type'))

    for ident in self._product_type.transition_identifiers:
      if ident == 'standard':
        self._write_c_transitions_to_capnp_individual_components(compiler, switch, vSegment, vCTransition, transitionI)
      elif ident == 'individual':
        self._write_c_transitions_to_capnp_individual_components(compiler, switch, vSegment, vCTransition, transitionI)
      elif ident == 'simultaneous':
        self._write_c_transitions_to_capnp_simultaneous(compiler, switch, vSegment, vCTransition, transitionI)
      else:
        raise RuntimeError(f"Unrecognized transition identifier {ident}.")

    loop.AddAssignment(cgen.UpdateVar(cTransitionsIndex), cTransitionsIndex + cgen.One)

  def _write_c_transitions_to_capnp_individual_components(self, compiler, switch, vSegment, vCTransition,
                                                          transitionCapnPtr):
    for key, value in self._items:
      name = f'product_on_{key}'
      block = switch.AddCase(self._c_transition_enum.literal(name))

      vStruct = cgen.Var(f'capn_single_transition_struct_{key}', self._capnp_single_transition_type.c_structure_type)
      block.AddDeclaration(cgen.CreateVar(vStruct))
      block.AddAssignment(
          cgen.UpdateVar(vStruct).Dot('which'), cgen.Constant(f"{self._product_type.name}Transition_productOn{key}"))

      vValuePtr = cgen.Var(f'capn_sts_value_{key}', value.capnp_transitions_type.c_ptr_type)
      block.AddAssignment(cgen.CreateVar(vValuePtr), value.capnp_transitions_type.c_new_ptr_function(vSegment))
      block.AddAssignment(cgen.UpdateVar(vStruct).Dot(f'productOn{key}'), vValuePtr)

      value._write_single_c_transition_to_capnp_ptr(compiler, block, vSegment,
                                                    vCTransition.Dot('value').Dot(name).Deref(), vValuePtr)

      block.AddAssignment(None, self._capnp_single_transition_type.c_write_function(vStruct.Address(),
                                                                                    transitionCapnPtr))

      block.AddBreak()

  def initialize(self, compiler):
    struct = compiler.program.AddStruct(self._product_type.name + '_c')
    self._items = []
    for key, value in self._product_type.items:
      self._items.append((key, compiler.get_concrete_type_for_type(value)))

    for key, value in self._items:
      struct.AddField(key, value.c_state_type.Star())

    self._c_state_type = struct
    self._c_transitions_type = self._write_c_transitions_definition(compiler)

    return self

  def initialize_capnp(self, compiler):
    if self._capnp_state_type is not None:
      return

    self._capnp_state_type = compiler.capnp.AddStructure(self.name)
    for key, value in self._items:
      value.initialize_capnp(compiler)
      self._capnp_state_type.AddField(key, value.capnp_state_type)

    self._capnp_single_transition_type = self._write_capnp_transitions_definition(compiler)
    self._capnp_transitions_type = _wrap_struct_in_list(compiler, self._capnp_single_transition_type)

  def generate_capnp_to_c_state(self, compiler, block, ptr, output_lvalue):
    key_to_expr = {}

    vProductStruct = cgen.Var(f'v_product_struct{self.name}', self._capnp_state_type.c_structure_type)
    block.AddDeclaration(cgen.CreateVar(vProductStruct))
    block.AddAssignment(None, self._capnp_state_type.c_read_function(vProductStruct.Address(), ptr))

    for key, value in self._items:
      vComponent = cgen.Var(f'component_{key}', value.c_state_type.Star())
      # FIXME(KK): Be sure this malloc is eventually freed
      block.AddAssignment(
          cgen.CreateVar(vComponent),
          cgen.malloc(value.c_state_type.Sizeof()).Cast(value.c_state_type.Star()))
      value.generate_capnp_to_c_state(compiler, block, vProductStruct.Dot(key), vComponent.Deref())
      key_to_expr[key] = vComponent

    block.AddAssignment(output_lvalue, cgen.StructureLiteral(struct=self._c_state_type, key_to_expr=key_to_expr))

  def generate_capnp_to_c_transition(self, compiler, block, ptr):
    ptrStruct = cgen.Var('ptr_struct', self._capnp_transitions_type.c_structure_type)
    block.AddDeclaration(cgen.CreateVar(ptrStruct))
    block.AddAssignment(None, self._capnp_transitions_type.c_read_function(ptrStruct.Address(), ptr))

    vList = ptrStruct.Dot('transitions')

    vItem = cgen.Var('list_item', self._capnp_single_transition_type.c_ptr_type)
    block.AddDeclaration(cgen.CreateVar(vItem))
    vIndex = cgen.Var('transition_index', cgen.MachineInt)

    block.AddAssignment(cgen.CreateVar(vIndex), cgen.Zero)
    nTransitions = cgen.capn_len(vList)
    block.logf(f"Product is looping over %d capnp_transitions to deserialize them.\n", nTransitions)
    loop = block.AddWhile(vIndex < nTransitions)

    loop.AddAssignment(cgen.UpdateVar(vItem).Dot('p'), cgen.capn_getp(vList.Dot('p'), vIndex, cgen.Zero))

    yield from self._generate_capnp_to_c_single_transition(compiler, loop, vItem)

    loop.AddAssignment(cgen.UpdateVar(vIndex), vIndex + cgen.One)

  def _generate_capnp_to_c_single_transition(self, compiler, block, singleTransitionPtr):
    vSingleTransition = cgen.Var('transition_structure', self._capnp_single_transition_type.c_structure_type)
    block.AddDeclaration(cgen.CreateVar(vSingleTransition))
    block.AddAssignment(
        None, self._capnp_single_transition_type.c_read_function(vSingleTransition.Address(), singleTransitionPtr))
    switch = block.AddSwitch(vSingleTransition.Dot('which'))

    for ident in self._product_type.transition_identifiers:
      if ident == 'standard':
        yield from self._generate_capnp_to_c_single_transition_individual_components(
            compiler, switch, vSingleTransition)
      elif ident == 'individual':
        yield from self._generate_capnp_to_c_single_transition_individual_components(
            compiler, switch, vSingleTransition)
      elif ident == 'simultaneous':
        yield from self._generate_capnp_to_c_single_transition_simultaneous(compiler, switch, vSingleTransition)
      else:
        raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _generate_capnp_to_c_single_transition_individual_components(self, compiler, switch, vSingleTransition):
    for key, value in self._items:
      c_name = f'product_on_{key}'
      name = f"productOn{key}"
      block = switch.AddCase(self._capnp_single_transition_union.c_enum_option_by_name(name))
      componentPtr = vSingleTransition.Dot(name)

      i = 0
      for cblock, cexpr in value.generate_capnp_to_c_transition(compiler, block, componentPtr):
        vFromValue = cgen.Var(f'from_value_{key}_{i}', value.c_transitions_type.Star())
        # FIXME(KK): Be sure this malloc is eventually freed
        cblock.AddAssignment(
            cgen.CreateVar(vFromValue),
            cgen.malloc(value.c_transitions_type.Sizeof()).Cast(value.c_transitions_type.Star()))
        cblock.AddAssignment(cgen.UpdateVar(vFromValue).Deref(), cexpr)
        cblock.logf(f"Deserialized %d.\n", vFromValue.Deref())
        yield cblock, cgen.StructureLiteral(
            struct=self._c_transitions_type,
            key_to_expr={
                'type': self._c_transition_enum.literal(c_name),
                'value': self._c_transition_union.literal(c_name, vFromValue),
            })
        i += 1

      block.AddBreak()

  def _generate_capnp_to_c_single_transition_simultaneous(self, compiler, switch, vSingleTransition):
    block = switch.AddCase(self._capnp_single_transition_union.c_enum_option_by_name('simultaneous'))
    raise errors.InternalError("Not Yet Implemented")
    block.AddBreak()

  @property
  def capnp_state_type(self):
    return self._capnp_state_type

  def _write_c_simultaneous_components_transition_definitions(self, compiler, union, enum):
    struct = compiler.program.AddStruct(f"{self.name}_simultaneous")
    union.AddField('simultaneous', struct.Star())
    enum.AddOption('simultaneous')
    for key, value in self._items:
      struct.AddField(key, value.c_transitions_type.Star())

  def _write_c_individual_components_transition_definitions(self, compiler, union, enum):
    for key, value in self._items:
      union.AddField(f'product_on_{key}', value.c_transitions_type.Star())
      enum.AddOption(f'product_on_{key}')

  def _write_c_transition_definition(self, compiler, ident, union, enum):
    self._c_transition_union = union
    self._c_transition_enum = enum
    if ident == 'standard':
      self._write_c_individual_components_transition_definitions(compiler, union, enum)
    elif ident == 'individual':
      self._write_c_individual_components_transition_definitions(compiler, union, enum)
    elif ident == 'simultaneous':
      self._write_c_simultaneous_components_transition_definitions(compiler, union, enum)
    else:
      raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _capnp_transitions_structure_name(self):
    return f"{self.name}"

  @property
  def c_transitions_type(self):
    return self._c_transitions_type

  @property
  def dz_type(self):
    return self._product_type

  @property
  def c_state_type(self):
    return self._c_state_type


class ConcreteSumType(ConcreteType):
  def __init__(self, sum_type):
    self._sum_type = sum_type
    self.name = self._sum_type.name

    self._c_state_type = None
    self._c_transitions_type = None
    self._capnp_state_type = None
    self._items = None

    self._capnp_state_type

  @property
  def c_transitions_type(self):
    return self._c_transitions_type

  @property
  def c_state_type(self):
    return self._c_state_type

  def _capnp_transitions_structure_name(self):
    return f"{self.name}"

  @property
  def dz_type(self):
    return self._sum_type

  @property
  def capnp_state_type(self):
    return self._capnp_state_type

  def initialize_capnp(self, compiler):
    struct = compiler.capnp.AddStructure(self.name)
    if len(self._items) == 0:
      pass
    elif len(self._items) == 1:
      for key, value in self.items:
        struct.AddField(key, value.capnp_state_type.name)
    else:
      union = struct.AddUnion()
      for key, value in self._items:
        union.AddField(key, value.capnp_state_type.name)

    self._capnp_state_type = struct

  def _write_c_individual_components_transition_definitions(self, compiler, union, enum):
    for key, value in self._items:
      union.AddField(f"sum_on_{key}", value.c_transitions_type.Star())
      enum.AddOption(f"sum_on_{key}")

  def _write_capnp_individual_components_transition_definitions(self, compiler, union):
    for key, value in self.items:
      union.AddField(f"sumOn{key}", value.capnp_transitions_type.name)

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

  def initialize(self, compiler):
    self._c_state_type = compiler.program.AddStruct(f"{self.name}_c")
    self._items = []
    for key, value in self._sum_type.items:
      self._items.append((key, compiler.get_concrete_type_for_type(value)))

    if len(self._items) > 0:
      union = compiler.program.AddUnion(f'{self.name}_union')
      enum = compiler.program.AddEnum(f'{self.name}_enum')

      self._c_state_type.AddField('type', enum)
      self._c_state_type.AddField('value', union)

      for key, value in self._items:
        enum.AddOption(key)
        union.AddField(key, value.c_state_type.Star())

    self._c_transitions_type = self._write_c_transitions_definition(compiler)

    return self


class ConcreteList(ConcreteType):
  def __init__(self, base_list_type):
    self._base_list_type = base_list_type
    self.base = None
    self.name = None

    self._c_state_type = None
    self._c_transitions_type = None
    self._initialized_capnp = False

  @property
  def c_transitions_type(self):
    return self._c_transitions_type

  def _capnp_transitions_structure_name(self):
    return f"{self.name}"

  @property
  def dz_type(self):
    return self._base_list_type

  @property
  def c_state_type(self):
    return self._c_state_type

  @property
  def capnp_state_type(self):
    return self._capnp_state_type

  def initialize(self, compiler):
    self.base = compiler.get_concrete_type_for_type(self._base_list_type.base)
    self.name = f"{self.base.name}List"
    self._c_state_type = self.base.c_state_type.Star().KVec()
    self._c_transitions_type = self._write_c_transitions_definition(compiler)

    return self

  def _write_c_append_transition_definition(self, compiler, union, enum):
    union.AddField('append', self.base.c_state_type.Star())

  def _write_c_insert_transition_definition(self, compiler, union, enum):
    insertStruct = compiler.program.AddStruct(f"insert_in_{self.name}")
    insertStruct.AddField('index', cgen.UInt32)
    insertStruct.AddField('value', self.base.c_state_type)
    union.AddField('insert', insertStruct.Star())
    enum.AddOption('insert')

  def _write_append_transition_definition(self, compiler, union):
    union.AddField('append', self.base.capnp_state_type)

  def _write_insert_transition_definition(self, compiler, union):
    insertStruct = compiler.capnp.AddStructure(f"InsertIn{self.name}")
    insertStruct.AddField('index', capnpgen.UInt32)
    insertStruct.AddField('value', self.base.capnp_state_type)

    union.AddField('insert', insertStruct)

  def _write_c_on_index_transition_definition(self, compiler, union, enum):
    onIndexStruct = compiler.program.AddStruct(f"on_index_{self.name}")
    onIndexStruct.AddField('index', cgen.UInt32)
    onIndexStruct.AddField('transition', self.base.c_transitions_type.Star())

    union.AddField('on_index', onIndexStruct.Star())
    enum.AddOption('on_index')

  def initialize_capnp(self, compiler):
    if not self._initialized_capnp:
      self.base.initialize_capnp(compiler)
      self._initialized_capnp = True

    self._capnp_transitions_type = _wrap_struct_in_list(compiler, self._write_capnp_transitions_definition(compiler))

  def _write_on_index_transition_definition(self, compiler, union):
    onIndexStruct = compiler.capnp.AddStructure(f"OnIndex{self.name}")
    onIndexStruct.AddField('index', capnpgen.UInt32)
    onIndexStruct.AddField('transition', self.base.capnp_transitions_type)

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

  @property
  def capnp_state_type(self):
    return f"List({self.base.capnp_state_type})"


def _wrap_struct_in_list(compiler, struct):
  many_struct = compiler.capnp.AddStructure(f"{struct.name}List")
  many_struct.AddField('transitions', f"List({struct.name})")
  return many_struct
