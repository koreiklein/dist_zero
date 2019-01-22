from dist_zero import capnpgen, errors, cgen


class ConcreteType(object):
  '''
  A concrete type consists of a `dist_zero.types.Type`, along with a specification for exactly
  how the states and transitions will be represented internally in C and externally in capnproto structures.
  '''

  @property
  def dz_type(self):
    '''The underlying abstract `dist_zero.types.Type`.'''
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  @property
  def c_state_type(self):
    '''The `cgen.type.CType` used in C to represent the state of this type.'''
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  @property
  def capnp_state_type(self):
    '''The `capnpgen.Structure` used to represent the state of this type.'''
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  @property
  def c_transitions_type(self):
    '''The `cgen.type.CType` used in C to represent the transitions for this type.'''
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  @property
  def capnp_transitions_type(self):
    '''The `capnpgen.Structure` used to represent the transitions for this type.'''
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  def initialize(self, compiler):
    '''
    Initialize this type to produce concrete types inside the given compiler.

    :param compiler: The compiler instance that will generate code using this concrete type.
    :type compiler: `ReactiveCompiler`

    :return: ``self``
    '''
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  def generate_free_state(self, compiler, block, stateRvalue):
    '''
    Generate C code to free the allocated memory associated with this type.

    :param compiler: The compiler instance that will generate code using this concrete type.
    :type compiler: `ReactiveCompiler`
    :param block: The block in which to generate the code.
    :type block: `Block`
    :param stateRvalue: A C expression for the state to free.
    :type stateRvalue: `cgen.expression.Expression`
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

  def _write_single_c_transition_to_capnp_ptr(self, transitionRvalue, write_ctx):
    '''
    Write a single c transition to a variable of type ``self.capnp_transitions_type.c_ptr_type``

    :param transitionRvalue: The C rvalue for a single c transition to read
    :type transitionRvalue: `cgen.expression.Var`
    :param write_ctx: The context for writing to the capnproto structure.
    :type write_ctx: `CapnpWriteContext`
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def _write_c_state_to_capnp_ptr(self, stateRvalue, write_ctx):
    '''
    Write the state associated with ``stateRvalue`` into the capnp pointer vPtr.

    :param stateRvalue: An expression for the rvalue giving a c state of the type of self.
    :type stateRvalue: `dist_zero.cgen.expression.Expression`
    :param write_ctx: The capnproto context for writing this structure.
    :type write_ctx: `CapnpWriteContext`
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def _write_c_transitions_to_capnp_ptr(self, transitionsRvalue, write_ctx):
    '''
    Write the transitions associated with ``transitionsRvalue`` into the capnp pointer vPtr.

    :param transitionsRvalue: An expression for the rvalue giving the kvec of c transitions of the c transition type of self.
    :type transitionsRvalue: `dist_zero.cgen.expression.Expression`
    :param write_ctx: The capnproto context for writing this structure.
    :type write_ctx: `CapnpWriteContext`
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def generate_capnp_to_c_state(self, read_ctx, output_lvalue):
    '''
    Generate C code in to write to the ``output_lvalue`` state from a capnproto structure.

    :param read_ctx: Context object describing a capnproto pointer.
    :type read_ctx: `CapnpReadContext`
    :param output_lvalue: An lvalue for c state
    :type output_lvalue: `cgen.lvalue.Lvalue`
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def generate_and_yield_capnp_to_c_transition(self, read_ctx):
    '''
    Generate C code in ``block`` to compute expressions for c transitions from a capnp ptr context,
    yield the (block, expression) pairs as they are generated.
    '''
    raise RuntimeError(f'Abstract Superclass {self.__class__}')

  def _write_c_transition_definition(self, compiler, ident, union, enum):
    raise RuntimeError(f"Unrecognized transition identifier {ident} for {self.__class__}.")

  def _capnp_transitions_structure_name(self):
    '''return the name to use for the capnp structure with this type's transitions..'''
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  def _write_indiscrete_transition_definition(self, compiler, union):
    union.AddField("jump", self.capnp_state_type.name)

  def _init_capn_mem(self, block):

    vCapn = block.AddDeclaration(cgen.Var('capn', cgen.Capn))
    block.AddAssignment(None, cgen.capn_init_malloc(vCapn.Address()))
    vCapnPtr = block.AddDeclaration(cgen.Var('msg_ptr', cgen.Capn_Ptr), cgen.capn_root(vCapn.Address()))
    vSegment = block.AddDeclaration(cgen.Var('seg', cgen.Capn_Segment.Star()), vCapnPtr.Dot('seg'))

    block.Newline()

    return vCapn, vCapnPtr, vSegment

  def generate_c_transitions_to_capnp(self, compiler, block, transitionsRvalue, result):
    '''
    Generate code in ``block`` to write the capnp data from ``transitionsRValue``.
    :param result: The c lvalue to assign the result to
    '''
    vCapn, vCapnPtr, vSegment = self._init_capn_mem(block)

    vPtr = cgen.Var(f'{self.name}_ptr', self.capnp_transitions_type.c_ptr_type)

    self._write_c_transitions_to_capnp_ptr(
        transitionsRvalue, CapnpWriteContext(compiler=compiler, block=block, segment=vSegment, ptr=vPtr))

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

    vPtr = block.AddDeclaration(
        cgen.Var(f'{self.name}_ptr', self.capnp_state_type.c_ptr_type),
        self.capnp_state_type.c_new_ptr_function(vSegment))

    self._write_c_state_to_capnp_ptr(stateRvalue,
                                     CapnpWriteContext(compiler=compiler, block=block, segment=vSegment, ptr=vPtr))

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


class ConcreteBasicType(ConcreteType):
  def __init__(self, basic_type):
    self._basic_type = basic_type
    self.name = self._basic_type.name

    self._capnp_state_type = None
    self._basicState_field = None
    self._capnp_transitions_type = None
    self._capnp_transitions_basicTransition_field = None

  def generate_free_state(self, compiler, block, stateRvalue):
    pass

  def generate_apply_transition(self, block, stateLvalue, stateRvalue, transition):
    block.AddAssignment(stateLvalue, self._basic_type._apply_transition(transition, stateRvalue))

  def generate_and_yield_capnp_to_c_transition(self, read_ctx):
    vStructure = read_ctx.block.AddDeclaration(
        cgen.Var(f'{self.name}_transition_structure', self._capnp_transitions_type.c_structure_type))
    read_ctx.block.AddAssignment(None, self._capnp_transitions_type.c_read_function(vStructure.Address(), read_ctx.ptr))

    yield read_ctx.block, vStructure.Dot('basicTransition')

    read_ctx.block.Newline()

  def generate_capnp_to_c_state(self, read_ctx, output_lvalue):
    vStructure = read_ctx.block.AddDeclaration(
        cgen.Var(f'{self.name}_structure_{cgen.inc_i()}', self._capnp_state_type.c_structure_type))
    read_ctx.block.AddAssignment(None, self._capnp_state_type.c_read_function(vStructure.Address(), read_ctx.ptr))

    read_ctx.block.AddAssignment(output_lvalue, vStructure.Dot('basicState'))

    read_ctx.block.Newline()

  def _write_capnp_transition_definition(self, compiler, ident, union):
    union.AddField('basicTransition', self.capnp_transition_type)

  def _write_single_c_transition_to_capnp_ptr(self, transitionRvalue, write_ctx):
    write_ctx.block.AddAssignment(None,
                                  self._capnp_transitions_basicTransition_field.c_set(write_ctx.ptr, transitionRvalue))

  def _write_c_transitions_to_capnp_ptr(self, transitionsRvalue, write_ctx):
    write_ctx.block.AddDeclaration(write_ctx.ptr, self._capnp_transitions_type.c_new_ptr_function(write_ctx.segment))

    vTotal = write_ctx.block.AddDeclaration(
        cgen.Var('combined_total', self._basic_type.c_transition_type), self._basic_type.nil_transition_c_expression)
    basicLValue = vTotal
    basicRValue = vTotal

    with write_ctx.block.ForInt(cgen.kv_size(transitionsRvalue)) as (loop, cTransitionsIndex):
      loop.AddAssignment(
          basicLValue, self._basic_type._apply_transition(basicRValue, cgen.kv_A(transitionsRvalue, cTransitionsIndex)))

    write_ctx.block.AddAssignment(None, self._capnp_transitions_basicTransition_field.c_set(write_ctx.ptr, basicRValue))

  @property
  def capnp_state_type(self):
    return self._capnp_state_type

  def _write_c_state_to_capnp_ptr(self, stateRvalue, write_ctx):
    write_ctx.block.AddAssignment(None, self._basicState_field.c_set(write_ctx.ptr, stateRvalue))

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

  def generate_apply_transition(self, block, stateLvalue, stateRvalue, transition):
    # NOTE(KK): When we need to maintain the product state, then the components' states
    # are also being maintained.  In that case, since the product shares data with them, it doesn't actually
    # need any updating at all.
    # The generated c code should be specifically designed to treat products differently so as to ensure
    # that when a product is maintaining its state, so are all its components.
    pass

  @property
  def capnp_transitions_type(self):
    return self._capnp_transitions_type

  def _write_single_c_transition_to_capnp_ptr(self, transitionRvalue, write_ctx):
    vTransitions = write_ctx.block.AddDeclaration(
        cgen.Var(f"{self.name}_transitions_list", self._capnp_single_transition_type.c_list_type),
        self._capnp_single_transition_type.c_new_list_function(write_ctx.segment, cgen.One))
    write_ctx.block.AddAssignment(None,
                                  self._capnp_transitions_type.c_set_field('transitions')(write_ctx.ptr, vTransitions))

    transitionI = write_ctx.block.AddDeclaration(
        cgen.Var(f'single_c_transition_{cgen.inc_i()}', self._capnp_single_transition_type.c_ptr_type))
    write_ctx.block.AddAssignment(transitionI.Dot('p'), cgen.capn_getp(vTransitions.Dot('p'), cgen.Zero, cgen.Zero))

    self._write_single_c_transition_to_single_capnp_ptr(transitionRvalue, write_ctx.update_ptr(transitionI))

  def generate_product_apply_transition_forced(self, block, stateLvalue, stateRvalue, transition):
    '''Like generate_apply_transition, but ensure that the state is actually updated in response to the transition.'''
    switch = block.AddSwitch(transition.Dot('type'))

    for ident in self._product_type.transition_identifiers:
      if ident == 'standard':
        self._apply_transition_individual_components(switch, stateLvalue, stateRvalue, transition)
      elif ident == 'individual':
        self._apply_transition_individual_components(switch, stateLvalue, stateRvalue, transition)
      elif ident == 'simultaneous':
        self._apply_transition_simultaneous(switch, stateLvalue, stateRvalue, transition)
      else:
        raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _write_single_c_transition_to_single_capnp_ptr(self, transitionRvalue, write_ctx):
    switch = write_ctx.block.AddSwitch(transitionRvalue.Dot('type'))

    for ident in self._product_type.transition_identifiers:
      if ident == 'standard':
        self._write_c_transitions_to_capnp_individual_components(transitionRvalue, write_ctx.update_block(switch))
      elif ident == 'individual':
        self._write_c_transitions_to_capnp_individual_components(transitionRvalue, write_ctx.update_block(switch))
      elif ident == 'simultaneous':
        self._write_c_transitions_to_capnp_simultaneous(transitionRvalue, write_ctx.update_block(switch))
      else:
        raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _apply_transition_simultaneous(self, switch, stateLvalue, stateRvalue, transition):
    # FIXME(KK): Test and implement this
    raise RuntimeError("Not Yet Implemented")

  def _write_c_transitions_to_capnp_simultaneous(self, transitionRvalue, write_ctx):
    # FIXME(KK): Test and implement this
    raise RuntimeError("Not Yet Implemented")

  def _write_c_state_to_capnp_ptr(self, stateRvalue, write_ctx):
    for field, t in self._items:
      itemPtr = write_ctx.block.AddDeclaration(
          cgen.Var(f'{field}_component_ptr', t.capnp_state_type.c_ptr_type),
          t.capnp_state_type.c_new_ptr_function(write_ctx.segment))

      itemValue = stateRvalue.Dot(field).Deref()
      t._write_c_state_to_capnp_ptr(itemValue, write_ctx.update_ptr(itemPtr))
      write_ctx.block.AddAssignment(None, self.capnp_state_type.c_set_field(field)(write_ctx.ptr, itemPtr))
      write_ctx.block.Newline()

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

  def _write_c_transitions_to_capnp_ptr(self, transitionsRvalue, write_ctx):
    nTransitions = cgen.kv_size(transitionsRvalue)
    write_ctx.block.AddDeclaration(write_ctx.ptr, self._capnp_transitions_type.c_new_ptr_function(write_ctx.segment))

    vTransitions = write_ctx.block.AddDeclaration(
        cgen.Var(f"{self.name}_transitions_list", self._capnp_single_transition_type.c_list_type),
        self._capnp_single_transition_type.c_new_list_function(write_ctx.segment, nTransitions))
    write_ctx.block.AddAssignment(None,
                                  self._capnp_transitions_type.c_set_field('transitions')(write_ctx.ptr, vTransitions))

    with write_ctx.block.ForInt(nTransitions) as (loop, cTransitionsIndex):
      transitionI = loop.AddDeclaration(cgen.Var('transition_i', self._capnp_single_transition_type.c_ptr_type))
      loop.AddAssignment(transitionI.Dot('p'), cgen.capn_getp(vTransitions.Dot('p'), cTransitionsIndex, cgen.Zero))

      vCTransition = cgen.kv_A(transitionsRvalue, cTransitionsIndex)

      self._write_single_c_transition_to_single_capnp_ptr(vCTransition,
                                                          write_ctx.update_block(loop).update_ptr(transitionI))

  def _apply_transition_individual_components(self, switch, stateLvalue, stateRvalue, transition):
    for key, value in self._items:
      name = f'product_on_{key}'
      block = switch.AddCase(self._c_transition_enum.literal(name))

      value.generate_apply_transition(block,
                                      stateLvalue.Dot(key).Deref(),
                                      stateRvalue.Dot(key).Deref(),
                                      transition.Dot('value').Dot(name).Deref())

      block.AddBreak()

  def _write_c_transitions_to_capnp_individual_components(self, vCTransition, write_ctx):
    for key, value in self._items:
      name = f'product_on_{key}'
      block = write_ctx.block.AddCase(self._c_transition_enum.literal(name))

      vStruct = block.AddDeclaration(
          cgen.Var(f'capn_single_transition_struct_{key}', self._capnp_single_transition_type.c_structure_type))
      block.AddAssignment(vStruct.Dot('which'), cgen.Constant(f"{self._product_type.name}Transition_productOn{key}"))

      vValuePtr = block.AddDeclaration(
          cgen.Var(f'capn_sts_value_{key}', value.capnp_transitions_type.c_ptr_type),
          value.capnp_transitions_type.c_new_ptr_function(write_ctx.segment))
      block.AddAssignment(vStruct.Dot(f'productOn{key}'), vValuePtr)

      value._write_single_c_transition_to_capnp_ptr(
          vCTransition.Dot('value').Dot(name).Deref(),
          write_ctx.update_block(block).update_ptr(vValuePtr))

      block.AddAssignment(None, self._capnp_single_transition_type.c_write_function(vStruct.Address(), write_ctx.ptr))

      block.AddBreak()

  def initialize(self, compiler):
    struct = compiler.program.AddStruct(self._product_type.name + '_c')
    self._items = []
    for key, value in self._product_type.items:
      self._items.append((key, compiler.get_concrete_type(value)))

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

  def generate_free_state(self, compiler, block, stateRvalue):
    for key, value in self._items:
      value.generate_free_state(compiler, block, stateRvalue.Dot(key).Deref())
      block.AddAssignment(None, cgen.free(stateRvalue.Dot(key)))

  def generate_capnp_to_c_state(self, read_ctx, output_lvalue):
    key_to_expr = {}

    vProductStruct = read_ctx.block.AddDeclaration(
        cgen.Var(f'v_product_struct{self.name}', self._capnp_state_type.c_structure_type))
    read_ctx.block.AddAssignment(None, self._capnp_state_type.c_read_function(vProductStruct.Address(), read_ctx.ptr))

    for key, value in self._items:
      vComponent = read_ctx.block.AddDeclaration(
          cgen.Var(f'component_{key}', value.c_state_type.Star()),
          cgen.malloc(value.c_state_type.Sizeof()).Cast(value.c_state_type.Star()))
      value.generate_capnp_to_c_state(read_ctx.update_ptr(vProductStruct.Dot(key)), vComponent.Deref())
      key_to_expr[key] = vComponent

    read_ctx.block.AddAssignment(output_lvalue, cgen.StructureLiteral(
        struct=self._c_state_type, key_to_expr=key_to_expr))

  def generate_and_yield_capnp_to_c_transition(self, read_ctx):
    ptrStruct = read_ctx.block.AddDeclaration(cgen.Var('ptr_struct', self._capnp_transitions_type.c_structure_type))
    read_ctx.block.AddAssignment(None, self._capnp_transitions_type.c_read_function(ptrStruct.Address(), read_ctx.ptr))

    vList = ptrStruct.Dot('transitions')

    vItem = read_ctx.block.AddDeclaration(cgen.Var('list_item', self._capnp_single_transition_type.c_ptr_type))
    nTransitions = cgen.capn_len(vList)
    read_ctx.block.logf(f"Product is looping over %d capnp_transitions to deserialize them.\n", nTransitions)
    with read_ctx.block.ForInt(nTransitions) as (loop, vIndex):
      loop.AddAssignment(vItem.Dot('p'), cgen.capn_getp(vList.Dot('p'), vIndex, cgen.Zero))
      yield from self._generate_capnp_to_c_single_transition(read_ctx.update_block(loop).update_ptr(vItem))

  def _generate_capnp_to_c_single_transition(self, read_ctx):
    vSingleTransition = read_ctx.block.AddDeclaration(
        cgen.Var('transition_structure', self._capnp_single_transition_type.c_structure_type))
    read_ctx.block.AddAssignment(
        None, self._capnp_single_transition_type.c_read_function(vSingleTransition.Address(), read_ctx.ptr))
    switch = read_ctx.block.AddSwitch(vSingleTransition.Dot('which'))

    for ident in self._product_type.transition_identifiers:
      if ident == 'standard':
        yield from self._generate_capnp_to_c_single_transition_individual_components(
            read_ctx.compiler, switch, read_ctx.ptrsToFree, vSingleTransition)
      elif ident == 'individual':
        yield from self._generate_capnp_to_c_single_transition_individual_components(
            read_ctx.compiler, switch, read_ctx.ptrsToFree, vSingleTransition)
      elif ident == 'simultaneous':
        yield from self._generate_capnp_to_c_single_transition_simultaneous(read_ctx.compiler, switch,
                                                                            vSingleTransition)
      else:
        raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _generate_capnp_to_c_single_transition_individual_components(self, compiler, switch, ptrsToFree,
                                                                   vSingleTransition):
    for key, value in self._items:
      c_name = f'product_on_{key}'
      name = f"productOn{key}"
      block = switch.AddCase(self._capnp_single_transition_union.c_enum_option_by_name(name))
      componentPtr = vSingleTransition.Dot(name)

      i = 0
      for cblock, cexpr in value.generate_and_yield_capnp_to_c_transition(
          CapnpReadContext(compiler=compiler, block=block, ptrsToFree=ptrsToFree, ptr=componentPtr)):
        vFromValue = cblock.AddDeclaration(
            cgen.Var(f'from_value_{key}_{i}', value.c_transitions_type.Star()),
            cgen.malloc(value.c_transitions_type.Sizeof()).Cast(value.c_transitions_type.Star()))
        cblock.AddAssignment(None, cgen.kv_push(cgen.Void.Star(), ptrsToFree, vFromValue.Cast(cgen.Void.Star())))
        cblock.AddAssignment(vFromValue.Deref(), cexpr)
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

  def generate_free_state(self, compiler, block, stateRvalue):
    # FIXME(KK): Actually implement this
    pass

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
      self._items.append((key, compiler.get_concrete_type(value)))

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

  def generate_free_state(self, compiler, block, stateRvalue):
    # FIXME(KK): Actually implement this
    pass

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
    self.base = compiler.get_concrete_type(self._base_list_type.base)
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


class CapnpReadContext(object):
  '''Context object for code generators that generate C code that reads from a capnproto structure.'''

  def __init__(self, compiler, block, ptrsToFree, ptr):
    self.compiler = compiler
    self.block = block
    self.ptrsToFree = ptrsToFree
    self.ptr = ptr

  def _copy(self):
    return CapnpReadContext(compiler=self.compiler, block=self.block, ptrsToFree=self.ptrsToFree, ptr=self.ptr)

  def update_compiler(self, compiler):
    result = self._copy()
    result.compiler = compiler
    return result

  def update_block(self, block):
    result = self._copy()
    result.block = block
    return result

  def update_ptr(self, ptr):
    result = self._copy()
    result.ptr = ptr
    return result


class CapnpWriteContext(object):
  '''Context object for code generators that generate C code that writes to a capnproto structure.'''

  def __init__(self, compiler, block, segment, ptr):
    self.compiler = compiler
    self.block = block
    self.segment = segment
    self.ptr = ptr

  def _copy(self):
    return CapnpWriteContext(compiler=self.compiler, block=self.block, segment=self.segment, ptr=self.ptr)

  def update_compiler(self, compiler):
    result = self._copy()
    result.compiler = compiler
    return result

  def update_block(self, block):
    result = self._copy()
    result.block = block
    return result

  def update_segment(self, segment):
    result = self._copy()
    result.segment = segment
    return result

  def update_ptr(self, ptr):
    result = self._copy()
    result.ptr = ptr
    return result
