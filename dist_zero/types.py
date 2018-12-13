import math
import os
import random

from dist_zero import capnpgen, errors

rand = random.Random("types")

lowerChars = 'pyfgcrlaoeuidhtnsqjkxbmwvz'
chars = lowerChars + lowerChars.upper()
n_names = [0]


def _gen_name():
  result = f"{''.join(rand.choice(chars) for i in range(6))}{n_names[0]}"
  n_names[0] += 1
  return result


class TypeCompiler(object):
  def __init__(self):
    self.added_types_states = set()
    self.unadded_root_types_states = set()

    self.added_types_transitions = set()
    self.unadded_root_types_transitions = set()

    self.capnp = capnpgen.CapnpFile(capnpgen.gen_capn_uid())

  def ensure_root_type_transitions(self, t):
    if t not in self.added_types_transitions:
      self.unadded_root_types_transitions.add(t)

    return self

  def ensure_root_type(self, t):
    self.ensure_root_type_states(t)
    self.ensure_root_type_transitions(t)

  def ensure_root_type_states(self, t):
    '''
    Guarantees that a definition of t will occur in the root of this file.
    In case t is already included in the root, this method does nothing.
    '''
    if t not in self.added_types_states:
      self.unadded_root_types_states.add(t)

    return self

  def build_capnp(self):
    '''
    Generate and return a CapnpFile instance defining all the states and transitions of the root types that are ensured.
    '''
    while self.unadded_root_types_states:
      to_iterate = list(self.unadded_root_types_states)
      self.unadded_root_types_states = set()
      for t in to_iterate:
        if t not in self.added_types_states:
          self.added_types_states.add(t)
          t._write_state_definition(self)

    while self.unadded_root_types_transitions:
      to_iterate = list(self.unadded_root_types_transitions)
      self.unadded_root_types_transitions = set()
      for t in to_iterate:
        if t not in self.added_types_transitions:
          self.added_types_transitions.add(t)
          t._write_transitions_definition(self)

    return self.capnp


class Type(object):
  def _write_state_definition(self, compiler):
    '''Add a definition for the states of this type at the current spot in the capnp file.'''
    raise RuntimeError("Abstract Superclass")

  def _transitions_structure_name(self):
    '''return the name to use for the structure with this type's transitions..'''
    raise RuntimeError('Abstract Superclass')

  def _write_transition_definition(self, compiler, ident, union):
    '''Add a definition for the transitions of this type matching ``ident`` to the capnp union object.'''
    raise RuntimeError('Abstract Superclass')

  def _reference_string(self, capnp):
    '''return a string reference to this type's state structure.'''
    raise RuntimeError('Abstract Superclass')

  def transition_reference(self, compiler):
    compiler.ensure_root_type_transitions(self)
    return self._transitions_structure_name()

  def state_reference(self, compiler):
    compiler.ensure_root_type_states(self)
    return self._reference_string(compiler)

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
    union.AddField("jump", self.state_reference(compiler))

  def _write_transition_identifiers(self, compiler, union):
    for ti in self.transition_identifiers:
      if ti == 'indiscrete':
        self._write_indiscrete_transition_definition(compiler, union)
      else:
        self._write_transition_definition(compiler, ti, union)

  def _write_transitions_definition(self, compiler):
    if len(self.transition_identifiers) == 0:
      pass
    else:
      struct = compiler.capnp.AddStructure(self._transitions_structure_name())
      union = struct.AddUnion()
      self._write_transition_identifiers(compiler, union)

      if union.RemoveIfTooSmall():
        self._write_transition_identifiers(compiler, struct)


class BasicType(Type):
  def __init__(self, s):
    self.s = s
    super(BasicType, self).__init__()

  def _transitions_structure_name(self):
    return f"{self.s}Transition"

  def _write_transition_definition(self, compiler, ident, union):
    raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _write_state_definition(self, compiler):
    pass

  def _reference_string(self, compiler):
    return self.s


class Product(Type):
  def __init__(self, items, name=None):
    self.items = items
    self.d = dict(items)
    self.name = name if name is not None else f"Product{_gen_name()}"

    self._wrote_simulatenous = False
    self._wrote_individual = False

    super(Product, self).__init__()

  def _transitions_structure_name(self):
    return f"{self.name}Transition"

  def _write_simultaneous_components_transition_definitions(self, compiler, union):
    if not self._wrote_simulatenous:
      self._wrote_simulatenous = True
      simultaneousStructName = f"{self.name}Simultaneous"
      union.AddField("simultaneous", simultaneousStructName)

      struct = compiler.capnp.AddStructure(simultaneousStructName)
      for key, value in self.items:
        struct.AddField(key, value.state_reference(compiler))

  def _write_individual_components_transition_definitions(self, compiler, union):
    if not self._wrote_individual:
      self._wrote_individual = True
      for key, value in self.items:
        union.AddField(f"productOn{key}", value.transition_reference(compiler))

  def _write_transition_definition(self, compiler, ident, union):
    if ident == 'standard':
      self._write_individual_components_transition_definitions(compiler, union)
      self._write_simultaneous_components_transition_definitions(compiler, union)
    elif ident == 'individual':
      self._write_individual_components_transition_definitions(compiler, union)
    elif ident == 'simultaneous':
      self._write_simultaneous_components_transition_definitions(compiler, union)
    else:
      raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def __abs__(self):
    result = 1
    for k, x in self.items:
      result *= abs(x)
    return result

  def _reference_string(self, compiler):
    return self.name

  def _write_state_definition(self, compiler):
    struct = compiler.capnp.AddStructure(self.name)

    for key, value in self.items:
      struct.AddField(key, value.state_reference(compiler))


class Sum(Type):
  def __init__(self, items, name=None):
    self.items = items
    self.d = dict(items)
    self.name = name if name is not None else f"Sum{_gen_name()}"
    super(Sum, self).__init__()

  def _transitions_structure_name(self):
    return f"{self.name}Transition"

  def _reference_string(self, compiler):
    return self.name

  def _write_individual_components_transition_definitions(self, compiler, union):
    for key, value in self.items:
      union.AddField(f"sumOn{key}", value.transition_reference(compiler))

  def _write_transition_definition(self, compiler, ident, union):
    if ident == 'standard':
      self._write_individual_components_transition_definitions(compiler, union)
    else:
      raise RuntimeError(f"Unrecognized transition identifier {ident}.")

  def _write_state_definition(self, compiler):
    struct = compiler.capnp.AddStructure(self.name)
    if len(self.items) == 0:
      pass
    elif len(self.items) == 0:
      for key, value in self.items:
        struct.AddField(key, value.state_reference(compiler))
    else:
      union = struct.AddUnion()
      for key, value in self.items:
        union.AddField(key, value.state_reference(compiler))

  def __abs__(self):
    return sum(abs(x) for k, x in self.items)


class List(Type):
  def __init__(self, base, name=None):
    self.name = name if name is not None else f"List{_gen_name()}"
    self.base = base
    super(List, self).__init__()

  def _transitions_structure_name(self):
    return f"{self.name}Transition"

  def _write_append_transition_definition(self, compiler, union):
    union.AddField('append', self.base.state_reference(compiler))

  def _write_insert_transition_definition(self, compiler, union):
    insertTransitionTypeName = f"InsertIn{self.name}"

    insertStruct = compiler.capnp.AddStructure(insertTransitionTypeName)
    insertStruct.AddField('index', capnpgen.UInt32)
    insertStruct.AddField('value', self.base.state_reference(compiler))

    union.AddField('insert', insertTransitionTypeName)

  def _write_on_index_transition_definition(self, compiler, union):
    onIndexTypeName = f"OnIndex{self.name}"

    onIndexStruct = compiler.capnp.AddStructure(onIndexTypeName)
    onIndexStruct.AddField('index', capnpgen.UInt32)
    onIndexStruct.AddField('transition', self.base.transition_reference(compiler))

    union.AddField('onIndex', onIndexTypeName)

  def _write_remove_transition_definition(self, compiler, union):
    removeTransitionTypeName = f"RemoveFrom{self.name}"
    union.AddField('remove', capnpgen.UInt32)

  def _write_transition_definition(self, compiler, ident, union):
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

  def _write_state_definition(self, compiler):
    compiler.ensure_root_type_states(self.base)

  def _reference_string(self, compiler):
    return f"List({self.base.state_reference(compiler)})"

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
