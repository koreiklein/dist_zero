from dist_zero import capnpgen, errors


class CTypeCompiler(object):
  def __init__(self, cprogram):
    self.type_to_state_ctype = {}
    self.type_to_transitions_ctype = {}

    self.cprogram = cprogram

  def ensure_root_type(self, t):
    self.c_state_ref(t)
    self.c_transitions_ref(t)
    return self

  def c_state_ref(self, t):
    if t not in self.type_to_state_ctype:
      result = t._write_c_state_definition(self)
      self.type_to_state_ctype[t] = result
      return result
    else:
      return self.type_to_state_ctype[t]

  def c_transitions_ref(self, t):
    if t not in self.type_to_transitions_ctype:
      result = t._write_c_transitions_definition(self)
      self.type_to_transitions_ctype[t] = result
      return result
    else:
      return self.type_to_transitions_ctype[t]


class CapnpTypeCompiler(object):
  def __init__(self):
    self.type_to_state_ref = {}
    self.type_to_transitions_ref = {}

    self.capnp = capnpgen.CapnpFile(capnpgen.gen_capn_uid())

  def ensure_root_type(self, t):
    self.capnp_state_ref(t)
    self.capnp_transitions_ref(t)
    return self

  def capnp_state_ref(self, t):
    '''
    Guarantees that a definition of t will occur in the root of this file.
    In case t is already included in the root, this method does nothing.
    '''
    if t not in self.type_to_state_ref:
      ref = t._write_capnp_state_definition(self)
      self.type_to_state_ref[t] = ref
      return ref
    else:
      return self.type_to_state_ref[t]

  def capnp_transitions_ref(self, t):
    if t not in self.type_to_transitions_ref:
      ref = t._write_capnp_transitions_definition(self)
      self.type_to_transitions_ref[t] = ref
      return ref
    else:
      return self.type_to_transitions_ref[t]
