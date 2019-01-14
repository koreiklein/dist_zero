import math
import random

from dist_zero import errors, cgen

rand = random.Random("types")

lowerChars = 'pyfgcrlaoeuidhtnsqjkxbmwvz'
chars = lowerChars + lowerChars.upper()
n_names = [0]


def _gen_name():
  result = f"{''.join(rand.choice(chars) for i in range(6))}{n_names[0]}"
  n_names[0] += 1
  return result


class Type(object):
  '''
  Abstract base class for DistZero input types.
  Instances of `Type` represent the types employed by the user constructing his input program.
  '''

  def _write_c_state_definition(self, compiler):
    '''Add a definition for the c states of this type.'''
    raise RuntimeError(f"Abstract Superclass {self.__class__}")

  def equivalent(self, other):
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


class FunctionType(Type):
  def __init__(self, src, tgt):
    self.src = src
    self.tgt = tgt

  def equivalent(self, other):
    return other.__class__ == FunctionType and self.src.equivalent(other.src) and self.tgt.equivalent(other.tgt)


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

  def equivalent(self, other):
    return other.__class__ == BasicType and \
        self.capnp_state == other.capnp_state and \
        self.c_state_type == other.c_state_type and \
        self.capnp_transition_type == other.capnp_transition_type and \
        self.c_transition_type == other.c_transition_type

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

  def __abs__(self):
    result = 1
    for k, x in self.items:
      result *= abs(x)
    return result


class Sum(Type):
  def __init__(self, items, name=None):
    self.items = items
    self.d = dict(items)
    self.name = name if name is not None else f"Sum{_gen_name()}"
    super(Sum, self).__init__()

  def __abs__(self):
    return sum(abs(x) for k, x in self.items)


class List(Type):
  def __init__(self, base, name=None):
    self.name = name if name is not None else f"List{_gen_name()}"
    self.base = base
    super(List, self).__init__()

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
