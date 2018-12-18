import math
import os
import tempfile

import pytest

from dist_zero import types, type_compiler, cgen

TwoByThree = types.Product([
    ('left', types.Two),
    ('right', types.Three),
])

X = types.Product([
    ('left', types.Three),
    ('right', types.List(types.Two)),
])


@pytest.mark.parametrize('t', [
    types.List(types.Two).With('append', 'insert', 'remove', 'onIndex', 'indiscrete'),
    types.Product([
        ('left', types.Three),
        ('right', types.List(types.Two).Discrete()),
    ]).Discrete().With('indiscrete', 'simultaneous'),
    types.List(types.List(types.Two).With('append', 'indiscrete', 'onIndex')).With('append', 'remove', 'onIndex',
                                                                                   'indiscrete'),
    X,
])
def test_type_to_capnp_and_c(t):
  f = type_compiler.CapnpTypeCompiler()

  f.ensure_root_type(t)

  dirname = os.path.join('.tmp', 'capnp')
  os.makedirs(dirname, exist_ok=True)
  f.capnp.build_in(dirname=dirname, filename='example.capnp')

  cf = type_compiler.CTypeCompiler(cgen.Program(name="test_program"))
  cf.ensure_root_type(t)

  cf.cprogram.build_and_import()


def test_type_cardinalities():
  assert 1 == abs(types.One)
  assert 2 == abs(types.Two)
  assert 3 == abs(types.Three)
  assert 4 == abs(types.Four)
  assert 5 == abs(types.Five)

  assert math.inf == abs(types.List(types.Three))
  assert math.inf == abs(types.List(types.Two))
  assert math.inf == abs(types.List(types.One))
  assert 1 == abs(types.List(types.Zero))

  assert 6 == abs(TwoByThree)

  assert 6 == abs(types.Sum([
      ('left', types.Three),
      ('middle', types.Two),
      ('right', types.One),
  ]))
