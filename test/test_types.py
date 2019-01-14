import math
import os
import tempfile

import pytest

from dist_zero import types, cgen
from dist_zero.reactive.compiler import ReactiveCompiler

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
  compiler = ReactiveCompiler(name='test_type_to_capnp_and_c')

  compiler.get_concrete_type(t)

  compiler._build_capnp()
  compiler.program.build_and_import()


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
