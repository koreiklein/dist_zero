import pytest

from dist_zero import types
from dist_zero.compiler import normalize
import test.common


@pytest.fixture
def normalizer():
  return normalize.Normalizer()


def test_normalize_simple(dz, normalizer):
  norm = normalizer.normalize(dz.Record(left=dz.Constant(2), right=dz.Constant(3)))
  assert norm.equal(normalize.NormRecord([('left', normalize.NormConstant(2)), ('right', normalize.NormConstant(3))]))
  assert not norm.equal(
      normalize.NormRecord([('loft', normalize.NormConstant(2)), ('right', normalize.NormConstant(3))]))


def test_normalize_function(dz, normalizer):
  norm = normalizer.normalize(dz.Lambda(lambda value: dz.Record(left=dz.Constant(2), right=value))(dz.Constant(3)))
  assert norm.equal(normalize.NormRecord([('left', normalize.NormConstant(2)), ('right', normalize.NormConstant(3))]))


def test_normalize_applications(dz, normalizer):
  record = dz.Record(left=dz.Constant(4), right=dz.Record(a=dz.Constant(5), b=dz.Constant(3)))
  getLeft = dz.Project('left')
  getB = dz.Project('b')
  functions = dz.Record(left=getB, middle=getLeft, last=dz.Lambda(lambda value: value['right']['b']))
  extract = functions['last']
  norm = normalizer.normalize(extract(record))
  assert norm.equal(normalize.NormConstant(3))


def test_normalize_case(dz, normalizer):
  onLeft = dz.Constant(4).Inject('left')
  onRight = dz.Constant(2).Inject('right')

  for value, expected in [(onLeft, 0), (onRight, 1)]:
    expr = dz.Case(value, left=dz.Lambda(lambda value: dz.Constant(0)), right=dz.Lambda(lambda value: dz.Constant(1)))

    assert normalizer.normalize(expr).equal(normalize.NormConstant(expected))


def test_normalize_complex(dz, normalizer):
  t = dz.Project('middle')
  f = dz.Lambda(lambda value: dz.Record(left=value['a'], middle=value['a'], right=value['b']))
  g = dz.Lambda(lambda value: dz.Case(value, x=f, y=dz.Lambda(lambda value: value['c'])))

  abc = dz.Record(a=dz.Constant(3), b=dz.Constant(4), c=dz.Constant(5))
  expr = t(g(abc.Inject('x')))
  assert normalizer.normalize(expr).equal(normalize.NormConstant(3))

  expr = g(abc.Inject('y'))
  assert normalizer.normalize(expr).equal(normalize.NormConstant(5))
