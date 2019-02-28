import pytest

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
