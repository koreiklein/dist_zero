import pytest

from dist_zero import types, primitive

from dist_zero.compiler import cardinality, normalize


def _norm_project_counter(normExpr):
  return normalize.Applied(arg=normExpr, p=primitive.Project('counter'))


def test_cardinalizer_simple(dz):
  c = cardinality.Cardinalizer()

  norm_web_input = normalize.NormWebInput(domain_name='www.example.com')
  zero = normalize.NormConstant(0)
  one = normalize.NormConstant(1)
  counter = _norm_project_counter(norm_web_input.element_of)
  record = normalize.NormRecord([
      ('a', one),
      ('b', counter.case_of('nonzero')),
  ])
  expr = normalize.NormCase(
      base=counter, items=[
          ('zero', zero),
          ('nonzero', record),
      ])
  assert 0 == len(c.cardinalize(norm_web_input))
  assert 0 == len(c.cardinalize(zero))
  assert 0 == len(c.cardinalize(one))

  elt_cardinality = cardinality.Cardinality(list_exprs=[norm_web_input])
  assert elt_cardinality.equal(c.cardinalize(norm_web_input.element_of))
  assert elt_cardinality.equal(c.cardinalize(expr))
  assert elt_cardinality.equal(c.cardinalize(record))


def test_cardinalizer_nested(dz):
  norm_web_input = normalize.NormWebInput(domain_name='www.example.com')
  typical_web_node_list = _norm_project_counter(norm_web_input.element_of)

  each_sum = normalize.Applied(
      arg=normalize.NormRecord([('left', typical_web_node_list.element_of), ('right', normalize.NormConstant(0))]),
      p=primitive.Plus(types.Int32))

  mapped = normalize.NormListOp(base=typical_web_node_list, opVariant='map', element_expr=each_sum)

  c = cardinality.Cardinalizer()
  assert len(c.cardinalize(each_sum)) == 2
  assert cardinality.Cardinality(list_exprs=[norm_web_input, typical_web_node_list]).equal(c.cardinalize(each_sum))
  assert cardinality.Cardinality(list_exprs=[norm_web_input]).equal(c.cardinalize(mapped))


@pytest.mark.asyncio
async def test_single_node(dz, demo):
  mainExpr = dz.RecordedUser(
      'recording',
      start=3,
      type=types.Int32,
      time_action_pairs=[
          (1400, [('inc', -2)]),
          (1700, [('inc', 1)]),
          (1950, [('inc', 4)]),
      ]).Spy('out')

  program = dz.compiler().compile(mainExpr)
  out_dataset = program.GetDatasetId(spy_key='out')
  # FIXME(KK): Rethink how to start a program and get the below to pass.
  demo.start_program(program)
  demo.run_for(ms=1000)
  assert 0 == demo.system.get_height(out_dataset)
  spy_result = demo.system.spy(out_dataset, 'out')
  ipdb.set_trace()
