import pytest

from dist_zero import types, primitive, program

from dist_zero.compiler import cardinality, normalize, partition


class TestCardinality(object):
  def _norm_project_counter(self, normExpr):
    return normalize.Applied(arg=normExpr, p=primitive.Project('counter'))

  def test_cardinalizer_simple(self):
    c = cardinality.Cardinalizer()

    norm_web_input = normalize.NormWebInput(domain_name='www.example.com')
    zero = normalize.NormConstant(0)
    one = normalize.NormConstant(1)
    counter = self._norm_project_counter(norm_web_input.element_of)
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

  def test_cardinalizer_nested(self):
    norm_web_input = normalize.NormWebInput(domain_name='www.example.com')
    typical_web_node_list = self._norm_project_counter(norm_web_input.element_of)

    each_sum = normalize.Applied(
        arg=normalize.NormRecord([('left', typical_web_node_list.element_of), ('right', normalize.NormConstant(0))]),
        p=primitive.Plus(types.Int32))

    mapped = normalize.NormListOp(base=typical_web_node_list, opVariant='map', element_expr=each_sum)

    c = cardinality.Cardinalizer()
    assert len(c.cardinalize(each_sum)) == 2
    assert cardinality.Cardinality(list_exprs=[norm_web_input, typical_web_node_list]).equal(c.cardinalize(each_sum))
    assert cardinality.Cardinality(list_exprs=[norm_web_input]).equal(c.cardinalize(mapped))

  def test_cardinality_trie(self):
    a, b, c, d, e, f, g = [normalize.NormWebInput(f"www{i}.example.com") for i in range(7)]
    card = lambda elts: cardinality.Cardinality(elts)
    trie = cardinality.CardinalityTrie.build_trie([
        card([]),
        card([a, b, c]),
        card([a, b, d, e, f]),
        card([a, b, d, e, f, g]),
        card([a, b]),
    ])
    included_cards = list(trie.cardinalities())
    assert len(included_cards) == 5

    assert card([]).equal(trie.cardinality)
    kids = dict(trie.items())
    assert len(kids) == 1
    keys, second_trie = kids.pop(a)
    assert [a, b] == keys

    second_kids = dict(second_trie.items())
    assert 2 == len(second_kids)
    assert card([a, b]).equal(second_trie.cardinality)
    keys, left_trie = second_kids.pop(c)
    assert [c] == keys
    assert card([a, b, c]).equal(left_trie.cardinality)
    assert 0 == len(list(left_trie.items()))

    keys, right_trie = second_kids.pop(d)
    assert [d, e, f] == keys
    right_kids = dict(right_trie.items())
    assert card([a, b, d, e, f]).equal(right_trie.cardinality)
    assert 1 == len(right_kids)

    keys, final_trie = right_kids.pop(g)
    assert [g] == keys
    assert 0 == len(list(final_trie.items()))
    assert card([a, b, d, e, f, g]).equal(final_trie.cardinality)


class TestPartitioner(object):
  def test_partitioner(self):
    a, b, c, d, e, f, g = [normalize.NormWebInput(f"www{i}.example.com") for i in range(7)]
    card = lambda elts: cardinality.Cardinality(elts)

    glob = card([])
    a = card([a])
    abc = card([a, b, c])
    abdef = card([a, b, d, e, f])
    abdefg = card([a, b, d, e, f, g])
    ab = card([a, b])

    trie = cardinality.CardinalityTrie.build_trie([glob, a, abc, abdef, abdefg, ab])

    prog = program.DistributedProgram()

    class _MockCompiler(object):
      def list_is_large(self, expr):
        return expr in [b, e, g]

      def new_dataset(self, name):
        return prog.new_dataset(name)

    compiler = _MockCompiler()

    part = partition.Partitioner(compiler)

    # Calling the main partition method makes things harder to mock.
    # This "white box" test is willing to invade into the internal variables of Partition.
    global_ds = prog.new_dataset(name='global')
    part._partition_subtrie(trie, ds=global_ds)
    ds = part._cardinality_to_ds
    assert global_ds == ds[glob]
    assert global_ds == ds[a]
    assert global_ds != ds[ab]
    assert ds[ab] == ds[abc]
    assert ds[abc] != ds[abdef]
    assert ds[abdef] != ds[abdefg]


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
