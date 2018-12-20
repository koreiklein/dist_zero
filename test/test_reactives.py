import pytest

from dist_zero import recorded, types, expression, reactive, primitive


class TestMultiplicativeReactive(object):
  def test_produce_inputs(self):
    compiler = reactive.ReactiveCompiler(name='test_simple_addition')
    net = compiler.compile({
        'output':
        expression.Applied(
            func=primitive.Plus(types.Int32),
            arg=expression.Product([
                ('left', expression.Input('a', types.Int32)),
                ('right', expression.Input('b', types.Int32)),
            ]),
        )
    })

    null_output = net.OnOutput_output()
    assert not null_output

    null_output = net.OnInput_a(2)
    assert not null_output

    first_output = net.OnInput_b(3)

    assert 5 == first_output['output']

    # FIXME(KK): Continue by submitting transitions to net

  def test_recorded_ints(self):
    indiscrete_int = types.Int32.With('indiscrete')

    changing_number = recorded.RecordedUser(
        'user',
        start=indiscrete_int.from_python(3),
        time_action_pairs=[
            (40, indiscrete_int.jump(1)),
            (70, indiscrete_int.jump(2)),
        ])

    constant_2 = primitive(indiscrete_int.from_python(2))

    thesum = op_plus.from_parts(indiscrete_int, changing_number)

    result = 'FIXME(KK): Finish this!'
