import pytest

from dist_zero import recorded, types, expression, reactive, primitive


class _ProgramData(object):
  pass


@pytest.fixture(scope='module')
def program_X():
  self = _ProgramData()

  self.inputA = expression.Input('a', types.Int32)
  self.inputB = expression.Input('b', types.Int32)
  self.outputExpr = expression.Applied(
      func=primitive.Plus(types.Int32),
      arg=expression.Product([
          ('left', self.inputA),
          ('right', self.inputB),
      ]),
  )

  self.compiler = reactive.ReactiveCompiler(name='test_simple_addition')
  self.module = self.compiler.compile({'output': self.outputExpr})

  self.capnpForA = self.compiler.capnp_state_module(self.inputA)
  self.capnpForB = self.compiler.capnp_state_module(self.inputB)
  self.capnpForOutput = self.compiler.capnp_state_module(self.outputExpr)

  return self


class TestMultiplicativeReactive(object):
  def test_output_then_input(self, program_X):
    net = program_X.module.Net()

    assert not net.OnOutput_output()
    assert not net.OnInput_a(program_X.capnpForA.new_message(basicState=2).to_bytes())
    first_output = net.OnInput_b(program_X.capnpForA.new_message(basicState=3).to_bytes())

    result = program_X.capnpForOutput.from_bytes(first_output['output'])
    assert 5 == result.basicState

    # FIXME(KK): Continue by submitting transitions to net

  def test_input_then_output(self, program_X):
    net = program_X.module.Net()

    assert not net.OnInput_a(program_X.capnpForA.new_message(basicState=2).to_bytes())
    assert not net.OnInput_b(program_X.capnpForA.new_message(basicState=3).to_bytes())
    first_output = net.OnOutput_output()

    result = program_X.capnpForOutput.from_bytes(first_output['output'])
    assert 5 == result.basicState

    # FIXME(KK): Continue by submitting transitions to net

  @pytest.mark.skip
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
