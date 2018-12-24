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


@pytest.fixture(scope='module')
def program_C():
  self = _ProgramData()

  self.inputA = expression.Input('a', types.Int32)
  self.inputB = expression.Input('b', types.Int32)
  self.inputC = expression.Input('c', types.Int32)

  self.outputX = expression.Applied(
      func=primitive.Plus(types.Int32),
      arg=expression.Product([
          ('left', self.inputA),
          ('right', self.inputB),
      ]),
  )

  self.outputY = expression.Applied(
      func=primitive.Times(types.Int32),
      arg=expression.Product([
          ('left', self.outputX),
          ('right', self.inputB),
      ]),
  )

  self.outputZ = expression.Applied(
      func=primitive.Plus(types.Int32),
      arg=expression.Product([
          ('left', self.inputA),
          ('right',
           expression.Applied(
               func=primitive.Plus(types.Int32),
               arg=expression.Product([
                   ('left', self.inputB),
                   ('right', self.inputC),
               ]),
           )),
      ]),
  )

  self.compiler = reactive.ReactiveCompiler(name='test_complex_addition')
  self.module = self.compiler.compile({
      'x': self.outputX,
      'y': self.outputY,
      'z': self.outputZ,
  })

  self.capnpForA = self.compiler.capnp_state_module(self.inputA)
  self.capnpForB = self.compiler.capnp_state_module(self.inputB)
  self.capnpForC = self.compiler.capnp_state_module(self.inputC)

  self.capnpForX = self.compiler.capnp_state_module(self.outputX)
  self.capnpForY = self.compiler.capnp_state_module(self.outputY)
  self.capnpForZ = self.compiler.capnp_state_module(self.outputZ)

  return self


class TestMultiplicativeReactive(object):
  def test_complex_initialization(self, program_C):
    net = program_C.module.Net()

    assert not net.OnInput_a(program_C.capnpForA.new_message(basicState=2).to_bytes())
    assert not net.OnOutput_y()
    assert not net.OnOutput_z()

    first_output = net.OnInput_b(program_C.capnpForB.new_message(basicState=3).to_bytes())
    assert 2 == len(first_output)
    assert 5 == program_C.capnpForX.from_bytes(first_output['x']).basicState
    assert 15 == program_C.capnpForY.from_bytes(first_output['y']).basicState

    second_output = net.OnInput_c(program_C.capnpForC.new_message(basicState=7).to_bytes())
    assert len(second_output) == 1
    assert 12 == program_C.capnpForZ.from_bytes(second_output['z']).basicState

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
