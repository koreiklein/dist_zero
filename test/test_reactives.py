import pytest

from dist_zero import recorded, types, expression, reactive, primitive


class TestMultiplicativeReactive(object):
  def test_reactive_errors(self, program_X):
    net = program_X.module.Net()

    for weird in [[], 7, None]:
      with pytest.raises(TypeError):
        net.OnInput_a(weird)

    with pytest.raises(program_X.module.BadReactiveInput):
      net.OnInput_a('this can definitely not be parsed into a proper network message')

    with pytest.raises(program_X.module.BadReactiveInput):
      net.OnInput_a(b'this can definitely not be parsed into a proper network message')

    assert not net.OnInput_a(program_X.capnpForA.new_message(basicState=4).to_bytes())
    assert not net.OnInput_b(program_X.capnpForB.new_message(basicState=1).to_bytes())

    assert 1 == len(net.OnOutput_output())

    assert not net.OnTransitions({})
    with pytest.raises(program_X.module.BadReactiveInput):
      net.OnTransitions({'not a valid key': []})

    # Empty transitions for "a" succeeds
    net.OnTransitions({'a': []})

    net = program_X.module.Net()
    with pytest.raises(program_X.module.BadReactiveInput):
      # Empty transitions for "a" fails since "a" has not been initialized.
      net.OnTransitions({'a': []})

  def test_update_product_state(self, program_Z):
    net = program_Z.module.Net()

    assert not net.OnInput_a(program_Z.capnpForA.new_message(basicState=7).to_bytes())
    assert not net.OnInput_b(program_Z.capnpForB.new_message(basicState=2).to_bytes())

    output = net.OnOutput_x()

    # This update should change the state of the product expression in program_Z
    assert 1 == len(net.OnTransitions({
        'a': [program_Z.capnpForA_T.new_message(basicTransition=1).to_bytes()],
    }))

    output = net.OnOutput_y()
    assert 1 == len(output)
    # If the product expression's state was not properly kept up to date, the output will erroneously contain 9
    assert 10 == program_Z.capnpForY.from_bytes(output['y']).basicState

  def test_multiple_output(self, program_Y):
    net = program_Y.module.Net()
    assert not net.OnOutput_y()
    output = net.OnInput_b(program_Y.capnpForB.new_message(basicState=3).to_bytes())
    assert 1 == len(output)
    assert 6 == program_Y.capnpForY.from_bytes(output['y']).basicState

    output = net.OnTransitions({
        'b': [program_Y.capnpForB_T.new_message(basicTransition=1).to_bytes()],
    })
    assert 1 == len(output)
    assert 2 == program_Y.capnpForY_T.from_bytes(output['y']).basicTransition

    assert not net.OnInput_a(program_Y.capnpForA.new_message(basicState=10).to_bytes())

    output = net.OnOutput_x()
    assert 1 == len(output)
    assert 14 == program_Y.capnpForX.from_bytes(output['x']).basicState

  def test_output_then_input(self, program_X):
    net = program_X.module.Net()

    assert not net.OnOutput_output()
    assert not net.OnInput_a(program_X.capnpForA.new_message(basicState=2).to_bytes())
    first_output = net.OnInput_b(program_X.capnpForA.new_message(basicState=3).to_bytes())

    result = program_X.capnpForOutput.from_bytes(first_output['output'])
    assert 5 == result.basicState

    transition_output = net.OnTransitions({
        'a': [
            program_X.capnpForA_T.new_message(basicTransition=2).to_bytes(),
        ],
        'b': [
            program_X.capnpForA_T.new_message(basicTransition=2).to_bytes(),
            program_X.capnpForA_T.new_message(basicTransition=6).to_bytes(),
        ],
    })
    assert 1 == len(transition_output)
    output = program_X.capnpForOutput_T.from_bytes(transition_output['output'])
    assert 10 == output.basicTransition

  def test_input_then_output(self, program_X):
    net = program_X.module.Net()

    assert not net.OnInput_a(program_X.capnpForA.new_message(basicState=2).to_bytes())
    assert not net.OnInput_b(program_X.capnpForA.new_message(basicState=3).to_bytes())
    first_output = net.OnOutput_output()

    result = program_X.capnpForOutput.from_bytes(first_output['output'])
    assert 5 == result.basicState

    assert 0 == len(net.OnTransitions({}))
    transition_output = net.OnTransitions({'a': []})
    assert 1 == len(transition_output)
    assert 0 == program_X.capnpForOutput_T.from_bytes(transition_output['output']).basicTransition

    transition_output = net.OnTransitions({
        'a': [program_X.capnpForA_T.new_message(basicTransition=2).to_bytes()],
    })
    assert 1 == len(transition_output)
    output = program_X.capnpForOutput_T.from_bytes(transition_output['output'])
    assert 2 == output.basicTransition

    transition_output = net.OnTransitions({
        'b': [program_X.capnpForB_T.new_message(basicTransition=3).to_bytes()],
    })
    assert 1 == len(transition_output)
    output = program_X.capnpForOutput_T.from_bytes(transition_output['output'])
    assert 3 == output.basicTransition

    transition_output = net.OnTransitions({
        'a': [
            program_X.capnpForA_T.new_message(basicTransition=2).to_bytes(),
        ],
        'b': [
            program_X.capnpForA_T.new_message(basicTransition=2).to_bytes(),
            program_X.capnpForA_T.new_message(basicTransition=6).to_bytes(),
        ],
    })
    assert 1 == len(transition_output)
    output = program_X.capnpForOutput_T.from_bytes(transition_output['output'])
    assert 10 == output.basicTransition

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

    # FIXME(KK): Finish this!

  def test_complex_initialization(self, program_C):
    net = program_C.module.Net()

    assert not net.OnInput_a(program_C.capnpForA.new_message(basicState=2).to_bytes())
    assert not net.OnOutput_y()
    assert not net.OnOutput_z()

    first_output = net.OnInput_b(program_C.capnpForB.new_message(basicState=3).to_bytes())
    assert 2 == len(first_output)
    assert 5 == program_C.capnpForX.from_bytes(first_output['x']).basicState
    assert 8 == program_C.capnpForY.from_bytes(first_output['y']).basicState

    second_output = net.OnInput_c(program_C.capnpForC.new_message(basicState=7).to_bytes())
    assert len(second_output) == 1
    assert 12 == program_C.capnpForZ.from_bytes(second_output['z']).basicState

    output = net.OnTransitions({
        'b': [program_C.capnpForB_T.new_message(basicTransition=6).to_bytes()],
    })

    assert 3 == len(output)

    assert 6 == program_C.capnpForX_T.from_bytes(output['x']).basicTransition
    assert 12 == program_C.capnpForY_T.from_bytes(output['y']).basicTransition
    assert 6 == program_C.capnpForZ_T.from_bytes(output['z']).basicTransition


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

  self.compiler = reactive.ReactiveCompiler(name='program_X')
  self.module = self.compiler.compile({'output': self.outputExpr})

  self.capnpForA = self.compiler.capnp_state_module(self.inputA)
  self.capnpForA_T = self.compiler.capnp_transitions_module(self.inputA)
  self.capnpForB = self.compiler.capnp_state_module(self.inputB)
  self.capnpForB_T = self.compiler.capnp_transitions_module(self.inputB)
  self.capnpForOutput = self.compiler.capnp_state_module(self.outputExpr)
  self.capnpForOutput_T = self.compiler.capnp_transitions_module(self.outputExpr)

  return self


@pytest.fixture(scope='module')
def program_Y():
  self = _ProgramData()

  self.inputA = expression.Input('a', types.Int32)
  self.inputB = expression.Input('b', types.Int32)
  self.outputX = expression.Applied(
      func=primitive.Plus(types.Int32),
      arg=expression.Product([
          ('left', self.inputA),
          ('right', self.inputB),
      ]),
  )
  self.outputY = expression.Applied(
      func=primitive.Plus(types.Int32), arg=expression.Product([
          ('left', self.inputB),
          ('right', self.inputB),
      ]))

  self.compiler = reactive.ReactiveCompiler(name='program_Y')
  self.module = self.compiler.compile({'x': self.outputX, 'y': self.outputY})

  self.capnpForA = self.compiler.capnp_state_module(self.inputA)
  self.capnpForA_T = self.compiler.capnp_transitions_module(self.inputA)
  self.capnpForB = self.compiler.capnp_state_module(self.inputB)
  self.capnpForB_T = self.compiler.capnp_transitions_module(self.inputB)
  self.capnpForX = self.compiler.capnp_state_module(self.outputX)
  self.capnpForX_T = self.compiler.capnp_transitions_module(self.outputX)
  self.capnpForY = self.compiler.capnp_state_module(self.outputY)
  self.capnpForY_T = self.compiler.capnp_transitions_module(self.outputY)

  return self


@pytest.fixture(scope='module')
def program_Z():
  self = _ProgramData()

  self.inputA = expression.Input('a', types.Int32)
  self.inputB = expression.Input('b', types.Int32)

  self.product = expression.Product([
      ('left', self.inputA),
      ('right', self.inputB),
  ])

  self.outputX = expression.Applied(func=primitive.Plus(types.Int32), arg=self.product)
  self.outputY = expression.Applied(func=primitive.Plus(types.Int32), arg=self.product)

  self.compiler = reactive.ReactiveCompiler(name='program_Z')
  self.module = self.compiler.compile({'x': self.outputX, 'y': self.outputY})

  self.capnpForA = self.compiler.capnp_state_module(self.inputA)
  self.capnpForA_T = self.compiler.capnp_transitions_module(self.inputA)
  self.capnpForB = self.compiler.capnp_state_module(self.inputB)
  self.capnpForB_T = self.compiler.capnp_transitions_module(self.inputB)
  self.capnpForX = self.compiler.capnp_state_module(self.outputX)
  self.capnpForX_T = self.compiler.capnp_transitions_module(self.outputX)
  self.capnpForY = self.compiler.capnp_state_module(self.outputY)
  self.capnpForY_T = self.compiler.capnp_transitions_module(self.outputY)

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
      func=primitive.Plus(types.Int32),
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

  self.compiler = reactive.ReactiveCompiler(name='program_C')
  self.module = self.compiler.compile({
      'x': self.outputX,
      'y': self.outputY,
      'z': self.outputZ,
  })

  self.capnpForA = self.compiler.capnp_state_module(self.inputA)
  self.capnpForB = self.compiler.capnp_state_module(self.inputB)
  self.capnpForC = self.compiler.capnp_state_module(self.inputC)

  self.capnpForA_T = self.compiler.capnp_transitions_module(self.inputA)
  self.capnpForB_T = self.compiler.capnp_transitions_module(self.inputB)
  self.capnpForC_T = self.compiler.capnp_transitions_module(self.inputC)

  self.capnpForX = self.compiler.capnp_state_module(self.outputX)
  self.capnpForY = self.compiler.capnp_state_module(self.outputY)
  self.capnpForZ = self.compiler.capnp_state_module(self.outputZ)

  self.capnpForX_T = self.compiler.capnp_transitions_module(self.outputX)
  self.capnpForY_T = self.compiler.capnp_transitions_module(self.outputY)
  self.capnpForZ_T = self.compiler.capnp_transitions_module(self.outputZ)

  return self
