import pytest

from dist_zero import recorded, types, expression, reactive, primitive


class TestMultiplicativeReactive(object):
  def test_interleave_recorded(self):
    indiscrete_int = types.Int32

    a = recorded.RecordedUser(
        'user',
        start=3,
        type=indiscrete_int,
        time_action_pairs=[
            (40, [('inc', -2)]),
            (70, [('inc', 1)]),
            (75, [('inc', 4)]),
        ])

    b = recorded.RecordedUser(
        'user',
        start=1,
        type=indiscrete_int,
        time_action_pairs=[
            (30, [('inc', 20)]),
            (40, [('inc', 10)]),
            (50, [('inc', 8)]),
        ])

    compiler = reactive.ReactiveCompiler(name='test_interleave_recorded')
    thesum = program_plus(a, b)
    module = compiler.compile({'thesum': thesum})
    net = module.Net()

    assert 4 == compiler.capnp_state_builder(thesum).from_bytes(net.OnOutput_thesum()['thesum']).basicState
    assert not net.Elapse(20)

    capnpForT = compiler.capnp_transitions_builder(thesum)

    assert 28 == capnpForT.from_bytes(net.Elapse(25)['thesum']).basicTransition
    assert 13 == capnpForT.from_bytes(net.Elapse(100)['thesum']).basicTransition

  def test_recorded_ints(self):
    indiscrete_int = types.Int32

    changing_number = recorded.RecordedUser(
        'user',
        start=3,
        type=indiscrete_int,
        time_action_pairs=[
            (40, [('inc', -2)]),
            (70, [('inc', 1)]),
            (75, [('inc', 4)]),
        ])

    constant_2 = expression.Constant(2, type=types.Int32)

    compiler = reactive.ReactiveCompiler(name='test_recorded_ints')
    thesum = program_plus(constant_2, changing_number)
    module = compiler.compile({'thesum': thesum})
    net = module.Net()

    capnpForOutput = compiler.capnp_state_builder(thesum)
    capnpForOutput_T = compiler.capnp_transitions_builder(thesum)

    output = net.OnOutput_thesum()
    assert 1 == len(output)
    assert 5 == capnpForOutput.from_bytes(output['thesum']).basicState

    assert 0 == net.CurTime()
    assert 40 == net.NextTime()
    assert not net.Elapse(20)
    assert 20 == net.CurTime()
    assert 40 == net.NextTime()
    assert not net.Elapse(10)

    output = net.Elapse(15)
    assert 1 == len(output)
    assert -2 == capnpForOutput_T.from_bytes(output['thesum']).basicTransition

    assert 45 == net.CurTime()
    assert 70 == net.NextTime()
    output = net.Elapse(30)
    assert 1 == len(output)
    assert 5 == capnpForOutput_T.from_bytes(output['thesum']).basicTransition

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

  def test_nested_products(self, program_U):
    net = program_U.module.Net()

    assert not net.OnOutput_output()
    assert not net.OnInput_b(program_U.capnpForB.new_message(basicState=2).to_bytes())
    aInput = program_U.capnpForA.new_message()
    aInput.left.basicState = 5
    aInput.right.x.basicState = 3
    aInput.right.y.basicState = 1

    result = net.OnInput_a(aInput.to_bytes())
    assert 1 == len(result)
    assert 8 == program_U.capnpForOutput.from_bytes(result['output']).basicState

    result = net.OnOutput_combined_output()
    assert 1 == len(result)
    combined_msg = program_U.capnpForCombinedOutput.from_bytes(result['combined_output'])
    assert 8 == combined_msg.a.basicState
    assert 3 == combined_msg.b.c.x.basicState
    assert 1 == combined_msg.b.c.y.basicState
    assert 5 == combined_msg.b.d.basicState
    assert 1 == combined_msg.b.e.basicState

    inputT = program_U.capnpForA_T.new_message()
    transition0, transition1 = inputT.init('transitions', 2)
    right_transition = transition0.init('productOnright')

    right_transition.init('transitions', 1)[0].init('productOny').basicTransition = 12

    transition1.init('productOnleft').basicTransition = 3

    result = net.OnTransitions({'a': [inputT.to_bytes()]})
    assert 2 == len(result)
    assert 15 == program_U.capnpForOutput_T.from_bytes(result['output']).basicTransition
    combinedOutputT = program_U.capnpForCombinedOutput_T.from_bytes(result['combined_output'])

    # There are MANY possible correct results for combinedOutputT.  Instead of checking for a specific one,
    # we test for certain properties that should be true of combinedOutputT no matter which correct result we get.
    aTotal, bcxTotal, bcyTotal, bdTotal, beTotal = 0, 0, 0, 0, 0
    for transition in combinedOutputT.transitions:
      if str(transition.which) == 'productOna':
        aTotal += transition.productOna.basicTransition
      elif str(transition.which) == 'productOnb':
        for bTransition in transition.productOnb.transitions:
          if str(bTransition.which) == 'productOnc':
            for cTransition in bTransition.productOnc.transitions:
              if str(cTransition.which) == 'productOnx':
                bcxTotal += cTransition.productOnx.basicTransition
              elif str(cTransition.which) == 'productOny':
                bcyTotal += cTransition.productOny.basicTransition
          elif str(bTransition.which) == 'productOnd':
            bdTotal += bTransition.productOnd.basicTransition
          elif str(bTransition.which) == 'productOne':
            beTotal += bTransition.productOne.basicTransition

    assert 15 == aTotal
    assert 0 == bcxTotal
    assert 12 == bcyTotal
    assert 3 == bdTotal
    assert 12 == beTotal

  def test_product_output(self, program_W):
    net = program_W.module.Net()

    assert not net.OnOutput_output()
    assert not net.OnInput_a(program_W.capnpForA.new_message(basicState=3).to_bytes())
    outputs = net.OnInput_b(program_W.capnpForA.new_message(basicState=5).to_bytes())
    assert 1 == len(outputs)
    msg = program_W.capnpForOutput.from_bytes(outputs['output'])
    assert 3 == msg.left.basicState
    assert 5 == msg.right.basicState

    outputs = net.OnTransitions({
        'b': [program_W.capnpForB.new_message(basicState=4).to_bytes()],
    })
    assert 1 == len(outputs)
    transitions = program_W.capnpForOutput_T.from_bytes(outputs['output']).transitions

    assert 1 == len(transitions)
    t = transitions[0]
    assert 'productOnright' == str(t.which)
    assert 4 == t.productOnright.basicTransition

  def test_maintain_product_state(self, program_V):
    net = program_V.module.Net()

    product_msg = program_V.capnpForA.new_message()
    product_msg.left.basicState = 4
    product_msg.right.basicState = 7
    assert not net.OnInput_a(product_msg.to_bytes())

    product_transition = program_V.capnpForA_T.new_message()
    product_transition.init('transitions', 1)[0].init('productOnright').basicTransition = 100
    assert not net.OnTransitions({'a': [product_transition.to_bytes()]})

    assert not net.OnInput_b(program_V.capnpForB.new_message(basicState=2).to_bytes())

    output = net.OnOutput_output()
    assert len(output) == 1
    assert 113 == program_V.capnpForOutput.from_bytes(output['output']).basicState

  def test_product_input(self, program_V):
    net = program_V.module.Net()

    assert not net.OnOutput_output()
    assert not net.OnInput_b(program_V.capnpForB.new_message(basicState=2).to_bytes())

    product_msg = program_V.capnpForA.new_message(
        left=program_V.capnpForInt32.new_message(basicState=4),
        right=program_V.capnpForInt32.new_message(basicState=7),
    )
    outputs = net.OnInput_a(product_msg.to_bytes())
    assert 1 == len(outputs)
    assert 13 == program_V.capnpForOutput.from_bytes(outputs['output']).basicState

    aTransitions = program_V.capnpForA_T.new_message()
    transition = aTransitions.init('transitions', 1)[0]
    transition.init('productOnright').basicTransition = 12

    outputs = net.OnTransitions({
        'a': [aTransitions.to_bytes()],
        'b': [program_V.capnpForB_T.new_message(basicTransition=1).to_bytes()],
    })

    assert 1 == len(outputs)
    assert 12 + 1 == program_V.capnpForOutput_T.from_bytes(outputs['output']).basicTransition

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


def program_plus(left, right):
  return expression.Applied(
      func=primitive.Plus(types.Int32),
      arg=expression.Product([
          ('left', left),
          ('right', right),
      ]),
  )


class _ProgramData(object):
  pass


@pytest.fixture(scope='module')
def program_U():
  self = _ProgramData()

  self.inputA = expression.Input(
      'a',
      types.Product(items=[
          ('left', types.Int32),
          ('right', types.Product(items=[
              ('x', types.Int32),
              ('y', types.Int32),
          ])),
      ]))
  self.inputB = expression.Input('b', types.Int32)

  self.left = expression.Project('left', self.inputA)
  self.right = expression.Project('right', self.inputA)
  self.right_y = expression.Project('y', self.right)
  self.intermediate = program_plus(self.left, self.right_y)

  self.output = program_plus(self.intermediate, self.inputB)

  self.combined_output = expression.Product(items=[
      ('a', self.output),
      ('b', expression.Product(items=[
          ('c', self.right),
          ('d', self.left),
          ('e', self.right_y),
      ])),
  ])

  self.compiler = reactive.ReactiveCompiler(name='program_U')
  self.module = self.compiler.compile({'output': self.output, 'combined_output': self.combined_output})

  self.capnpForA = self.compiler.capnp_state_builder(self.inputA)
  self.capnpForA_T = self.compiler.capnp_transitions_builder(self.inputA)

  self.capnpForInt32 = self.compiler.capnp_state_builder_for_type(types.Int32)
  self.capnpForInt32_T = self.compiler.capnp_transitions_builder_for_type(types.Int32)

  self.capnpForB = self.compiler.capnp_state_builder(self.inputB)
  self.capnpForB_T = self.compiler.capnp_transitions_builder(self.inputB)
  self.capnpForOutput = self.compiler.capnp_state_builder(self.output)
  self.capnpForOutput_T = self.compiler.capnp_transitions_builder(self.output)

  self.capnpForCombinedOutput = self.compiler.capnp_state_builder(self.combined_output)
  self.capnpForCombinedOutput_T = self.compiler.capnp_transitions_builder(self.combined_output)

  return self


@pytest.fixture(scope='module')
def program_V():
  self = _ProgramData()

  self.inputA = expression.Input('a', types.Product(items=[
      ('left', types.Int32),
      ('right', types.Int32),
  ]))
  self.inputB = expression.Input('b', types.Int32)
  self.outputExpr = program_plus(self.inputB, expression.Applied(func=primitive.Plus(types.Int32), arg=self.inputA))

  self.compiler = reactive.ReactiveCompiler(name='program_V')
  self.module = self.compiler.compile({'output': self.outputExpr})

  self.capnpForA = self.compiler.capnp_state_builder(self.inputA)
  self.capnpForA_T = self.compiler.capnp_transitions_builder(self.inputA)

  self.capnpForInt32 = self.compiler.capnp_state_builder_for_type(types.Int32)
  self.capnpForInt32_T = self.compiler.capnp_transitions_builder_for_type(types.Int32)

  self.capnpForB = self.compiler.capnp_state_builder(self.inputB)
  self.capnpForB_T = self.compiler.capnp_transitions_builder(self.inputB)
  self.capnpForOutput = self.compiler.capnp_state_builder(self.outputExpr)
  self.capnpForOutput_T = self.compiler.capnp_transitions_builder(self.outputExpr)

  return self


@pytest.fixture(scope='module')
def program_W():
  self = _ProgramData()

  self.inputA = expression.Input('a', types.Int32)
  self.inputB = expression.Input('b', types.Int32)

  self.outputExpr = expression.Product([
      ('left', self.inputA),
      ('right', self.inputB),
  ])

  self.compiler = reactive.ReactiveCompiler(name='program_W')
  self.module = self.compiler.compile({'output': self.outputExpr})

  self.capnpForA = self.compiler.capnp_state_builder(self.inputA)
  self.capnpForA_T = self.compiler.capnp_transitions_builder(self.inputA)
  self.capnpForB = self.compiler.capnp_state_builder(self.inputB)
  self.capnpForB_T = self.compiler.capnp_transitions_builder(self.inputB)
  self.capnpForOutput = self.compiler.capnp_state_builder(self.outputExpr)
  self.capnpForOutput_T = self.compiler.capnp_transitions_builder(self.outputExpr)

  return self


@pytest.fixture(scope='module')
def program_X():
  self = _ProgramData()

  self.inputA = expression.Input('a', types.Int32)
  self.inputB = expression.Input('b', types.Int32)
  self.outputExpr = program_plus(self.inputA, self.inputB)

  self.compiler = reactive.ReactiveCompiler(name='program_X')
  self.module = self.compiler.compile({'output': self.outputExpr})

  self.capnpForA = self.compiler.capnp_state_builder(self.inputA)
  self.capnpForA_T = self.compiler.capnp_transitions_builder(self.inputA)
  self.capnpForB = self.compiler.capnp_state_builder(self.inputB)
  self.capnpForB_T = self.compiler.capnp_transitions_builder(self.inputB)
  self.capnpForOutput = self.compiler.capnp_state_builder(self.outputExpr)
  self.capnpForOutput_T = self.compiler.capnp_transitions_builder(self.outputExpr)

  return self


@pytest.fixture(scope='module')
def program_Y():
  self = _ProgramData()

  self.inputA = expression.Input('a', types.Int32)
  self.inputB = expression.Input('b', types.Int32)
  self.outputX = program_plus(self.inputA, self.inputB)
  self.outputY = program_plus(self.inputB, self.inputB)

  self.compiler = reactive.ReactiveCompiler(name='program_Y')
  self.module = self.compiler.compile({'x': self.outputX, 'y': self.outputY})

  self.capnpForA = self.compiler.capnp_state_builder(self.inputA)
  self.capnpForA_T = self.compiler.capnp_transitions_builder(self.inputA)
  self.capnpForB = self.compiler.capnp_state_builder(self.inputB)
  self.capnpForB_T = self.compiler.capnp_transitions_builder(self.inputB)
  self.capnpForX = self.compiler.capnp_state_builder(self.outputX)
  self.capnpForX_T = self.compiler.capnp_transitions_builder(self.outputX)
  self.capnpForY = self.compiler.capnp_state_builder(self.outputY)
  self.capnpForY_T = self.compiler.capnp_transitions_builder(self.outputY)

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

  self.capnpForA = self.compiler.capnp_state_builder(self.inputA)
  self.capnpForA_T = self.compiler.capnp_transitions_builder(self.inputA)
  self.capnpForB = self.compiler.capnp_state_builder(self.inputB)
  self.capnpForB_T = self.compiler.capnp_transitions_builder(self.inputB)
  self.capnpForX = self.compiler.capnp_state_builder(self.outputX)
  self.capnpForX_T = self.compiler.capnp_transitions_builder(self.outputX)
  self.capnpForY = self.compiler.capnp_state_builder(self.outputY)
  self.capnpForY_T = self.compiler.capnp_transitions_builder(self.outputY)

  return self


@pytest.fixture(scope='module')
def program_C():
  self = _ProgramData()

  self.inputA = expression.Input('a', types.Int32)
  self.inputB = expression.Input('b', types.Int32)
  self.inputC = expression.Input('c', types.Int32)

  self.outputX = program_plus(self.inputA, self.inputB)

  self.outputY = program_plus(self.outputX, self.inputB)

  self.outputZ = program_plus(self.inputA, program_plus(self.inputB, self.inputC))

  self.compiler = reactive.ReactiveCompiler(name='program_C')
  self.module = self.compiler.compile({
      'x': self.outputX,
      'y': self.outputY,
      'z': self.outputZ,
  })

  self.capnpForA = self.compiler.capnp_state_builder(self.inputA)
  self.capnpForB = self.compiler.capnp_state_builder(self.inputB)
  self.capnpForC = self.compiler.capnp_state_builder(self.inputC)

  self.capnpForA_T = self.compiler.capnp_transitions_builder(self.inputA)
  self.capnpForB_T = self.compiler.capnp_transitions_builder(self.inputB)
  self.capnpForC_T = self.compiler.capnp_transitions_builder(self.inputC)

  self.capnpForX = self.compiler.capnp_state_builder(self.outputX)
  self.capnpForY = self.compiler.capnp_state_builder(self.outputY)
  self.capnpForZ = self.compiler.capnp_state_builder(self.outputZ)

  self.capnpForX_T = self.compiler.capnp_transitions_builder(self.outputX)
  self.capnpForY_T = self.compiler.capnp_transitions_builder(self.outputY)
  self.capnpForZ_T = self.compiler.capnp_transitions_builder(self.outputZ)

  return self
