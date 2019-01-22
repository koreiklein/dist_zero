import pytest

from dist_zero import cgen


@pytest.mark.cgen
def test_cgen_python_type():
  prog = cgen.Program("test_python_type_program", docstring='Dummy program exporting a new type')

  new_type = prog.AddPythonType('MyNewType', docstring='For testing adding types via a c extension.')

  init = new_type.AddInit()
  init.AddReturn(cgen.Constant(0))

  x = cgen.Var("x", cgen.Int32)
  pair_f = new_type.AddMethod('f', [x])
  pair_f.AddReturn(cgen.PyLong_FromLong(x + x))

  mod = prog.build_and_import()

  x = mod.MyNewType()
  assert 2 == x.f(1)
  assert 6 == x.f(3)


@pytest.mark.cgen
def test_cgen_basics():
  prog = cgen.Program("test_cgen_basics", docstring='Dummy program for testing')

  x = cgen.Var("x", cgen.Int32)
  f = prog.AddFunction('f', cgen.Int32, [x])
  y = f.AddDeclaration(cgen.Var("y", cgen.Int32), x + x)
  z = f.AddDeclaration(cgen.Var("z", cgen.Int32), x + (x + y))
  f.AddReturn(z)

  F_i = cgen.Var("i", cgen.Int32)
  F = prog.AddExternalFunction("F", [F_i])
  F_result = F.AddDeclaration(cgen.Var("result", cgen.Int32), f(F_i))
  F.AddReturn(cgen.PyLong_FromLong(F_result))

  mod = prog.build_and_import()
  assert 16 == mod.F(4)


@pytest.mark.cgen
def test_cgen_emptyif():
  prog = cgen.Program('test_emptyif')
  x = cgen.Var('x', cgen.Int32)
  f = prog.AddExternalFunction("f", [x])
  empty_if = f.AddIf(x == cgen.Constant(1))
  f.AddReturn(cgen.PyLong_FromLong(cgen.Constant(3)))

  c_f = prog.build_and_import().f

  assert 3 == c_f(2)
  assert 3 == c_f(0)


@pytest.mark.cgen
def test_cgen_break():
  def python_f(x):
    y = 0
    while True:
      if x == 1:
        break
      if x % 2 == 0:
        x = x // 2
      else:
        x = x * 3 + 1
      y = y + 1
    return y

  prog = cgen.Program("test_collatz")
  x = cgen.Var('x', cgen.Int32)
  f = prog.AddExternalFunction("f", [x])
  y = f.AddDeclaration(cgen.Var('y', cgen.Int32), cgen.Constant(0))
  whileblock = f.AddWhile(cgen.true)

  # It's good to try an if statement with no consequent, but an alternate.
  first_if = whileblock.AddIf((x == cgen.Constant(1)).Negate())
  first_if.alternate.AddBreak()

  second_if = whileblock.AddIf(x % cgen.Constant(2) == cgen.Constant(0))
  second_if.consequent.AddAssignment(x, x / cgen.Constant(2))
  second_if.alternate.AddAssignment(x, x * cgen.Constant(3) + cgen.Constant(1))

  whileblock.AddAssignment(y, y + cgen.Constant(1))

  f.AddReturn(cgen.PyLong_FromLong(y))

  c_f = prog.build_and_import().f

  for x in [1, 3, 5]:
    assert python_f(x) == c_f(x)


@pytest.mark.cgen
def test_cgen_while():
  # The python version of f
  def python_f(x):
    y = 0
    while x > 0:
      x = x - 1
      if x % 12 == 0:
        continue
      if x % 2 == 0:
        y = y + x // 3
      else:
        y = y + x // 5

    return y

  # The c version of f
  prog = cgen.Program("while_test_program")
  x = cgen.Var('x', cgen.Int32)
  f = prog.AddExternalFunction("f", [x])
  y = f.AddDeclaration(cgen.Var('y', cgen.Int32), cgen.Constant(0))

  whileblock = f.AddWhile(x > cgen.Constant(0))
  whileblock.AddAssignment(x, x - cgen.Constant(1))

  first_if = whileblock.AddIf(x % cgen.Constant(12) == cgen.Constant(0))
  first_if.consequent.AddContinue()

  second_if = whileblock.AddIf(x % cgen.Constant(2) == cgen.Constant(0))
  second_if.consequent.AddAssignment(y, y + x / cgen.Constant(3))
  second_if.alternate.AddAssignment(y, y + x / cgen.Constant(5))

  f.AddReturn(cgen.PyLong_FromLong(y))

  c_f = prog.build_and_import().f

  for x in [2, 4, 6, 10, 30]:
    assert python_f(x) == c_f(x)


@pytest.mark.cgen
def test_cgen_binops():
  prog = cgen.Program("binop_test_program")

  ops = [
      lambda a, b: a + b,
      lambda a, b: a - b,
      lambda a, b: a * b,
      lambda a, b: a % b,
      lambda a, b: a // b,
  ]

  for i, op in enumerate(ops):
    a = cgen.Var("a", cgen.Int32)
    b = cgen.Var("b", cgen.Int32)
    f = prog.AddExternalFunction(f"F_{i}", [a, b])
    f.AddReturn(cgen.PyLong_FromLong(op(a, b)))

  mod = prog.build_and_import()

  for i, op in enumerate(ops):
    for a in [2, 5, 23]:
      for b in [1, 6, 12, 100]:
        assert op(a, b) == getattr(mod, f"F_{i}")(a, b)


@pytest.mark.cgen
def test_cgen_switch():
  def python_f(x):
    val = x % 4
    if val == 0:
      return 12
    elif val == 1:
      return 7
    else:
      return 211

  prog = cgen.Program("switch_test_program")
  x = cgen.Var('x', cgen.Int32)
  f = prog.AddExternalFunction("f", [x])
  switch = f.AddSwitch(x % cgen.Constant(4))

  case_zero = switch.AddCase(cgen.Constant(0))
  case_zero.AddReturn(cgen.PyLong_FromLong(cgen.Constant(12)))

  case_one = switch.AddCase(cgen.Constant(1))
  case_one.AddReturn(cgen.PyLong_FromLong(cgen.Constant(7)))

  case_default = switch.AddDefault()
  case_default.AddReturn(cgen.PyLong_FromLong(cgen.Constant(211)))

  c_f = prog.build_and_import().f

  for i in range(10):
    assert python_f(i) == c_f(i)
