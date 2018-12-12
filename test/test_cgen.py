import pytest

from dist_zero import cgen


@pytest.mark.cgen
def test_cgen_basics():
  prog = cgen.Program("test_program", docstring='Dummy program for testing')

  x = cgen.Var("x", cgen.Int32)
  f = prog.AddFunction('f', cgen.Int32, [x])
  y = cgen.Var("y", cgen.Int32)
  z = cgen.Var("z", cgen.Int32)
  f.AddAssignment(cgen.CreateVar(y), x + x)
  f.AddAssignment(cgen.CreateVar(z), x + (x + y))
  f.AddReturn(z)

  F_i = cgen.Var("i", cgen.Int32)
  F = prog.AddExternalFunction("F", [F_i])
  F_result = cgen.Var("result", cgen.Int32)
  F.AddAssignment(cgen.CreateVar(F_result), cgen.Call(f, [F_i]))
  F.AddReturn(cgen.Call(cgen.PyLong_FromLong, [F_result]))

  mod = prog.build_and_import()
  assert 16 == mod.F(4)


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
  y = cgen.Var('y', cgen.Int32)
  f.AddAssignment(cgen.CreateVar(y), cgen.Constant(0))
  whileblock = f.AddWhile(cgen.true)

  first_if = whileblock.AddIf(x == cgen.Constant(1))
  first_if.consequent.AddBreak()

  second_if = whileblock.AddIf(x % cgen.Constant(2) == cgen.Constant(0))
  second_if.consequent.AddAssignment(x, x / cgen.Constant(2))
  second_if.alternate.AddAssignment(x, x * cgen.Constant(3) + cgen.Constant(1))

  whileblock.AddAssignment(y, y + cgen.Constant(1))

  f.AddReturn(cgen.Call(cgen.PyLong_FromLong, [y]))

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
  y = cgen.Var('y', cgen.Int32)
  f.AddAssignment(cgen.CreateVar(y), cgen.Constant(0))

  whileblock = f.AddWhile(x > cgen.Constant(0))
  whileblock.AddAssignment(x, x - cgen.Constant(1))

  first_if = whileblock.AddIf(x % cgen.Constant(12) == cgen.Constant(0))
  first_if.consequent.AddContinue()

  second_if = whileblock.AddIf(x % cgen.Constant(2) == cgen.Constant(0))
  second_if.consequent.AddAssignment(y, y + x / cgen.Constant(3))
  second_if.alternate.AddAssignment(y, y + x / cgen.Constant(5))

  f.AddReturn(cgen.Call(cgen.PyLong_FromLong, [y]))

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
    f.AddReturn(cgen.Call(cgen.PyLong_FromLong, [op(a, b)]))

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
