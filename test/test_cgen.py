from dist_zero import cgen


def test_cgen():
  prog = cgen.Program("test_program", docstring='Dummy program for testing')
  x = cgen.Var("x", cgen.Int32)
  f = prog.AddFunction('f', cgen.Int32, [x])
  y = cgen.Var("y", cgen.Int32)
  z = cgen.Var("z", cgen.Int32)
  f.AddAssignment(cgen.CreateVar(y), x + x)
  f.AddAssignment(cgen.CreateVar(z), x + (x + y))
  f.AddReturn(z)

  Fargs = cgen.Var('args', cgen.PyObject.Star())
  F = prog.AddFunction(
      "F",
      cgen.PyObject.Star(), [cgen.Var('self', cgen.PyObject.Star()), Fargs],
      export=True,
      docstring="A function with a bit of addition.")
  F_i = cgen.Var("i", cgen.Int32)
  F.AddDeclaration(cgen.CreateVar(F_i))
  an_if = F.AddIf(cgen.Call(cgen.PyArg_ParseTuple, [Fargs, cgen.StrConstant("i"), F_i.Address()]).Negate())
  an_if.consequent.AddReturn(cgen.NULL)
  F_result = cgen.Var("result", cgen.Int32)
  F.AddAssignment(cgen.CreateVar(F_result), cgen.Call(f, [F_i]))
  F.AddReturn(cgen.Call(cgen.PyLong_FromLong, [F_result]))

  argc = cgen.Var("argc", cgen.Int32)
  g = prog.AddFunction('g', cgen.Int32, [argc])
  g.AddReturn(cgen.Call(f, [argc]))

  mod = prog.build_and_import()
  assert 16 == mod.F(4)
