from dist_zero import cgen, errors, expression, capnpgen, types, primitive


class ReactiveCompiler(object):
  '''
  For building a reactive program from a set of normalized expressions.
  '''

  def __init__(self, name, docstring=''):
    self.name = name
    self.docstring = docstring

    self.program = cgen.Program(self.name, docstring=self.docstring)
    self.types = types.TypeCompiler(program=self.program)

    self.output_key_to_norm_expr = None

    self._type_by_expr = {}

  def _get_type_for_expr(self, expr):
    if expr not in self._type_by_expr:
      t = self._compute_type(expr)
      self._type_by_expr[expr] = t
      return t
    else:
      return self._type_by_expr[expr]

  def _compute_type(self, expr):
    if expr.__class__ == expression.Applied:
      arg_type = self._get_type_for_expr(expr.arg)
      if not isinstance(expr.func, primitive.PrimitiveOp):
        raise RuntimeError(
            f"Expected a normalized expression, but function an application of a non-PrimitiveOp: {expr.func}.")

      if not expr.func.get_input_type().equivalent(arg_type):
        raise RuntimeError(
            f"Badly typed normalized expression.  Applied a function taking {expr.func.get_input_type()} to an {arg_type}."
        )

      return expr.func.get_output_type()
    elif expr.__class__ == expression.Product:
      return types.Product(items=[(k, self._get_type_for_expr(v)) for k, v in expr.items])
    elif expr.__class__ == expression.Input:
      return expr.type
    elif expr.__class__ == expression.Prim:
      return expr.op.get_type()
    else:
      raise RuntimeError(f"Unrecognized type of normalized expression {expr.__class__}.")

  def _generate_structs(self):
    '''Generate the graph struct in self.program.'''
    graph_struct = self.program.AddStruct('graph')
    graph_struct.AddField('current_states_data', cgen.Int8.Array(self._states_data_n_bytes()))
    graph_struct.AddField('current_states', cgen.Void.Star().Array(len(self._top_exprs)))

    # FIXME(KK): Finish this
    self.program.build_and_import()
    import ipdb
    ipdb.set_trace()

  def _generate_initialize_root_graph(self):
    '''Generate code in self.program defining the InitializeRootGraph function.'''
    self._generate_structs()

    initialize = self.program.AddExternalFunction(
        'InitializeRootGraph', [], docstring="Initialize and return a root graph object.")

  def _states_data_n_bytes(self):
    # FIXME(KK): This is not correct.  Figure out the sizes of each nodelet's state structure and sum them!
    return 666

  def compile(self, output_key_to_norm_expr):
    self._output_key_to_norm_expr = output_key_to_norm_expr
    self._top_exprs = _Topsorter(list(output_key_to_norm_expr.values())).topsort()

    self._state_types = [self._get_type_for_expr(expr) for expr in self._top_exprs]
    for t in self._state_types:
      self.types.ensure_root_type(t)

    self.protos = self.types.build_capnp()

    self._generate_initialize_root_graph()
    import ipdb
    ipdb.set_trace()

    print(self.program.to_c_string())

    module = self.program.build_and_import()
    return module.InitializeRootGraph()


class _Topsorter(object):
  '''Populate self._top_exprs from a topological traversal of self.output_key_to_norm_expr'''

  def __init__(self, root_exprs):
    self.root_exprs = root_exprs
    self.visited = set()
    self.active = set()
    self.result = []

  def topsort(self):
    for expr in self.root_exprs:
      self._visit(expr)

    return self.result

  def _visit(self, expr):
    if expr in self.visited:
      return
    elif expr in self.active:
      raise errors.InternalError("Cycle detected in normalized expression.")
    else:
      self.active.add(expr)
      try:
        self._visit_all_kids(expr)
      finally:
        self.active.remove(expr)
        self.result.append(expr)
        self.visited.add(expr)

  def _visit_all_kids(self, expr):
    if expr.__class__ == expression.Applied:
      self._visit(expr.arg)
    elif expr.__class__ == expression.Product:
      for key, kid in expr.items:
        self._visit(kid)
    elif expr.__class__ in [expression.Input, expression.Prim]:
      pass
    else:
      raise errors.InternalError(f"Unrecognized type of normalized expression {expr.__class__}.")
