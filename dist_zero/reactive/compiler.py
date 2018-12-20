from collections import defaultdict

from dist_zero import cgen, errors, expression, capnpgen, types, primitive
from dist_zero import type_compiler


class ReactiveCompiler(object):
  '''
  For building a reactive program from a set of normalized expressions.
  '''

  def __init__(self, name, docstring=''):
    self.name = name
    self.docstring = docstring

    self.program = cgen.Program(self.name, docstring=self.docstring)
    self.c_types = type_compiler.CTypeCompiler(self.program)
    self.capnp_types = type_compiler.CapnpTypeCompiler()

    self.output_key_to_norm_expr = None

    self._type_by_expr = {}

    self._graph_struct = None
    self._cached_n_exprs = None

    self._top_exprs = None
    self.expr_to_inputs = None
    self.expr_to_outputs = None
    self.expr_index = None
    self.expr_type = None
    self._state_types = None
    self._input_exprs = None
    self.protos = None
    self._net = None

  def _get_type_for_expr(self, expr):
    if expr not in self._type_by_expr:
      t = self._compute_type(expr)
      self._type_by_expr[expr] = t
      return t
    else:
      return self._type_by_expr[expr]

  def state_lvalue(self, vGraph, expr):
    index = self.expr_index[expr]
    return cgen.UpdateVar(vGraph).Arrow(self._state_key_in_graph(index))

  def state_rvalue(self, vGraph, expr):
    index = self.expr_index[expr]
    return vGraph.Arrow(self._state_key_in_graph(index))

  def get_c_state_type(self, expr):
    return self.c_types.type_to_state_ctype[self._get_type_for_expr(expr)]

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
    self._graph_struct = self._net.struct
    self._graph_struct.AddField('n_missing_inputs', cgen.Int32.Array(self._n_exprs()))

    for i, expr in enumerate(self._top_exprs):
      c_state_type = self.get_c_state_type(expr)
      self._graph_struct.AddField(self._state_key_in_graph(i), c_state_type)

  def _state_key_in_graph(self, index):
    return f'state_{index}'

  def _n_exprs(self):
    if self._cached_n_exprs is None:
      self._cached_n_exprs = cgen.Constant(len(self._top_exprs))

    return self._cached_n_exprs

  def _generate_initialize_root_graph(self):
    '''Generate code in self.program defining the Net type.'''
    self._generate_structs()

    init = self._net.AddInit()

    for i, expr in enumerate(self._top_exprs):
      init.AddAssignment(cgen.UpdateVar(init.SelfArg()).Arrow('n_missing_inputs').Sub(cgen.Constant(i)), cgen.MinusOne)

    init.AddReturn(cgen.PyLong_FromLong(cgen.Constant(0)))

  def _initialize_state_function_name(self, index):
    return f"initialize_state_{index}"

  def _subscribe_function_name(self, index):
    return f"subscribe_to_{index}"

  def _produce_function_name(self, index):
    return f"produce_on_{index}"

  def _on_input_function_name(self, expr):
    return f"OnInput_{expr.name}"

  def _on_output_function_name(self, key):
    return f"OnOutput_{key}"

  def _generate_initialize_state(self, index):
    '''
    Generate the state initialization function for this index.

    It should be called once all the input states have already been populated.
    '''

    expr = self._top_exprs[index]
    if expr.__class__ == expression.Input:
      return # Input expressions do not require an ordinary state initialization function

    vGraph = cgen.Var('graph', self._graph_struct.Star())
    initialize_state = self.program.AddFunction(
        name=self._initialize_state_function_name(index), retType=cgen.Void, args=[vGraph])

    expr.generate_initialize_state(self, initialize_state, vGraph)

  def _generate_produce(self, index):
    '''
    Generate the produce function in c for this expression index.
    This function will only be called after the state for the expression has been initialized and its
      n_missing_inputs variable set to zero.
    Calling it ensures that any expression enabled by the setting of this state will be initialized and its
    produced function will be called.
    '''
    vGraph = cgen.Var('graph', self._graph_struct.Star())
    produce = self.program.AddFunction(name=self._produce_function_name(index), retType=cgen.Void, args=[vGraph])

    expr = self._top_exprs[index]
    for output_expr in self.expr_to_outputs[expr]:
      output_index = self.expr_index[output_expr]

      vNMissingInputs = vGraph.Arrow('n_missing_inputs').Sub(cgen.Constant(output_index))
      whenSubscribed = produce.AddIf(vNMissingInputs >= cgen.Zero).consequent

      whenSubscribed.AddAssignment(
          cgen.UpdateVar(vGraph).Arrow('n_missing_inputs').Sub(cgen.Constant(output_index)), vNMissingInputs - cgen.One)

      whenReady = whenSubscribed.AddIf(vNMissingInputs == cgen.Zero).consequent
      initializeFunction = cgen.Var(self._initialize_state_function_name(output_index), None)
      produceFunction = cgen.Var(self._produce_function_name(output_index), None)
      whenReady.AddAssignment(None, initializeFunction(vGraph))
      whenReady.AddAssignment(None, produceFunction(vGraph))

  def _generate_on_output(self, key, expr):
    '''
    Generate the OnOutput_{key} function in c for ``expr``.
    '''
    on_output = self._net.AddMethod(name=self._on_output_function_name(key), args=None)

  def _generate_on_input(self, expr):
    '''
    Generate the OnInput_{name} function in c for ``expr``.
    '''
    vBuf = cgen.Var('buf', cgen.Char.Star())
    vBuflen = cgen.Var('buflen', cgen.MachineInt)

    on_input = self._net.AddMethod(name=self._on_input_function_name(expr), args=None) # We'll do our own arg parsing
    vGraph = on_input.SelfArg()
    vArgsArg = on_input.ArgsArg()

    on_input.AddDeclaration(cgen.CreateVar(vBuf))
    on_input.AddDeclaration(cgen.CreateVar(vBuflen))

    whenParseFail = on_input.AddIf(
        cgen.PyArg_ParseTuple(vArgsArg, cgen.StrConstant("s#"), vBuf.Address(), vBuflen.Address()).Negate()).consequent
    whenParseFail.AddReturn(cgen.NULL)

  def _generate_subscribe(self, index):
    '''
    Generate the function to subscribe on this index when one of its outputs is subscribed to.

    It will return true iff the expression at this index has a state.
    '''
    expr = self._top_exprs[index]
    if expr.__class__ == expression.Input:
      self._generate_subscribe_input(index, expr)
    else:
      self._generate_subscribe_noninput(index, expr)

  def _generate_subscribe_input(self, index, expr):
    vGraph = cgen.Var('graph', self._graph_struct.Star())
    subscribe = self.program.AddFunction(name=self._subscribe_function_name(index), retType=cgen.Int32, args=[vGraph])

    # Inputs will have their n_missing_inputs value set to 0 only after they have been initialized.
    subscribe.AddReturn(vGraph.Arrow('n_missing_inputs').Sub(cgen.Constant(index)) == cgen.Zero)

  def _generate_subscribe_noninput(self, index, expr):
    vGraph = cgen.Var('graph', self._graph_struct.Star())
    subscribe = self.program.AddFunction(name=self._subscribe_function_name(index), retType=cgen.Int32, args=[vGraph])

    missingInputsI = vGraph.Arrow('n_missing_inputs').Sub(cgen.Constant(index))
    updateMissingInputsI = cgen.UpdateVar(vGraph).Arrow('n_missing_inputs').Sub(cgen.Constant(index))

    ifAlreadySubscribed = subscribe.AddIf(missingInputsI >= cgen.Zero)
    ifAlreadySubscribed.consequent.AddReturn(missingInputsI == cgen.Zero)
    whenNotAlreadySubscribed = ifAlreadySubscribed.alternate

    nMissingInputs = cgen.Var('n_missing_inputs', cgen.Int32)
    whenNotAlreadySubscribed.AddAssignment(
        cgen.CreateVar(nMissingInputs), cgen.Constant(len(self.expr_to_inputs[expr])))

    for inputExpr in self.expr_to_inputs[expr]:
      inputSubscribeFunction = cgen.Var(self._subscribe_function_name(self.expr_index[inputExpr]), None)
      ifInputIsReady = whenNotAlreadySubscribed.AddIf(inputSubscribeFunction(vGraph))
      ifInputIsReady.consequent.AddAssignment(cgen.UpdateVar(nMissingInputs), nMissingInputs - cgen.One)

    whenNotAlreadySubscribed.AddAssignment(updateMissingInputsI, nMissingInputs)

    ifInputsAreSubscribed = whenNotAlreadySubscribed.AddIf(nMissingInputs == cgen.Zero)
    ifInputsAreSubscribed.alternate.AddReturn(cgen.false)
    whenInputsAreSubscribed = ifInputsAreSubscribed.consequent

    initializeFunction = cgen.Var(self._initialize_state_function_name(self.expr_index[expr]), None)
    whenInputsAreSubscribed.AddAssignment(None, initializeFunction(vGraph))
    whenInputsAreSubscribed.AddReturn(cgen.true)

  def compile(self, output_key_to_norm_expr):
    self._output_key_to_norm_expr = output_key_to_norm_expr
    topsorter = _Topsorter(list(output_key_to_norm_expr.values()))
    self._top_exprs = topsorter.topsort()
    self.expr_to_inputs = topsorter.expr_to_inputs
    self.expr_to_outputs = topsorter.expr_to_outputs
    self.expr_index = {}
    for i, expr in enumerate(self._top_exprs):
      self.expr_index[expr] = i

    self._state_types = [self._get_type_for_expr(expr) for expr in self._top_exprs]
    self.expr_type = dict(zip(self._top_exprs, self._state_types))

    self._net = self.program.AddPythonType(name='Net', docstring=f"For running the {self.name} reactive network.")

    # Add c types for all
    for t in self._state_types:
      self.c_types.ensure_root_type(t)

    # Add capnproto types for outputs
    for expr in self._output_key_to_norm_expr.values():
      self.capnp_types.ensure_root_type(self._get_type_for_expr(expr))

    # Add capnproto types for inputs
    self._input_exprs = []
    for expr in self._top_exprs:
      if expr.__class__ == expression.Input:
        self._input_exprs.append(expr)
        self.capnp_types.ensure_root_type(self._get_type_for_expr(expr))

    self.protos = self.capnp_types.capnp

    self._generate_initialize_root_graph()

    for i in range(0, len(self._top_exprs)):
      self._generate_initialize_state(i)

    for i in range(0, len(self._top_exprs)):
      self._generate_subscribe(i)

    for i in range(len(self._top_exprs) - 1, -1, -1):
      self._generate_produce(i)

    for expr in self._input_exprs:
      self._generate_on_input(expr)

    for key, expr in self._output_key_to_norm_expr.items():
      self._generate_on_output(key, expr)

    module = self.program.build_and_import()
    return module.Net()


class _Topsorter(object):
  '''Populate self._top_exprs from a topological traversal of self.output_key_to_norm_expr'''

  def __init__(self, root_exprs):
    self.root_exprs = root_exprs
    self.visited = set()
    self.active = set()
    self.result = []

    self.expr_to_inputs = {}
    self.expr_to_outputs = defaultdict(list)

  def topsort(self):
    for expr in self.root_exprs:
      self._visit(expr)

    for expr, inputExprs in self.expr_to_inputs.items():
      for inputExpr in inputExprs:
        self.expr_to_outputs[inputExpr].append(expr)

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
      self.expr_to_inputs[expr] = [expr.arg]
      self._visit(expr.arg)
    elif expr.__class__ == expression.Product:
      self.expr_to_inputs[expr] = []
      for key, kid in expr.items:
        self.expr_to_inputs[expr].append(kid)
        self._visit(kid)
    elif expr.__class__ == expression.Input:
      self.expr_to_inputs[expr] = []
    else:
      raise errors.InternalError(f"Unrecognized type of normalized expression {expr.__class__}.")
