import os
from collections import defaultdict

import capnp
capnp.remove_import_hook()

from dist_zero import cgen, errors, expression, capnpgen, types, primitive
from dist_zero import type_compiler, settings


class ReactiveCompiler(object):
  '''
  For building a reactive program from a set of normalized expressions.
  '''

  def __init__(self, name, docstring=''):
    self.name = name
    self.docstring = docstring

    self.program = cgen.Program(
        name=self.name,
        docstring=self.docstring,
        includes=[
            '"capnp_c.h"',
            f'"{self._capnp_header_filename()}"',
            #f'"{self._capnp_header_filename()}"',
            #"<capnp/message.h>",
            #"<capnp/serialize-packed.h>",
        ],
        library_dirs=[
            settings.CAPNP_INCLUDE_DIR,
        ],
        sources=[
            os.path.join(self._capnp_dirname(), self._capnp_source_filename()),
            # NOTE(KK): We must compile all these files into each extension.
            os.path.join(settings.CAPNP_INCLUDE_DIR, "capn.c"),
            os.path.join(settings.CAPNP_INCLUDE_DIR, "capn-malloc.c"),
            os.path.join(settings.CAPNP_INCLUDE_DIR, "capn-stream.c"),
        ],
        libraries=[],
        include_dirs=[
            self._capnp_dirname(),
            settings.CAPNP_INCLUDE_DIR,
        ])
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
    self._net = None

    self._built_capnp = False
    self._pycapnp_module = None

  def type_to_capnp_state_type(self, type):
    state_ref = self.capnp_types.type_to_state_ref[type]
    return cgen.BasicType(f"struct {state_ref}")

  def type_to_capnp_state_ptr(self, type):
    state_ref = self.capnp_types.type_to_state_ref[type]
    return cgen.BasicType(f"{state_ref}_ptr")

  def type_to_capnp_state_write_function(self, type):
    state_ref = self.capnp_types.type_to_state_ref[type]
    return cgen.Var(f"write_{state_ref}", None)

  def type_to_capnp_state_new_ptr_function(self, type):
    state_ref = self.capnp_types.type_to_state_ref[type]
    return cgen.Var(f"new_{state_ref}", None)

  def type_to_capnp_state_read_function(self, type):
    state_ref = self.capnp_types.type_to_state_ref[type]
    return cgen.Var(f"read_{state_ref}", None)

  def _capnp_filename(self):
    return f"{self.name}.capnp"

  def _capnp_source_filename(self):
    return f"{self._capnp_filename()}.c"

  def _capnp_header_filename(self):
    return f"{self._capnp_filename()}.h"

  def _capnp_dirname(self):
    return os.path.join(os.path.realpath('.'), '.tmp', 'capnp')

  def _build_capnp(self):
    if not self._built_capnp:
      dirname = self._capnp_dirname()
      os.makedirs(dirname, exist_ok=True)
      filename = self._capnp_filename()
      self.capnp_types.capnp.build_in(dirname=dirname, filename=filename)
      self._built_capnp = True

  def get_pycapnp_module(self):
    if self._pycapnp_module is None:
      self._build_capnp()
      dirname = self._capnp_dirname()
      filename = self._capnp_filename()

      self._pycapnp_module = capnp.load(os.path.join(dirname, filename))

    return self._pycapnp_module

  def capnp_state_module(self, expr):
    t = self._type_by_expr[expr]
    capnp_module = self.get_pycapnp_module()
    name = self.capnp_types.type_to_state_ref[t]
    return capnp_module.__dict__[name]

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

    init.AddReturn(cgen.Constant(0))

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

  def _pyerr_from_string(self, s):
    return cgen.PyErr_SetString(cgen.PyExc_RuntimeError, cgen.StrConstant(s))

  def _generate_on_output(self, key, expr):
    '''
    Generate the OnOutput_{key} function in c for ``expr``.
    '''
    on_output = self._net.AddMethod(name=self._on_output_function_name(key), args=None)
    outputType = self.expr_type[expr]
    output_index = self.expr_index[expr]

    vGraph = on_output.SelfArg()

    vResult = cgen.Var('result', cgen.PyObject.Star())
    on_output.AddAssignment(cgen.CreateVar(vResult), cgen.PyDict_New())

    (on_output.AddIf(vResult == cgen.NULL).consequent.AddAssignment(
        None, self._pyerr_from_string("Failed to create output dictionary")).AddReturn(cgen.NULL))

    subscribeFunction = cgen.Var(self._subscribe_function_name(output_index), None)
    ifHasState = on_output.AddIf(subscribeFunction(vGraph))
    whenHasState = ifHasState.consequent

    outputState = self.state_rvalue(vGraph, expr)
    vBytes = outputType.generate_c_state_to_capnp(self, whenHasState, outputState)

    whenHasState.Newline().AddIf(
        cgen.MinusOne == cgen.PyDict_SetItemString(vResult, cgen.StrConstant(key), vBytes)).consequent.AddReturn(
            cgen.NULL)

    on_output.Newline().AddReturn(vResult)

  def _generate_on_input(self, expr):
    '''
    Generate the OnInput_{name} function in c for ``expr``.
    '''
    index = self.expr_index[expr]
    inputType = self.expr_type[expr]

    vBuf = cgen.Var('buf', cgen.UInt8.Star())
    vBuflen = cgen.Var('buflen', cgen.MachineInt)
    vCapn = cgen.Var('capn', cgen.Capn)

    on_input = self._net.AddMethod(name=self._on_input_function_name(expr), args=None) # We'll do our own arg parsing
    vGraph = on_input.SelfArg()
    vArgsArg = on_input.ArgsArg()

    on_input.AddDeclaration(cgen.CreateVar(vBuf))
    on_input.AddDeclaration(cgen.CreateVar(vBuflen))
    on_input.AddDeclaration(cgen.CreateVar(vCapn))

    whenParseFail = on_input.AddIf(
        cgen.PyArg_ParseTuple(vArgsArg, cgen.StrConstant("s#"), vBuf.Address(), vBuflen.Address()).Negate()).consequent
    whenParseFail.AddReturn(cgen.NULL)

    on_input.Newline()

    (on_input.AddIf(
        cgen.Zero != cgen.capn_init_mem(vCapn.Address(), vBuf, vBuflen, cgen.Zero)).consequent.AddAssignment(
            None,
            self._pyerr_from_string("Failed to initialize struct capn when parsing a message.")).AddReturn(cgen.NULL))

    on_input.Newline()

    vResult = cgen.Var('result', cgen.PyObject.Star())
    on_input.AddAssignment(cgen.CreateVar(vResult), cgen.PyDict_New())

    vCapnPtr = cgen.Var('msg_ptr', cgen.Capn_Ptr)
    on_input.AddAssignment(
        cgen.CreateVar(vCapnPtr), cgen.capn_getp(cgen.capn_root(vCapn.Address()), cgen.Zero, cgen.One))

    inputType.generate_capnp_to_c_state(self, on_input, vCapnPtr, self.state_lvalue(vGraph, expr))

    on_input.AddAssignment(cgen.UpdateVar(vGraph).Arrow('n_missing_inputs').Sub(cgen.Constant(index)), cgen.Zero)
    on_input.AddAssignment(None, cgen.capn_free(vCapn.Address()))

    produceState = cgen.Var(self._produce_function_name(index), None)
    on_input.AddAssignment(None, produceState(vGraph))

    # FIXME(KK): Come up with a mechanism whereby the result dictionary is actually populated.
    on_input.AddReturn(vResult)

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

    self._build_capnp()

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
    return module


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
