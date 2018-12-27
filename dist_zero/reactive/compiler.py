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
    self._turn_struct = None
    self._cached_n_exprs = None

    self._top_exprs = None
    self.expr_to_inputs = None
    self.expr_to_outputs = None
    self.expr_index = None
    self.expr_type = None
    self._state_types = None
    self._input_exprs = None
    self._output_exprs = None # Dictionary from output expression to its list of keys
    self._net = None

    self._built_capnp = False
    self._pycapnp_module = None

  def type_to_capnp_state_type(self, type):
    state_ref = self.capnp_types.type_to_state_ref[type]
    return cgen.BasicType(f"struct {state_ref}")

  def type_to_capnp_transitions_type(self, type):
    transitions_ref = self.capnp_types.type_to_transitions_ref[type]
    return cgen.BasicType(f"struct {transitions_ref}")

  def type_to_capnp_state_ptr(self, type):
    state_ref = self.capnp_types.type_to_state_ref[type]
    return cgen.BasicType(f"{state_ref}_ptr")

  def type_to_capnp_transitions_ptr(self, type):
    transitions_ref = self.capnp_types.type_to_transitions_ref[type]
    return cgen.BasicType(f"{transitions_ref}_ptr")

  def type_to_capnp_state_write_function(self, type):
    state_ref = self.capnp_types.type_to_state_ref[type]
    return cgen.Var(f"write_{state_ref}", None)

  def type_to_capnp_transitions_write_function(self, type):
    transitions_ref = self.capnp_types.type_to_transitions_ref[type]
    return cgen.Var(f"write_{transitions_ref}", None)

  def type_to_capnp_state_new_ptr_function(self, type):
    state_ref = self.capnp_types.type_to_state_ref[type]
    return cgen.Var(f"new_{state_ref}", None)

  def type_to_capnp_transitions_new_ptr_function(self, type):
    transitions_ref = self.capnp_types.type_to_transitions_ref[type]
    return cgen.Var(f"new_{transitions_ref}", None)

  def type_to_capnp_state_read_function(self, type):
    state_ref = self.capnp_types.type_to_state_ref[type]
    return cgen.Var(f"read_{state_ref}", None)

  def type_to_capnp_transitions_read_function(self, type):
    transitions_ref = self.capnp_types.type_to_transitions_ref[type]
    return cgen.Var(f"read_{transitions_ref}", None)

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

  def capnp_transitions_module(self, expr):
    t = self._type_by_expr[expr]
    capnp_module = self.get_pycapnp_module()
    name = self.capnp_types.type_to_transitions_ref[t]
    return capnp_module.__dict__[name]

  def get_type_for_expr(self, expr):
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
    return self.c_types.type_to_state_ctype[self.get_type_for_expr(expr)]

  def get_c_transition_type(self, expr):
    return self.c_types.type_to_transitions_ctype[self.get_type_for_expr(expr)]

  def _compute_type(self, expr):
    if expr.__class__ == expression.Applied:
      arg_type = self.get_type_for_expr(expr.arg)
      if not isinstance(expr.func, primitive.PrimitiveOp):
        raise RuntimeError(
            f"Expected a normalized expression, but function an application of a non-PrimitiveOp: {expr.func}.")

      if not expr.func.get_input_type().equivalent(arg_type):
        raise RuntimeError(
            f"Badly typed normalized expression.  Applied a function taking {expr.func.get_input_type()} to an {arg_type}."
        )

      return expr.func.get_output_type()
    elif expr.__class__ == expression.Product:
      return types.Product(items=[(k, self.get_type_for_expr(v)) for k, v in expr.items])
    elif expr.__class__ == expression.Input:
      return expr.type
    else:
      raise RuntimeError(f"Unrecognized type of normalized expression {expr.__class__}.")

  def _generate_structs(self):
    '''Generate the graph struct in self.program.'''
    self._graph_struct = self._net.struct

    # -1 if the expr has not been subscribed to, otherwise the number of inputs that still need to be produced.
    self._graph_struct.AddField('n_missing_productions', cgen.Int32.Array(self._n_exprs()))

    # The number of output expressions (or graph outputs) that have yet to subscribe to the expr.
    self._graph_struct.AddField('n_missing_subscriptions', cgen.Int32.Array(self._n_exprs()))

    # Array of react_to_transitions* functions.  They should be called on initialized states when they
    # have input transitions to react to.
    self._graph_struct.AddField('react_to_transitions',
                                cgen.Function(cgen.UInt8, [self._graph_struct.Star()]).Star().Array(self._n_exprs()))

    self._turn_struct = self.program.AddStruct('turn')
    self._graph_struct.AddField('turn', self._turn_struct)

    # To hold a python dict of outputs.  This variable is used by chained produce* calls starting
    # from an OnInput call.
    self._turn_struct.AddField('result', cgen.PyObject.Star())

    self._turn_struct.AddField('remaining', cgen.Queue)
    self._turn_struct.AddField('was_added', cgen.UInt8.Array(cgen.Constant(len(self._top_exprs))))
    self._turn_struct.AddField('to_free', cgen.KVec(cgen.Void.Star()))

    for i, expr in enumerate(self._top_exprs):
      c_state_type = self.get_c_state_type(expr)
      self._graph_struct.AddField(self._state_key_in_graph(i), c_state_type)

      c_transition_type = self.get_c_transition_type(expr)
      self._turn_struct.AddField(self._transition_key_in_turn(i), cgen.KVec(c_transition_type))

  def _transition_key_in_turn(self, index):
    return f'transitions_{index}'

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
      init.AddAssignment(
          cgen.UpdateVar(init.SelfArg()).Arrow('n_missing_productions').Sub(cgen.Constant(i)), cgen.MinusOne)

    for i, expr in enumerate(self._top_exprs):
      n_expr_outputs = len(self.expr_to_outputs[expr])
      n_output_outputs = len(self._output_exprs.get(expr, []))
      init.AddAssignment(
          cgen.UpdateVar(init.SelfArg()).Arrow('n_missing_subscriptions').Sub(cgen.Constant(i)),
          cgen.Constant(n_expr_outputs + n_output_outputs))

    for expr in self._top_exprs:
      init.AddAssignment(None, cgen.kv_init(self.transitions_rvalue(init.SelfArg(), expr)))

    for i in range(len(self._top_exprs)):
      react = cgen.Var(self._react_to_transitions_function_name(i), None)
      init.AddAssignment(
          cgen.UpdateVar(init.SelfArg()).Arrow('react_to_transitions').Sub(cgen.Constant(i)), react.Address())

    init.AddReturn(cgen.Constant(0))

  def _shall_maintain_state_function_name(self):
    return 'shall_maintain_state'

  def _python_bytes_from_capn_function_name(self):
    return "python_bytes_from_capn"

  def _react_to_transitions_function_name(self, index):
    return f"react_to_transitions_{index}"

  def _write_output_state_function_name(self, index):
    return f"write_output_state_{index}"

  def _write_output_transitions_function_name(self, index):
    return f"write_output_transitions_{index}"

  def _initialize_state_function_name(self, index):
    return f"initialize_state_{index}"

  def _deserialize_transitions_function_name(self, index):
    return f"deserialize_transitions_{index}"

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
      n_missing_productions variable set to zero.
    Calling it ensures that any expression enabled by the setting of this state will be initialized and its
    produced function will be called.
    '''
    vGraph = cgen.Var('graph', self._graph_struct.Star())
    produce = self.program.AddFunction(name=self._produce_function_name(index), retType=cgen.Void, args=[vGraph])

    expr = self._top_exprs[index]

    if expr in self._output_exprs:
      getBytes = cgen.Var(self._write_output_state_function_name(index), None)
      vBytes = cgen.Var('result_bytes', cgen.PyObject.Star())
      produce.AddAssignment(cgen.CreateVar(vBytes), getBytes(vGraph))
      produce.AddIf(vBytes == cgen.NULL).consequent.AddReturnVoid()
      for key in self._output_exprs[expr]:
        (produce.AddIf(cgen.MinusOne == cgen.PyDict_SetItemString(
            vGraph.Arrow('turn').Dot('result'), cgen.StrConstant(key), vBytes)).consequent.AddReturnVoid())

    for output_expr in self.expr_to_outputs[expr]:
      output_index = self.expr_index[output_expr]

      vNMissingInputs = vGraph.Arrow('n_missing_productions').Sub(cgen.Constant(output_index))
      whenSubscribed = produce.AddIf(vNMissingInputs >= cgen.Zero).consequent

      whenSubscribed.AddAssignment(
          cgen.UpdateVar(vGraph).Arrow('n_missing_productions').Sub(cgen.Constant(output_index)),
          vNMissingInputs - cgen.One)

      whenReady = whenSubscribed.AddIf(vNMissingInputs == cgen.Zero).consequent
      initializeFunction = cgen.Var(self._initialize_state_function_name(output_index), None)
      produceFunction = cgen.Var(self._produce_function_name(output_index), None)
      whenReady.AddAssignment(None, initializeFunction(vGraph))
      whenReady.AddAssignment(None, produceFunction(vGraph))

    produce.AddReturnVoid()

  def pyerr_from_string(self, s):
    return cgen.PyErr_SetString(cgen.PyExc_RuntimeError, cgen.StrConstant(s))

  def _generate_write_output_transitions(self, key, expr):
    '''
    Generate the write_output_transitions_{key} function in c for ``expr``.
    '''
    index = self.expr_index[expr]
    exprType = self._state_types[index]
    vGraph = cgen.Var('graph', self._graph_struct.Star())
    write_output_transitions = self.program.AddFunction(
        name=self._write_output_transitions_function_name(index), retType=cgen.PyObject.Star(), args=[vGraph])

    vPythonBytes = cgen.Var('resulting_python_bytes', cgen.PyObject.Star())
    write_output_transitions.AddDeclaration(cgen.CreateVar(vPythonBytes))
    exprType.generate_c_transitions_to_capnp(self, write_output_transitions, self.transitions_rvalue(vGraph, expr),
                                             vPythonBytes)

    write_output_transitions.AddReturn(vPythonBytes)

  def _generate_write_output_state(self, key, expr):
    '''
    Generate the write_output_state_{key} function in c for ``expr``.
    '''
    index = self.expr_index[expr]
    exprType = self._state_types[index]
    vGraph = cgen.Var('graph', self._graph_struct.Star())
    write_output_state = self.program.AddFunction(
        name=self._write_output_state_function_name(index), retType=cgen.PyObject.Star(), args=[vGraph])

    vPythonBytes = cgen.Var('resulting_python_bytes', cgen.PyObject.Star())
    write_output_state.AddDeclaration(cgen.CreateVar(vPythonBytes))
    exprType.generate_c_state_to_capnp(self, write_output_state, self.state_rvalue(vGraph, expr), vPythonBytes)

    write_output_state.AddReturn(vPythonBytes)

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
        None, self.pyerr_from_string("Failed to create output dictionary")).AddReturn(cgen.NULL))

    subscribeFunction = cgen.Var(self._subscribe_function_name(output_index), None)
    ifHasState = on_output.AddIf(subscribeFunction(vGraph))
    whenHasState = ifHasState.consequent

    outputState = self.state_rvalue(vGraph, expr)

    vBytes = cgen.Var('result_bytes', cgen.PyObject.Star())
    getBytes = cgen.Var(self._write_output_state_function_name(output_index), None)
    whenHasState.AddAssignment(cgen.CreateVar(vBytes), getBytes(vGraph))

    (whenHasState.AddIf(vBytes == cgen.NULL).consequent.AddAssignment(None,
                                                                      cgen.Py_DECREF(vResult)).AddReturn(cgen.NULL))

    (whenHasState.Newline().AddIf(
        cgen.MinusOne == cgen.PyDict_SetItemString(vResult, cgen.StrConstant(key), vBytes)).consequent.AddAssignment(
            None, cgen.Py_DECREF(vResult)).AddAssignment(None, cgen.Py_DECREF(vBytes)).AddReturn(cgen.NULL))

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
            self.pyerr_from_string("Failed to initialize struct capn when parsing a message.")).AddReturn(cgen.NULL))

    on_input.Newline()

    vResult = cgen.Var('result', cgen.PyObject.Star())
    on_input.AddAssignment(cgen.CreateVar(vResult), cgen.PyDict_New())
    (on_input.AddIf(vResult == cgen.NULL).consequent.AddAssignment(
        None, self.pyerr_from_string("Failed to create output dictionary")).AddReturn(cgen.NULL))
    on_input.AddAssignment(cgen.UpdateVar(vGraph).Arrow('turn').Dot('result'), vResult)

    vCapnPtr = cgen.Var('msg_ptr', cgen.Capn_Ptr)
    on_input.AddAssignment(
        cgen.CreateVar(vCapnPtr), cgen.capn_getp(cgen.capn_root(vCapn.Address()), cgen.Zero, cgen.One))

    inputType.generate_capnp_to_c_state(self, on_input, vCapnPtr, self.state_lvalue(vGraph, expr))

    on_input.AddAssignment(None, cgen.capn_free(vCapn.Address()))
    on_input.AddAssignment(cgen.UpdateVar(vGraph).Arrow('n_missing_productions').Sub(cgen.Constant(index)), cgen.Zero)

    produceState = cgen.Var(self._produce_function_name(index), None)
    on_input.AddAssignment(None, produceState(vGraph))

    on_input.AddAssignment(cgen.UpdateVar(vGraph).Arrow('turn').Dot('result'), cgen.NULL)
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

    subscribe.AddAssignment(
        cgen.UpdateVar(vGraph).Arrow('n_missing_subscriptions').Sub(cgen.Constant(index)),
        vGraph.Arrow('n_missing_subscriptions').Sub(cgen.Constant(index)) - cgen.One)

    # Inputs will have their n_missing_productions value set to 0 only after they have been initialized.
    subscribe.AddReturn(vGraph.Arrow('n_missing_productions').Sub(cgen.Constant(index)) == cgen.Zero)

  def _generate_subscribe_noninput(self, index, expr):
    vGraph = cgen.Var('graph', self._graph_struct.Star())
    subscribe = self.program.AddFunction(name=self._subscribe_function_name(index), retType=cgen.Int32, args=[vGraph])

    subscribe.AddAssignment(
        cgen.UpdateVar(vGraph).Arrow('n_missing_subscriptions').Sub(cgen.Constant(index)),
        vGraph.Arrow('n_missing_subscriptions').Sub(cgen.Constant(index)) - cgen.One)

    missingInputsI = vGraph.Arrow('n_missing_productions').Sub(cgen.Constant(index))
    updateMissingInputsI = cgen.UpdateVar(vGraph).Arrow('n_missing_productions').Sub(cgen.Constant(index))

    ifAlreadySubscribed = subscribe.AddIf(missingInputsI >= cgen.Zero)
    ifAlreadySubscribed.consequent.AddReturn(missingInputsI == cgen.Zero)
    whenNotAlreadySubscribed = ifAlreadySubscribed.alternate

    nMissingInputs = cgen.Var('n_missing_productions', cgen.Int32)
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

  def _generate_shall_maintain_state(self):
    '''
    Generate a shall_maintain_state c function that determines whether an index must maintain its state
    as new transitions arrive.
    '''
    vGraph = cgen.Var('graph', self._graph_struct.Star())
    vIndex = cgen.Var('index', cgen.MachineInt)
    shall_maintain_state = self.program.AddFunction(
        name=self._shall_maintain_state_function_name(), retType=cgen.MachineInt, args=[vGraph, vIndex])

    # Current implementation: Check whether any other expr is still unsubscribed to it.
    shall_maintain_state.AddReturn(vGraph.Arrow('n_missing_subscriptions').Sub(vIndex) > cgen.Zero)

  def _generate_python_bytes_from_capnp(self):
    vCapn = cgen.Var('capn', cgen.Capn.Star())
    python_bytes_from_capn = self.program.AddFunction(
        name=self._python_bytes_from_capn_function_name(), retType=cgen.PyObject.Star(), args=[vCapn])

    vSize = cgen.Var('n_bytes', cgen.MachineInt)
    vBuf = cgen.Var('result_buf', cgen.UInt8.Star())
    vWroteBytes = cgen.Var('wrote_bytes', cgen.MachineInt)
    pyBuffer = cgen.Var('py_buffer_result', cgen.PyObject.Star())

    python_bytes_from_capn.AddDeclaration(cgen.CreateVar(vBuf))
    python_bytes_from_capn.AddDeclaration(cgen.CreateVar(vWroteBytes))
    python_bytes_from_capn.AddDeclaration(cgen.CreateVar(pyBuffer))
    python_bytes_from_capn.AddAssignment(cgen.CreateVar(vSize), cgen.Constant(4096))

    python_bytes_from_capn.Newline()

    loop = python_bytes_from_capn.AddWhile(cgen.true)

    loop.AddAssignment(vBuf, cgen.malloc(vSize).Cast(vBuf.type))
    (loop.AddIf(vBuf == cgen.NULL).consequent.AddAssignment(None, self.pyerr_from_string("malloc failed")).AddReturn(
        cgen.NULL))
    loop.AddAssignment(vWroteBytes, cgen.capn_write_mem(vCapn, vBuf, vSize, cgen.Zero))

    ifsuccess = loop.AddIf(vSize > vWroteBytes)
    ifsuccess.consequent.AddAssignment(pyBuffer, cgen.PyBytes_FromStringAndSize(
        vBuf.Cast(cgen.Char.Star()), vWroteBytes))
    (ifsuccess.consequent.AddIf(pyBuffer == cgen.NULL).consequent.AddAssignment(
        None, self.pyerr_from_string("Could not allocate a python bytes object.")))
    ifsuccess.consequent.AddAssignment(None, cgen.free(vBuf))
    ifsuccess.consequent.AddReturn(pyBuffer)

    loop.AddAssignment(None, cgen.free(vBuf))
    loop.AddAssignment(cgen.UpdateVar(vSize), vSize + vSize)

  def _generate_on_transitions(self):
    vTransitionsDict = cgen.Var('input_transitions_dict', cgen.PyObject.Star())
    on_transitions = self._net.AddMethod(name='OnTransitions', args=[vTransitionsDict]) # We'll do our own arg parsing
    vGraph = on_transitions.SelfArg()

    # Create the result dictionary
    vResult = cgen.Var('result', cgen.PyObject.Star())
    on_transitions.Newline()
    on_transitions.AddAssignment(cgen.CreateVar(vResult), cgen.PyDict_New())
    (on_transitions.AddIf(vResult == cgen.NULL).consequent.AddAssignment(
        None, self.pyerr_from_string("Failed to create output dictionary")).AddReturn(cgen.NULL))
    on_transitions.AddAssignment(cgen.UpdateVar(vGraph).Arrow('turn').Dot('result'), vResult)
    on_transitions.Newline()

    # Initialize the queue
    toFree = vGraph.Arrow('turn').Dot('to_free')
    on_transitions.AddAssignment(None, cgen.kv_init(toFree))

    # initialize was_added
    vIndexWas = cgen.Var('was_added_init_i', cgen.MachineInt)
    on_transitions.AddAssignment(cgen.CreateVar(vIndexWas), cgen.Zero)
    initWasAdded = on_transitions.AddWhile(vIndexWas < cgen.Constant(len(self._top_exprs)))
    initWasAdded.AddAssignment(cgen.UpdateVar(vGraph).Arrow('turn').Dot('was_added').Sub(vIndexWas), cgen.Zero)
    initWasAdded.AddAssignment(vIndexWas, vIndexWas + cgen.One)

    on_transitions.Newline().AddAssignment(
        cgen.UpdateVar(vGraph).Arrow('turn').Dot('remaining').Dot('count'), cgen.Zero)
    vRemainingData = cgen.Var('data', cgen.MachineInt.Array(cgen.Constant(len(self._top_exprs))))
    on_transitions.AddDeclaration(cgen.CreateVar(vRemainingData))
    on_transitions.AddAssignment(cgen.UpdateVar(vGraph).Arrow('turn').Dot('remaining').Dot('data'), vRemainingData)

    on_transitions.Newline()

    vKey, vValue = cgen.Var('input_key', cgen.PyObject.Star()), cgen.Var('input_value', cgen.PyObject.Star())
    vPos = cgen.Var('loop_pos', cgen.Py_ssize_t)
    on_transitions.AddDeclaration(cgen.CreateVar(vKey))
    on_transitions.AddDeclaration(cgen.CreateVar(vValue))
    on_transitions.AddAssignment(cgen.CreateVar(vPos), cgen.Zero)

    dictLoop = on_transitions.AddWhile(
        cgen.PyDict_Next(vTransitionsDict, vPos.Address(), vKey.Address(), vValue.Address()))

    condition = dictLoop
    for inputExpr in self._input_exprs:
      key = inputExpr.name
      input_index = self.expr_index[inputExpr]
      ifMatch = condition.AddIf(cgen.Zero == cgen.PyUnicode_CompareWithASCIIString(vKey, cgen.StrConstant(key)))
      (ifMatch.consequent.AddIf(
          vGraph.Arrow('n_missing_productions').Sub(cgen.Constant(input_index)) != cgen.Zero).consequent.AddAssignment(
              None,
              self.pyerr_from_string("Transitions were given for a key that has not been initialized.")).AddReturn(
                  cgen.NULL))
      ifMatch.consequent.AddAssignment(
          None, cgen.queue_push(vGraph.Arrow('turn').Dot('remaining').Address(), cgen.Constant(input_index)))
      deserializeTransitions = cgen.Var(self._deserialize_transitions_function_name(input_index), None)
      ifMatch.consequent.AddAssignment(None, deserializeTransitions(vGraph, vValue))
      condition = ifMatch.alternate

    (condition.AddAssignment(
        None, self.pyerr_from_string("keys of the argument OnTransition must correspond to inputs.")).AddReturn(
            cgen.NULL))

    queueLoop = on_transitions.Newline().AddWhile(cgen.Zero != vGraph.Arrow('turn').Dot('remaining').Dot('count'))

    nextIndex = cgen.Var('next_index', cgen.MachineInt)
    queueLoop.AddAssignment(cgen.CreateVar(nextIndex), cgen.queue_pop(vGraph.Arrow('turn').Dot('remaining').Address()))

    queueLoop.AddIf(vGraph.Arrow('react_to_transitions').Sub(nextIndex)(vGraph)).consequent.AddReturn(cgen.NULL)

    freeIndex = cgen.Var('free_index', cgen.MachineInt)
    on_transitions.Newline().AddAssignment(cgen.CreateVar(freeIndex), cgen.Zero)
    freeLoop = on_transitions.AddWhile(freeIndex < cgen.kv_size(toFree))
    kvecToFree = cgen.kv_A(toFree, freeIndex).Cast(cgen.KVec(cgen.Void).Star()).Deref()

    # make sure to free the vec, and reinitialize it.
    freeLoop.AddAssignment(None, cgen.kv_destroy(kvecToFree))
    freeLoop.AddAssignment(None, cgen.kv_init(kvecToFree))

    freeLoop.AddAssignment(cgen.UpdateVar(freeIndex), freeIndex + cgen.One)

    on_transitions.AddAssignment(None, cgen.kv_destroy(toFree))

    on_transitions.Newline().AddAssignment(cgen.UpdateVar(vGraph).Arrow('turn').Dot('result'), cgen.NULL)
    on_transitions.AddReturn(vResult)

  def transitions_rvalue(self, vGraph, expr):
    index = self.expr_index[expr]
    return vGraph.Arrow('turn').Dot(self._transition_key_in_turn(index))

  def _generate_react_to_transitions(self, expr):
    index = self.expr_index[expr]
    vGraph = cgen.Var('graph', self._graph_struct.Star())
    react = self.program.AddFunction(
        name=self._react_to_transitions_function_name(index),
        retType=cgen.UInt8, # Return 1 if there was an error
        args=[vGraph])

    shallMaintainState = cgen.Var(self._shall_maintain_state_function_name(), None)

    # Update the state and write the transitions.
    expr.generate_react_to_transitions(
        self,
        react.Newline(),
        vGraph,
        shallMaintainState(vGraph, cgen.Constant(index)),
    )

    if expr in self._output_exprs:
      getBytes = cgen.Var(self._write_output_transitions_function_name(index), None)
      vBytes = cgen.Var('result_bytes', cgen.PyObject.Star())
      react.Newline().AddAssignment(cgen.CreateVar(vBytes), getBytes(vGraph))
      react.AddIf(vBytes == cgen.NULL).consequent.AddReturn(cgen.true)
      for key in self._output_exprs[expr]:
        react.AddIf(cgen.MinusOne == cgen.PyDict_SetItemString(
            vGraph.Arrow('turn').Dot('result'), cgen.StrConstant(key), vBytes)).consequent.AddReturn(cgen.true)

    for next_expr in self.expr_to_outputs[expr]:
      nextIndex = cgen.Constant(self.expr_index[next_expr])
      whenShouldAdd = react.Newline().AddIf(
          cgen.BinOp(cgen.And, (vGraph.Arrow('n_missing_productions').Sub(nextIndex) == cgen.Zero),
                     (vGraph.Arrow('turn').Dot('was_added').Sub(nextIndex).Negate()))).consequent

      whenShouldAdd.AddAssignment(None, cgen.queue_push(vGraph.Arrow('turn').Dot('remaining').Address(), nextIndex))
      whenShouldAdd.AddAssignment(cgen.UpdateVar(vGraph).Arrow('turn').Dot('was_added').Sub(nextIndex), cgen.One)

    react.AddAssignment(
        None,
        cgen.kv_push(cgen.Void.Star(),
                     vGraph.Arrow('turn').Dot('to_free'),
                     self.transitions_rvalue(vGraph, expr).Address().Cast(cgen.Void.Star())))

    react.AddReturn(cgen.false)

  def _generate_deserialize_transitions(self, inputExpr):
    index = self.expr_index[inputExpr]
    vGraph = cgen.Var('graph', self._graph_struct.Star())
    vPythonList = cgen.Var('user_input_list_of_bytes', cgen.PyObject.Star())
    deserialize_transitions = self.program.AddFunction(
        name=self._deserialize_transitions_function_name(index), retType=cgen.Void, args=[vGraph, vPythonList])

    vKVec = vGraph.Arrow('turn').Dot(self._transition_key_in_turn(index))

    vNumber = cgen.Var('n_transitions', cgen.Py_ssize_t)
    deserialize_transitions.AddAssignment(cgen.CreateVar(vNumber), cgen.PyList_Size(vPythonList))
    vI = cgen.Var('i', cgen.Py_ssize_t)
    deserialize_transitions.AddAssignment(cgen.CreateVar(vI), cgen.Zero)

    listLoop = deserialize_transitions.Newline().AddWhile(vI < vNumber)

    vPythonBytes = cgen.Var('python_bytes', cgen.PyObject.Star())
    listLoop.AddAssignment(cgen.CreateVar(vPythonBytes), cgen.PyList_GetItem(vPythonList, vI))
    listLoop.AddIf(cgen.NULL == vPythonBytes).consequent.AddReturnVoid()

    listLoop.Newline()

    vBuf = cgen.Var('buf', cgen.Char.Star())
    vBuflen = cgen.Var('buflen', cgen.Py_ssize_t)
    vCapn = cgen.Var('capn', cgen.Capn)
    listLoop.AddDeclaration(cgen.CreateVar(vBuf))
    listLoop.AddDeclaration(cgen.CreateVar(vBuflen))
    listLoop.AddDeclaration(cgen.CreateVar(vCapn))
    (listLoop.AddIf(cgen.MinusOne == cgen.PyBytes_AsStringAndSize(vPythonBytes, vBuf.Address(), vBuflen.Address())).
     consequent.AddReturnVoid())

    (listLoop.AddIf(
        cgen.Zero != cgen.capn_init_mem(vCapn.Address(), vBuf.Cast(cgen.UInt8.Star()), vBuflen, cgen.Zero)
    ).consequent.AddAssignment(
        None,
        self.pyerr_from_string("Failed to initialize struct capn when parsing a transitions message.")).AddReturnVoid())

    vCapnPtr = cgen.Var('msg_ptr', cgen.Capn_Ptr)
    listLoop.AddAssignment(
        cgen.CreateVar(vCapnPtr), cgen.capn_getp(cgen.capn_root(vCapn.Address()), cgen.Zero, cgen.One))

    inputExpr.type.generate_capnp_to_c_transition(self, listLoop, vCapnPtr, vKVec)
    listLoop.AddAssignment(None, cgen.capn_free(vCapn.Address()))

    listLoop.Newline().AddAssignment(cgen.UpdateVar(vI), vI + cgen.One)

  def compile(self, output_key_to_norm_expr):
    self._output_key_to_norm_expr = output_key_to_norm_expr
    topsorter = _Topsorter(list(output_key_to_norm_expr.values()))
    self._top_exprs = topsorter.topsort()
    self.expr_to_inputs = topsorter.expr_to_inputs
    self.expr_to_outputs = topsorter.expr_to_outputs
    self.expr_index = {}
    for i, expr in enumerate(self._top_exprs):
      self.expr_index[expr] = i

    self._state_types = [self.get_type_for_expr(expr) for expr in self._top_exprs]
    self.expr_type = dict(zip(self._top_exprs, self._state_types))

    self._net = self.program.AddPythonType(name='Net', docstring=f"For running the {self.name} reactive network.")

    # Add c types for all
    for t in self._state_types:
      self.c_types.ensure_root_type(t)

    # Add capnproto types for outputs
    self._output_exprs = defaultdict(list)
    for key, expr in self._output_key_to_norm_expr.items():
      self._output_exprs[expr].append(key)
      self.capnp_types.ensure_root_type(self.get_type_for_expr(expr))

    # Add capnproto types for inputs
    self._input_exprs = []
    for expr in self._top_exprs:
      if expr.__class__ == expression.Input:
        self._input_exprs.append(expr)
        self.capnp_types.ensure_root_type(self.get_type_for_expr(expr))

    self._build_capnp()

    self._generate_initialize_root_graph()
    self._generate_python_bytes_from_capnp()
    self._generate_shall_maintain_state()

    for i in range(0, len(self._top_exprs)):
      self._generate_initialize_state(i)

    for i in range(0, len(self._top_exprs)):
      self._generate_subscribe(i)

    for key, expr in self._output_key_to_norm_expr.items():
      self._generate_write_output_state(key, expr)
      self._generate_write_output_transitions(key, expr)

    for i in range(len(self._top_exprs) - 1, -1, -1):
      self._generate_produce(i)

    for expr in self._input_exprs:
      self._generate_on_input(expr)
      self._generate_deserialize_transitions(expr)

    for key, expr in self._output_key_to_norm_expr.items():
      self._generate_on_output(key, expr)

    for expr in self._top_exprs:
      self._generate_react_to_transitions(expr)

    self._generate_on_transitions()

    # FIXME(KK): Remove
    with open('msg.capnp', 'w') as f:
      for line in self.capnp_types.capnp.lines():
        f.write(line)

    with open('example.c', 'w') as f:
      for line in self.program.to_c_string():
        f.write(line)

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
