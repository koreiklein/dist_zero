import os
from collections import defaultdict

import capnp
capnp.remove_import_hook()

from dist_zero import cgen, errors, capnpgen, primitive, settings, concrete_types, recorded
from dist_zero.reactive import expression
from dist_zero import settings
from dist_zero import types, concrete_types

EVENT_QUEUE_INITIAL_CAPACITY = 10


class ReactiveCompiler(object):
  '''
  The root object for building a reactive program from a set of normalized expressions.

  Usage:

  - Create a ``compiler = ReactiveCompiler()`` instance.
  - Call `ReactiveCompiler.compile` to produce a python module from some normalize expressions.
  - Create a Net with from the generated python module
  - Call methods on the Net to run the reactive program.
  '''

  def __init__(self, name, docstring=''):
    '''
    :param str name: A name, safe to use in c variables and filenames.
    :param str docstring: The python docstring to use for the module this compiler will eventually generate.
    '''
    self.name = name
    self.docstring = docstring

    capnp_lib_dir = os.path.join(settings.CAPNP_DIR, 'c-capnproto', 'lib')

    self.program = cgen.Program(
        name=self.name,
        docstring=self.docstring,
        includes=[
            '"capnp_c.h"',
            f'"{self._capnp_header_filename()}"',
        ],
        library_dirs=[
            settings.CAPNP_DIR,
            capnp_lib_dir,
        ],
        sources=[
            os.path.join(self._capnp_dirname(), self._capnp_source_filename()),
            # NOTE(KK): We compile all these files into each extension.
            os.path.join(capnp_lib_dir, "capn.c"),
            os.path.join(capnp_lib_dir, "capn-malloc.c"),
            os.path.join(capnp_lib_dir, "capn-stream.c"),
        ],
        libraries=[],
        include_dirs=[
            self._capnp_dirname(),
            settings.CAPNP_DIR,
            capnp_lib_dir,
        ])

    self.type_to_concrete_type = {}
    self.capnp = capnpgen.CapnpFile(capnpgen.gen_capn_uid())

    self.BadInputError = self.program.AddException('BadReactiveInput')

    self.output_key_to_norm_expr = None

    self._finalize_turn = None # A function to clean up data associated with a turn
    self._initialize_turn = None # A function to initialize a turn

    self._cached_after_transitions_function = {} # map each expr to a function to run after adding new transitions
    self._shall_maintain_state = None
    self._serialize_output_transitions = None # Function to serialize all output transitions inside the turn
    self._write_output_transitions = {} # map expr to the function to write its output transitions to the turn result
    self._write_output_state = {} # map expr to the function to return the bytes object for its current state

    self._type_by_expr = {} # expr to dist_zero.types.Type
    self._concrete_type_by_type = {} # type to dist_zero.concrete_types.ConcreteType

    # when in the middle of generating code for a turn, this variable will refer to a kvec of pointers
    # that will be freed at the end of the turn,
    self.ptrsToFree = lambda vGraph: vGraph.Arrow('turn').Dot('ptrs_to_free')

    self._graph_struct = None
    self._turn_struct = None
    self._cached_n_exprs = None

    self._top_exprs = None
    self.expr_to_inputs = None
    self.expr_to_outputs = None
    self.expr_index = None
    self._input_exprs = None
    self._output_exprs = None # Dictionary from output expression to its list of keys
    self._net = None

    self._built_capnp = False
    self._pycapnp_module = None

  def compile(self, output_key_to_norm_expr, other_concrete_exprs=None):
    '''
    Compile normalized expressions into a reactive program.

      ``mod = reactive_compiler.compile(output_key_to_norm_expr)``

    The input program is provided as a dictionary that maps output keys to `ConcreteExpression` instances.
    Any expression used in constructing one of these "output" expressions is considered part of the program.
    Any such `dist_zero.reactive.expression.Input` expression is treated as an input to the reactive program.
    In general, much structure will be shared between distinct output expressions.

    The return value of ``compile`` will be a python module that exports a new type ``Net`` which
    can be used in the following ways:


    Passing in and receiving states:

      Each call to ``net = mod.Net()`` creates a separate instance of the
      reactive program described by ``output_key_to_norm_expr``.
      Once the reactive program ``net`` has been created, you can

        - Register an output with ``net.OnOutput_{output_key}()``.  This method may be called exactly once for each output key
          in ``output_key_to_norm_expr``
        - For each input expression ``I`` that was provided to `ReactiveCompiler.compile`, you can register the input key
          for ``I`` by calling ``net.OnInput_{I.name}(bytes)`` where ``bytes`` is a python bytes object containing
          a capnpproto serialized message for ``I``.  You can use `ReactiveCompiler.capnp_state_builder_for_type` to obtain
          a builder for such a python bytes object.

      Each of the above methods of ``Net`` will return a python dictionary mapping output keys to byte-like objects.
      For each mapping ``output_key`` -> ``bytes``, ``bytes`` will be a serialized capnproto message for that
      output key.  You can use ``compiler.capnp_state_builder_for_type(output_key_to_norm_expr[output_key].type)``
      to get a builder that will parse ``bytes``.

      Each output key will only ever produce at most one output state, and that state will be returned
      as soon as the output has received all the inputs it needs to calculate it.
      The calculated state will be exactly the one determined by its associated
      `ConcreteExpression`.

    Passing in and receiving transitions:

      ``net.OnTransitions(input_transitions)`` takes as input an ``input_transitions`` dictionary that maps
      certain registered input keys to lists of bytes objects representing capnproto transitions
      (use `ReactiveCompiler.capnp_transitions_builder_for_type` to obtain a builder that can generate the proper bytes).
      This method will return a dictionary mapping output keys to bytes-like objects with the
      appropriate output capnproto transitions.

      Output transitions will be returned whenever an update to an input expression leads to an update to an output
      expression.  The calculated transitions will be exactly those determined by the associated `ConcreteExpression`

    See :file:`test/test_reactives.py` for some examples of how to use reactives.

    :param output_key_to_norm_expr: A map from strings to normalized expressions.
    :type output_key_to_norm_expr: dict[str, ConcreteExpression]
    :param set other_exprs: If provided, a set of `ConcreteExpression` instances that should also be compiled in,
      whether or not they're accessible from an output key.

    :return: The compiled c extension module, loaded into the current interpret as a python module.
    '''
    all_exprs = set(other_concrete_exprs) if other_concrete_exprs is not None else set()
    all_exprs.update(output_key_to_norm_expr.values())
    self._output_key_to_norm_expr = output_key_to_norm_expr

    topsorter = _Topsorter(list(all_exprs))
    self._top_exprs = topsorter.topsort()

    self.expr_to_inputs = topsorter.expr_to_inputs
    self.expr_to_outputs = topsorter.expr_to_outputs
    self.expr_index = {}
    for i, expr in enumerate(self._top_exprs):
      self.expr_index[expr] = i

    self._concrete_types = [self.get_concrete_type(expr.type) for expr in self._top_exprs]

    self._net = self.program.AddPythonType(name='Net', docstring=f"For running the {self.name} reactive network.")

    # Add capnproto types for outputs
    self._output_exprs = defaultdict(list)
    for key, expr in self._output_key_to_norm_expr.items():
      self._output_exprs[expr].append(key)
      self.get_concrete_type(expr.type).initialize_capnp(self)

    # Add capnproto types for inputs
    self._input_exprs = []
    for expr in self._top_exprs:
      if expr.__class__ == expression.Input:
        self._input_exprs.append(expr)
        self.get_concrete_type(expr.type).initialize_capnp(self)
      elif expr.spy_keys:
        self.get_concrete_type(expr.type).initialize_capnp(self)

    self._build_capnp()

    self._generate_graph_struct()
    self._generate_graph_initializer()
    self._generate_graph_finalizer()
    self._generate_python_bytes_from_capnp()
    self._shall_maintain_state_function()

    self._generate_cur_time()
    self._generate_next_time()
    self._generate_elapse()

    for i in range(0, len(self._top_exprs)):
      self._generate_initialize_state(i)

    for i in range(0, len(self._top_exprs)):
      self._generate_subscribe(i)

    for expr in self._output_exprs.keys():
      self._write_output_state_function(expr)

    for expr in self._output_exprs.keys():
      self._write_output_transitions_function(expr)

    for i in range(len(self._top_exprs) - 1, -1, -1):
      self._generate_produce(i)

    for expr in self._input_exprs:
      self._generate_on_input(expr)
      self._generate_deserialize_transitions(expr)

    self._serialize_output_transitions_function()

    for key, expr in self._output_key_to_norm_expr.items():
      self._generate_on_output(key, expr)

    for expr in self._top_exprs:
      self._generate_react_to_transitions(expr)

    self._generate_on_transitions()

    spies = set()
    for expr in self._top_exprs:
      for key in expr.spy_keys:
        if key in spies:
          raise errors.InternalError(f"spy key \"{key}\" should not be used twice in the same reactive compiler.")
        else:
          self._write_output_state_function(expr)
          self._generate_spy_method(expr, key)

    if settings.c_debug:
      with open('msg.capnp', 'w') as f:
        for line in self.capnp.lines():
          f.write(line)

      with open('example.c', 'w') as f:
        for line in self.program.to_c_string():
          f.write(line)

    module = self.program.build_and_import()

    return module

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
      self.capnp.build_in(dirname=dirname, filename=filename)
      self._built_capnp = True

  def get_pycapnp_module(self):
    '''
    Return a python module for generating and parsing capnp messages.
    This method caches it's result, and should only be called after the program is finished being compiled.
    '''
    if self._pycapnp_module is None:
      self._build_capnp()
      dirname = self._capnp_dirname()
      filename = self._capnp_filename()

      self._pycapnp_module = capnp.load(os.path.join(dirname, filename), imports=[settings.CAPNP_DIR])

    return self._pycapnp_module

  def capnp_state_builder(self, expr):
    '''
    Get the capnp builder for ``expr``

    :param expr: A dist_zero expression involved in compiling the program.
    :type expr: `ConcreteExpression`

    :return: The pycapnp builder object for ``expr``.  The specific builder subclass class
      will be generated by the capnproto compiler.
    :rtype: `capnp._DynamicStructBuilder`
    '''
    t = self.get_concrete_type(expr.type).capnp_state_type
    capnp_module = self.get_pycapnp_module()
    return capnp_module.__dict__[t.name]

  def capnp_state_builder_for_type(self, t):
    '''
    Get the capnp builder for ``t``

    :param t: The dist_zero type involved in compiling the program.
    :type t: `dist_zero.types.Type`
    :return: The pycapnp builder object for ``t``.  The specific builder subclass class
      will be generated by the capnproto compiler.
    :rtype: `capnp._DynamicStructBuilder`
    '''
    capnp_module = self.get_pycapnp_module()
    return capnp_module.__dict__[self.get_concrete_type(t).name]

  def capnp_transitions_builder(self, expr):
    '''
    Get the capnp builder for transitions on ``expr``

    :param expr: A dist_zero expression involved in compling the program.
    :type expr: `ConcreteExpression`
    :return: The pycapnp builder object for transitions on ``expr``.  The specific builder subclass class
      will be generated by the capnproto compiler.
    :rtype: `capnp._DynamicStructBuilder`
    '''
    t = self.get_concrete_type(expr.type).capnp_transitions_type
    capnp_module = self.get_pycapnp_module()
    return capnp_module.__dict__[t.name]

  def capnp_transitions_builder_for_type(self, t):
    '''
    Get the capnp builder for transitions on ``t``

    :param t: The dist_zero type involved in compiling the program.
    :type t: `dist_zero.types.Type`
    :return: The pycapnp builder object for transitions on ``t``.  The specific builder subclass class
      will be generated by the capnproto compiler.
    :rtype: `capnp._DynamicStructBuilder`
    '''
    capnp_module = self.get_pycapnp_module()
    return capnp_module.__dict__[self.get_concrete_type(t).name]

  def get_concrete_type(self, t):
    '''
    :param t: A type in the input program.
    :type t: `dist_zero.types.Type`
    :return: The unique `ConcreteType` this `ReactiveCompiler` instance will eventually use to represent ``t``.
    :rtype: `ConcreteType`
    '''
    if t not in self._concrete_type_by_type:
      result = self._compute_concrete_type(t)
      self._concrete_type_by_type[t] = result
      return result
    else:
      return self._concrete_type_by_type[t]

  def state_lvalue(self, vGraph, expr):
    '''
    :param vGraph: The c variable for the relevant graph structure.
    :type vGraph: `CExpression`
    :param expr: Any expression in the input program. 
    :type expr: `ConcreteExpression`
    :return: The c lvalue that holds the current state of ``expr``.
    :rtype: `dist_zero.cgen.lvalue.Lvalue`
    '''
    index = self.expr_index[expr]
    return vGraph.Arrow(self._state_key_in_graph(index))

  def state_rvalue(self, vGraph, expr):
    '''
    :param vGraph: The c variable for the relevant graph structure.
    :type vGraph: `CExpression`
    :param expr: Any expression in the input program. 
    :type expr: `ConcreteExpression`
    :return: A c expression that holds the current state of ``expr``.
    :rtype: `CExpression`
    '''
    index = self.expr_index[expr]
    return vGraph.Arrow(self._state_key_in_graph(index))

  def transitions_rvalue(self, vGraph, expr):
    '''
    :param vGraph: The c variable for the relevant graph structure.
    :type vGraph: `CExpression`
    :param expr: Any expression in the input program. 
    :type expr: `ConcreteExpression`
    :return: A c expression that holds the current transitions kvec for ``expr``.
    :rtype: `CExpression`
    '''
    index = self.expr_index[expr]
    return vGraph.Arrow('turn').Dot(self._transition_key_in_turn(index))

  def _compute_concrete_type(self, t):
    '''Determine which `ConcreteType` to use for ``t``'''
    if isinstance(t, concrete_types.ConcreteType):
      return t.initialize(self)
    elif t.__class__ == types.Product:
      return concrete_types.ConcreteProductType(t).initialize(self)
    elif t.__class__ == types.Sum:
      return concrete_types.ConcreteSumType(t).initialize(self)
    elif t.__class__ == types.List:
      return concrete_types.ConcreteList(t).initialize(self)
    elif t.__class__ == types.FunctionType:
      raise errors.InternalError(
          "Reactive compiler can't produce a concrete type for a function type. It should have been normalized away.")
    elif t.__class__ == types.BasicType:
      return concrete_types.ConcreteBasicType(t).initialize(self)
    else:
      raise RuntimeError(f"Unrecognized dist_zero type {t.__class__}.")

  def _generate_graph_struct(self):
    '''Generate the graph struct in self.program.'''
    self._graph_struct = self._net.struct

    self._graph_struct.AddField('cur_time', cgen.UInt64)

    self._graph_struct.AddField('events', cgen.EventQueue)

    # -1 if the expr has not been subscribed to, otherwise the number of inputs that still need to be produced.
    self._graph_struct.AddField('n_missing_productions', cgen.Int32.Array(self._n_exprs()))

    # The number of output expressions (or graph outputs) that have yet to subscribe to the expr.
    self._graph_struct.AddField('n_missing_subscriptions', cgen.Int32.Array(self._n_exprs()))

    # Array of react_to_transitions* functions.  They should be called on initialized states when they
    # have input transitions to react to.
    self._graph_struct.AddField(
        'react_to_transitions',
        cgen.FunctionType(cgen.UInt8, [self._graph_struct.Star()]).Star().Array(self._n_exprs()))

    self._turn_struct = self.program.AddStruct('turn')
    self._graph_struct.AddField('turn', self._turn_struct)

    # To hold a python dict of outputs.  This variable is used by chained produce* calls starting
    # from an OnInput call.
    self._turn_struct.AddField('result', cgen.PyObject.Star())

    self._turn_struct.AddField('processed_transitions', cgen.MachineInt.Array(self._n_exprs()))

    # kvec of functions that will serialize output transitions into the turn
    self._turn_struct.AddField('turn_outputs', cgen.KVec(cgen.Void.Star()))
    # true iff expr i has been added to turn_outputs.  Used to avoid adding the same function to the array twice
    self._turn_struct.AddField('is_turn_output', cgen.UInt8.Array(self._n_exprs()))

    self._turn_struct.AddField('remaining', cgen.Queue)
    self._turn_struct.AddField('was_added', cgen.UInt8.Array(self._n_exprs()))
    self._turn_struct.AddField('vecs_to_free', cgen.KVec(cgen.Void.Star()))
    self._turn_struct.AddField('ptrs_to_free', cgen.KVec(cgen.Void.Star()))

    for i, expr in enumerate(self._top_exprs):
      ct = self.get_concrete_type(expr.type)
      self._graph_struct.AddField(self._state_key_in_graph(i), ct.c_state_type)
      self._turn_struct.AddField(self._transition_key_in_turn(i), cgen.KVec(ct.c_transitions_type))

  @property
  def graph_struct(self):
    return self._graph_struct

  def _n_exprs(self):
    if self._cached_n_exprs is None:
      self._cached_n_exprs = cgen.Constant(len(self._top_exprs))

    return self._cached_n_exprs

  def _generate_graph_initializer(self):
    '''Generate the graph initialization function.'''
    init = self._net.AddInit()
    vGraph = init.SelfArg()

    init.AddAssignment(vGraph.Arrow('cur_time'), cgen.Zero)
    (init.AddIf(cgen.event_queue_init(
        vGraph.Arrow('events').Address(), cgen.Constant(EVENT_QUEUE_INITIAL_CAPACITY))).consequent.AddAssignment(
            None, self.pyerr_from_string("Failed to allocate a new event queue")).AddReturn(cgen.MinusOne))
    init.Newline()

    init.AddAssignment(
        None, cgen.memset(vGraph.Arrow('turn').Dot('is_turn_output'), cgen.Zero,
                          self._n_exprs() * cgen.UInt8.Sizeof()))

    init.AddAssignment(None, cgen.kv_init(vGraph.Arrow('turn').Dot('turn_outputs')))

    init.Newline()

    for i, expr in enumerate(self._top_exprs):
      init.AddAssignment(vGraph.Arrow('n_missing_productions').Sub(i), cgen.MinusOne)

    for i, expr in enumerate(self._top_exprs):
      n_outputs = len(self._output_exprs.get(expr, []))
      for outputExpr in self.expr_to_outputs[expr]:
        if outputExpr.__class__ == expression.Product:
          # We add an extra output for a product expression to ensure that this expression's
          # state is maintained if the product's state must be maintained.
          # In the event that the product's state need NOT be maintained, it will satisfy this addition output.
          n_outputs += 2
        else:
          n_outputs += 1

      total_subscriptions = n_outputs
      if expr.spy_keys:
        total_subscriptions += 1 # 1 extra subscription for the spy key
      init.AddAssignment(vGraph.Arrow('n_missing_subscriptions').Sub(i), cgen.Constant(total_subscriptions))

    for expr in self._top_exprs:
      init.AddAssignment(None, cgen.kv_init(self.transitions_rvalue(vGraph, expr)))

    for i, expr in enumerate(self._top_exprs):
      if self._expr_can_react(expr):
        react = cgen.Var(self._react_to_transitions_function_name(i))
        init.AddAssignment(vGraph.Arrow('react_to_transitions').Sub(i), react.Address())

    for i, expr in enumerate(self._top_exprs):
      if expr.spy_keys:
        subscribeFunction = cgen.Var(self._subscribe_function_name(i))
        init.AddAssignment(None, subscribeFunction(vGraph))

    init.AddReturn(cgen.Constant(0))

  def _generate_graph_finalizer(self):
    '''Generate the graph finalization function.'''
    finalize = self._net.AddFinalize()

    vGraph = finalize.SelfArg()

    # Free memory associated with the events queue.
    with finalize.ForInt(vGraph.Arrow('events').Dot('count')) as (loop, eventIndex):
      vData = vGraph.Arrow('events').Dot('data').Sub(eventIndex).Dot('data')
      loop.AddIf(vData != cgen.NULL).consequent.AddAssignment(None, cgen.free(vData))
    finalize.AddAssignment(None, cgen.free(vGraph.Arrow('events').Dot('data'))).Newline()

    for i, expr in enumerate(self._top_exprs):
      ifInitialized = finalize.AddIf(cgen.Zero == vGraph.Arrow('n_missing_productions').Sub(i)).consequent
      expr.generate_free_state(self, ifInitialized, self.state_rvalue(vGraph, expr))

  def _python_bytes_from_capn_function_name(self):
    return "python_bytes_from_capn"

  def _transition_key_in_turn(self, index):
    return f'transitions_{index}'

  def _state_key_in_graph(self, index):
    return f'state_{index}'

  def _react_to_transitions_function_name(self, index):
    return f"react_to_transitions_{index}"

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

    vGraph = self._graph_struct.Star().Var('graph')
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
    vGraph = self._graph_struct.Star().Var('graph')
    produce = self.program.AddFunction(name=self._produce_function_name(index), retType=cgen.Void, args=[vGraph])

    expr = self._top_exprs[index]

    if expr in self._output_exprs:
      getBytes = self._write_output_state_function(expr)
      vBytes = produce.AddDeclaration(cgen.PyObject.Star().Var('result_bytes'), getBytes(vGraph))
      produce.AddIf(vBytes == cgen.NULL).consequent.AddReturnVoid()
      for key in self._output_exprs[expr]:
        (produce.AddIf(cgen.MinusOne == cgen.PyDict_SetItemString(
            vGraph.Arrow('turn').Dot('result'), cgen.StrConstant(key), vBytes)).consequent.AddReturnVoid())

    for output_expr in self.expr_to_outputs[expr]:
      output_index = self.expr_index[output_expr]

      vNMissingInputs = vGraph.Arrow('n_missing_productions').Sub(output_index)
      whenSubscribed = produce.AddIf(vNMissingInputs >= cgen.Zero).consequent

      whenSubscribed.AddAssignment(vGraph.Arrow('n_missing_productions').Sub(output_index), vNMissingInputs - cgen.One)

      whenReady = whenSubscribed.AddIf(vNMissingInputs == cgen.Zero).consequent
      initializeFunction = cgen.Var(self._initialize_state_function_name(output_index))
      produceFunction = cgen.Var(self._produce_function_name(output_index))
      whenReady.AddAssignment(None, initializeFunction(vGraph))
      whenReady.AddAssignment(None, produceFunction(vGraph))

    produce.AddReturnVoid()

  def pyerr(self, err_type, s, *args):
    '''
    Return a c function call that sets a python exception.
    :param err_type: A c variable that refers to a python exception type.
    :type err_type: `cgen.expression.Var`
    :param str s: The printf format string
    :param args: The c variables to matching the format specifiers in ``s``
    :type args: list[`cgen.expression.Var`]
    '''
    if len(args) == 0:
      return cgen.PyErr_SetString(err_type, cgen.StrConstant(s))
    else:
      return cgen.PyErr_Format(err_type, cgen.StrConstant(s), *args)

  def pyerr_from_string(self, s, *args):
    '''
    Return a c function call that sets a python RuntimeError.
    :param str s: The printf format string
    :param args: The c variables to matching the format specifiers in ``s``
    :type args: list[`cgen.expression.Var`]
    '''
    return self.pyerr(cgen.PyExc_RuntimeError, s, *args)

  def _write_output_transitions_function(self, expr):
    if expr not in self._write_output_transitions:
      index = self.expr_index[expr]
      exprType = self._concrete_types[index]
      vGraph = self._graph_struct.Star().Var('graph')
      block = self.program.AddFunction(f'write_output_transitions_{index}', cgen.UInt8, args=[vGraph])
      self._write_output_transitions[expr] = block

      vBytes = block.AddDeclaration(cgen.PyObject.Star().Var('resulting_python_bytes'))
      exprType.generate_c_transitions_to_capnp(self, block, self.transitions_rvalue(vGraph, expr), vBytes)

      block.AddIf(vBytes == cgen.NULL).consequent.AddReturn(cgen.true)
      for key in self._output_exprs[expr]:
        block.AddIf(cgen.MinusOne == cgen.PyDict_SetItemString(
            vGraph.Arrow('turn').Dot('result'), cgen.StrConstant(key), vBytes)).consequent.AddReturn(cgen.true)

      block.AddReturn(cgen.false)

    return self._write_output_transitions[expr]

  def _write_output_state_function(self, expr):
    '''
    Generate the write_output_state function in c for ``expr``.
    '''
    if expr not in self._write_output_state:
      index = self.expr_index[expr]
      exprType = self._concrete_types[index]
      vGraph = self._graph_struct.Star().Var('graph')
      write_output_state = self.program.AddFunction(
          name=f"write_output_state_{index}", retType=cgen.PyObject.Star(), args=[vGraph])
      self._write_output_state[expr] = write_output_state

      vPythonBytes = write_output_state.AddDeclaration(cgen.PyObject.Star().Var('resulting_python_bytes'))
      exprType.generate_c_state_to_capnp(self, write_output_state, self.state_rvalue(vGraph, expr), vPythonBytes)

      write_output_state.AddReturn(vPythonBytes)
    return self._write_output_state[expr]

  def _generate_on_output(self, key, expr):
    '''
    Generate the OnOutput_{key} function in c for ``expr``.
    '''
    on_output = self._net.AddMethod(name=self._on_output_function_name(key), args=None)
    output_index = self.expr_index[expr]

    vGraph = on_output.SelfArg()

    vResult = on_output.AddDeclaration(cgen.PyObject.Star().Var('result'), cgen.PyDict_New())

    (on_output.AddIf(vResult == cgen.NULL).consequent.AddAssignment(
        None, self.pyerr_from_string("Failed to create output dictionary")).AddReturn(cgen.NULL))

    subscribeFunction = cgen.Var(self._subscribe_function_name(output_index))
    ifHasState = on_output.AddIf(subscribeFunction(vGraph))
    whenHasState = ifHasState.consequent

    outputState = self.state_rvalue(vGraph, expr)

    getBytes = self._write_output_state_function(expr)
    vBytes = whenHasState.AddDeclaration(cgen.PyObject.Star().Var('result_bytes'), getBytes(vGraph))

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
    inputType = self.get_concrete_type(expr.type)

    on_input = self._net.AddMethod(name=self._on_input_function_name(expr), args=None) # We'll do our own arg parsing
    vGraph = on_input.SelfArg()
    vArgsArg = on_input.ArgsArg()

    vBuf = on_input.AddDeclaration(cgen.UInt8.Star().Var('buf'))
    vBuflen = on_input.AddDeclaration(cgen.MachineInt.Var('buflen'))
    vCapn = on_input.AddDeclaration(cgen.Capn.Var('capn'))

    whenParseFail = on_input.AddIf(
        cgen.PyArg_ParseTuple(vArgsArg, cgen.StrConstant("s#"), vBuf.Address(), vBuflen.Address()).Negate()).consequent
    whenParseFail.AddReturn(cgen.NULL)

    on_input.Newline()

    (on_input.AddIf(
        cgen.Zero != cgen.capn_init_mem(vCapn.Address(), vBuf, vBuflen, cgen.Zero)).consequent.AddAssignment(
            None, self.pyerr(self.BadInputError, "Failed to parse message input.")).AddReturn(cgen.NULL))

    on_input.Newline()

    vResult = on_input.AddDeclaration(cgen.PyObject.Star().Var('result'), cgen.PyDict_New())
    (on_input.AddIf(vResult == cgen.NULL).consequent.AddAssignment(
        None, self.pyerr_from_string("Failed to create output dictionary")).AddReturn(cgen.NULL))
    on_input.AddAssignment(vGraph.Arrow('turn').Dot('result'), vResult)

    ptr = on_input.AddDeclaration(inputType.capnp_state_type.c_ptr_type.Var(f'ptr'))
    on_input.AddAssignment(ptr.Dot('p'), cgen.capn_getp(cgen.capn_root(vCapn.Address()), cgen.Zero, cgen.One))

    inputType.generate_capnp_to_c_state(
        concrete_types.CapnpReadContext(compiler=self, block=on_input, ptrsToFree=None, ptr=ptr),
        self.state_lvalue(vGraph, expr))

    on_input.AddAssignment(None, cgen.capn_free(vCapn.Address()))
    on_input.AddAssignment(vGraph.Arrow('n_missing_productions').Sub(index), cgen.Zero)

    produceState = cgen.Var(self._produce_function_name(index))
    on_input.AddAssignment(None, produceState(vGraph))

    on_input.AddAssignment(vGraph.Arrow('turn').Dot('result'), cgen.NULL)
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
    '''see _generate_subscribe'''
    vGraph = self._graph_struct.Star().Var('graph')
    subscribe = self.program.AddFunction(name=self._subscribe_function_name(index), retType=cgen.Int32, args=[vGraph])

    subscribe.AddAssignment(
        vGraph.Arrow('n_missing_subscriptions').Sub(index),
        vGraph.Arrow('n_missing_subscriptions').Sub(index) - cgen.One)

    # Inputs will have their n_missing_productions value set to 0 only after they have been initialized.
    subscribe.AddReturn(vGraph.Arrow('n_missing_productions').Sub(index) == cgen.Zero)

  def _generate_subscribe_noninput(self, index, expr):
    '''see _generate_subscribe'''
    vGraph = self._graph_struct.Star().Var('graph')
    subscribe = self.program.AddFunction(name=self._subscribe_function_name(index), retType=cgen.Int32, args=[vGraph])

    subscribe.AddAssignment(
        vGraph.Arrow('n_missing_subscriptions').Sub(index),
        vGraph.Arrow('n_missing_subscriptions').Sub(index) - cgen.One)

    if expr.__class__ == expression.Product:
      ifZero = subscribe.AddIf(vGraph.Arrow('n_missing_subscriptions').Sub(index) == cgen.Zero)
      for inputExpr in self.expr_to_inputs[expr]:
        ifZero.consequent.AddAssignment(
            vGraph.Arrow('n_missing_subscriptions').Sub(self.expr_index[inputExpr]),
            vGraph.Arrow('n_missing_subscriptions').Sub(self.expr_index[inputExpr]) - cgen.One)

    subscribe.Newline()

    missingInputsI = vGraph.Arrow('n_missing_productions').Sub(index)
    updateMissingInputsI = vGraph.Arrow('n_missing_productions').Sub(index)

    ifAlreadySubscribed = subscribe.AddIf(missingInputsI >= cgen.Zero)
    ifAlreadySubscribed.consequent.AddReturn(missingInputsI == cgen.Zero)
    whenNotAlreadySubscribed = ifAlreadySubscribed.alternate

    nMissingInputs = whenNotAlreadySubscribed.AddDeclaration(
        cgen.Int32.Var('n_missing_productions'), cgen.Constant(len(self.expr_to_inputs[expr])))

    for inputExpr in self.expr_to_inputs[expr]:
      inputSubscribeFunction = cgen.Var(self._subscribe_function_name(self.expr_index[inputExpr]))
      ifInputIsReady = whenNotAlreadySubscribed.AddIf(inputSubscribeFunction(vGraph))
      ifInputIsReady.consequent.AddAssignment(nMissingInputs, nMissingInputs - cgen.One)

    whenNotAlreadySubscribed.AddAssignment(updateMissingInputsI, nMissingInputs)

    ifInputsAreSubscribed = whenNotAlreadySubscribed.AddIf(nMissingInputs == cgen.Zero)
    ifInputsAreSubscribed.alternate.AddReturn(cgen.false)
    whenInputsAreSubscribed = ifInputsAreSubscribed.consequent

    initializeFunction = cgen.Var(self._initialize_state_function_name(self.expr_index[expr]))
    whenInputsAreSubscribed.AddAssignment(None, initializeFunction(vGraph))
    whenInputsAreSubscribed.AddReturn(cgen.true)

  def _shall_maintain_state_function(self):
    '''
    Generate a shall_maintain_state c function that determines whether an index must maintain its state
    as new transitions arrive.
    '''
    if self._shall_maintain_state is None:
      vGraph = self._graph_struct.Star().Var('graph')
      vIndex = cgen.MachineInt.Var('index')
      shall_maintain_state = self.program.AddFunction(
          name='shall_maintain_state', retType=cgen.MachineInt, args=[vGraph, vIndex])

      # Current implementation: Check whether any other expr is still unsubscribed to it.
      shall_maintain_state.AddReturn(vGraph.Arrow('n_missing_subscriptions').Sub(vIndex) > cgen.Zero)

      self._shall_maintain_state = shall_maintain_state

    return self._shall_maintain_state

  def _generate_cur_time(self):
    '''Generate the CurTime C function.'''
    cur_time = self._net.AddMethod(name='CurTime', args=[])
    vGraph = cur_time.SelfArg()

    cur_time.AddReturn(cgen.PyLong_FromLong(vGraph.Arrow('cur_time')))

  def _generate_next_time(self):
    next_time = self._net.AddMethod(name='NextTime', args=[])
    vGraph = next_time.SelfArg()

    ifEmpty = next_time.AddIf(vGraph.Arrow('events').Dot('count') == cgen.Zero)
    ifEmpty.consequent.AddReturn(cgen.Py_None)
    ifEmpty.alternate.AddReturn(cgen.PyLong_FromLong(vGraph.Arrow('events').Dot('data').Sub(cgen.Zero).Dot('when')))

  def _generate_python_bytes_from_capnp(self):
    '''generate a c function to produce a python bytes object from a capnp structure.'''
    vCapn = cgen.Capn.Star().Var('capn')
    python_bytes_from_capn = self.program.AddFunction(
        name=self._python_bytes_from_capn_function_name(), retType=cgen.PyObject.Star(), args=[vCapn])

    vBuf = python_bytes_from_capn.AddDeclaration(cgen.UInt8.Star().Var('result_buf'))
    vWroteBytes = python_bytes_from_capn.AddDeclaration(cgen.MachineInt.Var('wrote_bytes'))
    pyBuffer = python_bytes_from_capn.AddDeclaration(cgen.PyObject.Star().Var('py_buffer_result'))
    vSize = python_bytes_from_capn.AddDeclaration(cgen.MachineInt.Var('n_bytes'), cgen.Constant(4096))

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
    loop.AddAssignment(vSize, vSize + vSize)

  def _generate_elapse(self):
    ms = cgen.UInt64.Var('ms')
    elapse = self._net.AddMethod(name="Elapse", args=[ms])
    vGraph = elapse.SelfArg()

    vResult = self._generate_output_dictionary(elapse, vGraph)
    elapse.AddAssignment(vGraph.Arrow('cur_time'), vGraph.Arrow('cur_time') + ms)

    elapse.logf("Responding to %llu ms of events.\n", ms)

    whenHasEvents = elapse.AddIf(self._has_events(vGraph)).consequent

    self._generate_initialize_turn(whenHasEvents, vGraph, vResult)
    self._generate_events_loop(whenHasEvents, vGraph, vResult)
    self._generate_finalize_turn(whenHasEvents, vGraph)

    elapse.AddReturn(vResult)

  def _has_events(self, vGraph):
    return (cgen.BinOp(cgen.And, (vGraph.Arrow('events').Dot('count') > cgen.Zero),
                       (vGraph.Arrow('events').Dot('data').Sub(cgen.Zero).Dot('when') <= vGraph.Arrow('cur_time'))))

  def _generate_events_loop(self, block, vGraph, vResult):
    loop = block.AddWhile(self._has_events(vGraph))
    for expr in self._top_exprs:
      loop.AddAssignment(self.vProcessedTransitions(vGraph, expr), cgen.kv_size(self.transitions_rvalue(vGraph, expr)))

    loop.AddAssignment(None, cgen.memset(vGraph.Arrow('turn').Dot('was_added'), cgen.Zero, self._n_exprs()))

    vEvent = loop.AddDeclaration(
        cgen.BasicType('struct event').Var('next_event'), cgen.event_queue_pop(vGraph.Arrow('events').Address()))
    loop.logf("Responding to event at time %llu.\n", vEvent.Dot('when'))
    loop.AddAssignment(None, vEvent.Dot('occur').Deref()(vGraph, vEvent.Dot('data')))

    queueLoop = loop.Newline().AddWhile(cgen.Zero != vGraph.Arrow('turn').Dot('remaining').Dot('count'))
    nextIndex = queueLoop.AddDeclaration(
        cgen.MachineInt.Var('next_index'), cgen.queue_pop(vGraph.Arrow('turn').Dot('remaining').Address()))
    reactFailed = queueLoop.AddIf(vGraph.Arrow('react_to_transitions').Sub(nextIndex)(vGraph)).consequent
    reactFailed.AddAssignment(None, cgen.Py_DECREF(vResult))
    reactFailed.AddAssignment(None, self._finalize_turn_function()(vGraph))
    reactFailed.AddReturn(cgen.NULL)

    serializeFailed = block.AddIf(self._serialize_output_transitions_function()(vGraph)).consequent
    serializeFailed.AddAssignment(None, cgen.Py_DECREF(vResult))
    serializeFailed.AddAssignment(vResult, cgen.NULL)

  def _generate_on_transitions(self):
    '''Generate the c function that implements the OnTransitions method of the Net object.'''
    vTransitionsDict = cgen.PyObject.Star().Var('input_transitions_dict')
    on_transitions = self._net.AddMethod(name='OnTransitions', args=[vTransitionsDict]) # We'll do our own arg parsing
    vGraph = on_transitions.SelfArg()

    vResult = self._generate_output_dictionary(on_transitions, vGraph)
    self._generate_initialize_turn(on_transitions, vGraph, vResult)
    self._generate_read_input_transitions(on_transitions, vGraph, vResult, vTransitionsDict)
    self._generate_queue_loop(on_transitions, vGraph, vResult)
    serializeFailed = on_transitions.AddIf(self._serialize_output_transitions_function()(vGraph)).consequent
    serializeFailed.AddAssignment(None, cgen.Py_DECREF(vResult))
    serializeFailed.AddAssignment(vResult, cgen.NULL)
    self._generate_finalize_turn(on_transitions, vGraph)
    on_transitions.AddReturn(vResult)

  def _generate_spy_method(self, expr, key):
    index = self.expr_index[expr]
    spy = self._net.AddMethod(name=f'Spy_{key}', args=[])
    vGraph = spy.SelfArg()

    ifHasState = spy.AddIf(vGraph.Arrow('n_missing_productions').Sub(index) == cgen.Zero)
    ifHasState.alternate.logf("Spy does not have a state. n_missing_productions = %d.\n",
                              vGraph.Arrow('n_missing_productions').Sub(index))

    block = ifHasState.consequent
    ifHasState.alternate.AddReturn(cgen.Py_None)

    getBytes = self._write_output_state_function(expr)
    block.AddReturn(getBytes(vGraph))

  def _generate_output_dictionary(self, block, vGraph):
    vResult = block.Newline().AddDeclaration(cgen.PyObject.Star().Var('result'), cgen.PyDict_New())
    (block.AddIf(vResult == cgen.NULL).consequent.AddAssignment(
        None, self.pyerr_from_string("Failed to create output dictionary")).AddReturn(cgen.NULL))
    block.Newline()
    return vResult

  def _generate_initialize_turn(self, block, vGraph, vResult):
    block.AddAssignment(vGraph.Arrow('turn').Dot('result'), vResult)

    vRemainingData = block.AddDeclaration(cgen.MachineInt.Array(self._n_exprs()).Var('data'))
    block.AddAssignment(None, self._initialize_turn_function()(vGraph, vRemainingData))

  def _initialize_turn_function(self):
    if self._initialize_turn is None:
      vGraph = self._graph_struct.Star().Var('graph')
      vRemainingData = cgen.MachineInt.Star().Var('data')
      self._initialize_turn = self.program.AddFunction(
          'initialize_turn', cgen.Void, args=[vGraph, vRemainingData], predeclare=True)
      block = self._initialize_turn

      # Initialize the queue
      block.AddAssignment(None, cgen.kv_init(self.ptrsToFree(vGraph)))

      # Initialize procesed_transitions
      block.AddAssignment(
          None,
          cgen.memset(
              vGraph.Arrow('turn').Dot('processed_transitions'), cgen.Zero,
              self._n_exprs() * cgen.MachineInt.Sizeof()))

      # initialize was_added
      block.AddAssignment(None, cgen.memset(vGraph.Arrow('turn').Dot('was_added'), cgen.Zero, self._n_exprs()))

      block.Newline().AddAssignment(vGraph.Arrow('turn').Dot('remaining').Dot('count'), cgen.Zero)
      block.AddAssignment(vGraph.Arrow('turn').Dot('remaining').Dot('data'), vRemainingData)

      block.Newline()

    return self._initialize_turn

  def _generate_read_input_transitions(self, block, vGraph, vResult, vTransitionsDict):
    vKey = block.AddDeclaration(cgen.PyObject.Star().Var('input_key'))
    vValue = block.AddDeclaration(cgen.PyObject.Star().Var('input_value'))
    vPos = block.AddDeclaration(cgen.Py_ssize_t.Var('loop_pos'), cgen.Zero)

    dictLoop = block.AddWhile(cgen.PyDict_Next(vTransitionsDict, vPos.Address(), vKey.Address(), vValue.Address()))

    condition = dictLoop
    for inputExpr in self._input_exprs:
      key = inputExpr.name
      input_index = self.expr_index[inputExpr]
      ifMatch = condition.AddIf(cgen.Zero == cgen.PyUnicode_CompareWithASCIIString(vKey, cgen.StrConstant(key)))
      (ifMatch.consequent.AddIf(
          vGraph.Arrow('n_missing_productions').Sub(input_index) != cgen.Zero).consequent.AddAssignment(
              None,
              self.pyerr(self.BadInputError,
                         f'Transitions were given for a key "{key}" that has not been initialized.')).AddAssignment(
                             None, cgen.Py_DECREF(vResult)).AddReturn(cgen.NULL))
      deserializeTransitions = cgen.Var(self._deserialize_transitions_function_name(input_index))
      ifMatch.consequent.AddAssignment(None, deserializeTransitions(vGraph, vValue))
      condition = ifMatch.alternate

    (condition.AddAssignment(
        None,
        self.pyerr(self.BadInputError, 'keys of the argument OnTransition must correspond to inputs. Got "%S"',
                   vKey)).AddAssignment(None, cgen.Py_DECREF(vResult)).AddReturn(cgen.NULL))

  def _generate_queue_loop(self, block, vGraph, vResult):
    queueLoop = block.Newline().AddWhile(cgen.Zero != vGraph.Arrow('turn').Dot('remaining').Dot('count'))
    nextIndex = queueLoop.AddDeclaration(
        cgen.MachineInt.Var('next_index'), cgen.queue_pop(vGraph.Arrow('turn').Dot('remaining').Address()))
    (queueLoop.AddIf(vGraph.Arrow('react_to_transitions').Sub(nextIndex)(vGraph)).consequent.AddAssignment(
        None, cgen.Py_DECREF(vResult)).AddAssignment(vResult, cgen.NULL).AddBreak())

  def vProcessedTransitions(self, vGraph, expr):
    return vGraph.Arrow('turn').Dot('processed_transitions').Sub(self.expr_index[expr])

  def _finalize_turn_function(self):
    if self._finalize_turn is None:
      vGraph = self._graph_struct.Star().Var('graph')
      self._finalize_turn = self.program.AddFunction("finalize_turn", cgen.Void, args=[vGraph], predeclare=True)
      block = self._finalize_turn

      # free from ptrs_to_free
      with block.Newline().ForInt(cgen.kv_size(self.ptrsToFree(vGraph))) as (freeLoop, ptrsFreeIndex):
        freeLoop.AddAssignment(None, cgen.free(cgen.kv_A(self.ptrsToFree(vGraph), ptrsFreeIndex)))
      block.AddAssignment(None, cgen.kv_destroy(self.ptrsToFree(vGraph)))

      # free from vecs_to_free
      for expr in self._top_exprs:
        transitions = self.transitions_rvalue(vGraph, expr)
        block.AddAssignment(None, cgen.kv_destroy(transitions))
        block.AddAssignment(None, cgen.kv_init(transitions))

      block.Newline().AddAssignment(vGraph.Arrow('turn').Dot('result'), cgen.NULL)

    return self._finalize_turn

  def _generate_finalize_turn(self, block, vGraph):
    block.AddAssignment(None, self._finalize_turn_function()(vGraph))

  def _expr_can_react(self, expr):
    return expr.__class__ not in [expression.Input, recorded.RecordedUser]

  def _generate_react_to_transitions(self, expr):
    '''
    Generate a c function that implement ``expr`` reacting to transitions on its inputs.
    This function will read the transitions for the input exprs to ``expr`` (from the `transitions_rvalue` kvecs),
    and based on their values, write output transitions in `transitions_rvalue` for ``expr``.

    :param expr: Any expression in the input program. 
    :type expr: `ConcreteExpression`
    '''
    if self._expr_can_react(expr):
      index = self.expr_index[expr]
      vGraph = self._graph_struct.Star().Var('graph')
      react = self.program.AddFunction(
          name=self._react_to_transitions_function_name(index),
          retType=cgen.UInt8, # Return 1 if there was an error
          args=[vGraph])

      # Update the state and write the transitions.
      expr.generate_react_to_transitions(
          self,
          react.Newline(),
          vGraph,
      )

      react.AddAssignment(None, self._after_transitions_function(expr)(vGraph))

      react.AddReturn(cgen.false)

  def _after_transitions_function(self, expr):
    if expr not in self._cached_after_transitions_function:
      index = self.expr_index[expr]
      vGraph = self._graph_struct.Star().Var('graph')
      block = self.program.AddFunction(f'after_transitions_{index}', cgen.Void, [vGraph], predeclare=True)
      self._cached_after_transitions_function[expr] = block

      if expr in self._output_exprs:
        isTurnOutput = vGraph.Arrow('turn').Dot('is_turn_output').Sub(index)
        whenNeedsToSetOutput = block.AddIf(isTurnOutput == cgen.Zero).consequent
        whenNeedsToSetOutput.AddAssignment(isTurnOutput, cgen.One)
        whenNeedsToSetOutput.AddAssignment(
            None,
            cgen.kv_push(cgen.Void.Star(),
                         vGraph.Arrow('turn').Dot('turn_outputs'),
                         self._write_output_transitions_function(expr).Address().Cast(cgen.Void.Star())))

      if expr.spy_keys:
        whenMaintainsState = block
      else:
        whenMaintainsState = block.AddIf(self._shall_maintain_state_function()(vGraph, cgen.Constant(index))).consequent
      transitions = self.transitions_rvalue(vGraph, expr)
      with whenMaintainsState.ForInt(cgen.kv_size(transitions)) as (loop, vIndex):
        ct = self.get_concrete_type(expr.type)
        if expr.__class__ == expression.Input and self.get_concrete_type(
            expr.type).__class__ == concrete_types.ConcreteProductType:
          generate_apply = ct.generate_product_apply_transition_forced
        else:
          generate_apply = ct.generate_apply_transition

        generate_apply(loop, self.state_lvalue(vGraph, expr), self.state_rvalue(vGraph, expr),
                       cgen.kv_A(transitions, vIndex))

      for next_expr in self.expr_to_outputs[expr]:
        nextIndex = cgen.Constant(self.expr_index[next_expr])
        whenShouldAdd = block.Newline().AddIf(
            cgen.BinOp(cgen.And, (vGraph.Arrow('n_missing_productions').Sub(nextIndex) == cgen.Zero),
                       (vGraph.Arrow('turn').Dot('was_added').Sub(nextIndex).Negate()))).consequent

        whenShouldAdd.AddAssignment(None, cgen.queue_push(vGraph.Arrow('turn').Dot('remaining').Address(), nextIndex))
        whenShouldAdd.AddAssignment(vGraph.Arrow('turn').Dot('was_added').Sub(nextIndex), cgen.One)

      block.AddAssignment(
          None,
          cgen.kv_push(cgen.Void.Star(),
                       vGraph.Arrow('turn').Dot('vecs_to_free'),
                       self.transitions_rvalue(vGraph, expr).Address().Cast(cgen.Void.Star())))

    return self._cached_after_transitions_function[expr]

  def _serialize_output_transitions_function(self):
    if self._serialize_output_transitions is None:
      vGraph = self._graph_struct.Star().Var('graph')
      block = self.program.AddFunction('serialize_output_transitions', cgen.UInt8, [vGraph])
      self._serialize_output_transitions = block

      vTurnOutputs = vGraph.Arrow('turn').Dot('turn_outputs')
      block.logf('Serializing %zu outputs.\n', cgen.kv_size(vTurnOutputs))
      with block.ForInt(cgen.kv_size(vTurnOutputs)) as (loop, index):
        failed = loop.AddIf(
            cgen.kv_A(vTurnOutputs, index).Cast(cgen.BasicType('uint8_t (*)(struct Net *)')).Deref()
            (vGraph) != cgen.Zero).consequent
        failed.AddReturn(cgen.One)

      block.AddAssignment(
          None, cgen.memset(
              vGraph.Arrow('turn').Dot('is_turn_output'), cgen.Zero,
              self._n_exprs() * cgen.UInt8.Sizeof()))

      block.AddAssignment(None, cgen.kv_destroy(vTurnOutputs))
      block.AddAssignment(None, cgen.kv_init(vTurnOutputs))
      block.AddReturn(cgen.Zero)

    return self._serialize_output_transitions

  def _generate_deserialize_transitions(self, inputExpr):
    '''
    Generate a c function to convert from a capnproto representation of a transition on an input expression
    to the internal c representation inside the graph struct.
    '''
    index = self.expr_index[inputExpr]
    vGraph = self._graph_struct.Star().Var('graph')
    vPythonList = cgen.PyObject.Star().Var('user_input_list_of_bytes')
    deserialize_transitions = self.program.AddFunction(
        name=self._deserialize_transitions_function_name(index), retType=cgen.Void, args=[vGraph, vPythonList])

    vKVec = vGraph.Arrow('turn').Dot(self._transition_key_in_turn(index))

    vNumber = deserialize_transitions.AddDeclaration(
        cgen.Py_ssize_t.Var('n_transitions'), cgen.PyList_Size(vPythonList))
    vI = deserialize_transitions.AddDeclaration(cgen.Py_ssize_t.Var('i'), cgen.Zero)

    listLoop = deserialize_transitions.Newline().AddWhile(vI < vNumber)

    vPythonBytes = listLoop.AddDeclaration(cgen.PyObject.Star().Var('python_bytes'), cgen.PyList_GetItem(
        vPythonList, vI))
    listLoop.AddIf(cgen.NULL == vPythonBytes).consequent.AddReturnVoid()

    listLoop.Newline()

    vBuf = listLoop.AddDeclaration(cgen.Char.Star().Var('buf'))
    vBuflen = listLoop.AddDeclaration(cgen.Py_ssize_t.Var('buflen'))
    vCapn = listLoop.AddDeclaration(cgen.Capn.Var('capn'))
    (listLoop.AddIf(cgen.MinusOne == cgen.PyBytes_AsStringAndSize(vPythonBytes, vBuf.Address(), vBuflen.Address())).
     consequent.AddReturnVoid())

    (listLoop.AddIf(cgen.Zero != cgen.capn_init_mem(vCapn.Address(), vBuf.Cast(cgen.UInt8.Star()), vBuflen, cgen.Zero)).
     consequent.AddAssignment(
         None, self.pyerr(self.BadInputError,
                          "Failed to initialize struct capn when parsing a transitions message.")).AddReturnVoid())

    listLoop.Newline()

    concreteInputType = self.get_concrete_type(inputExpr.type)
    ptr = listLoop.AddDeclaration(concreteInputType.capnp_transitions_type.c_ptr_type.Var(f'ptr'))
    listLoop.AddAssignment(ptr.Dot('p'), cgen.capn_getp(cgen.capn_root(vCapn.Address()), cgen.Zero, cgen.One))

    read_ctx = concrete_types.CapnpReadContext(
        compiler=self, block=listLoop, ptrsToFree=vGraph.Arrow('turn').Dot('ptrs_to_free'), ptr=ptr)
    for cblock, cexp in concreteInputType.generate_and_yield_capnp_to_c_transition(read_ctx):
      cblock.AddAssignment(None, cgen.kv_push(concreteInputType.c_transitions_type, vKVec, cexp))

    listLoop.AddAssignment(None, cgen.capn_free(vCapn.Address()))

    listLoop.Newline().AddAssignment(vI, vI + cgen.One)

    deserialize_transitions.AddAssignment(None, self._after_transitions_function(inputExpr)(vGraph))


class _Topsorter(object):
  '''
  Helper class to populate ReactiveCompiler._top_exprs via topological traversal of `ConcreteExpression`
  instances referenced by a list of root expressions.
  '''

  def __init__(self, root_exprs):
    '''
    :param list root_exprs: A list of `ConcreteExpression` instances in any order.
    '''
    self.root_exprs = root_exprs
    self.visited = set()
    self.active = set()
    self.result = []

    self.expr_to_inputs = {}
    self.expr_to_outputs = defaultdict(list)

  def topsort(self):
    '''
    Populate all the parameters of self by traversing expressions in ``self.root_exprs`` in topological order.
    '''
    for expr in self.root_exprs:
      self._visit(expr)

    for expr, inputExprs in self.expr_to_inputs.items():
      for inputExpr in inputExprs:
        self.expr_to_outputs[inputExpr].append(expr)

    return self.result

  def _visit(self, expr):
    '''Visit a single expression.  It may have already been visited.'''
    if expr in self.visited:
      return
    elif expr in self.active:
      raise errors.InternalError("Cycle detected in normalized expression.")
    else:
      self.active.add(expr)
      inputs = []
      self.expr_to_inputs[expr] = inputs
      try:
        for kid in self._yield_kids(expr):
          inputs.append(kid)
          self._visit(kid)
      finally:
        self.active.remove(expr)
        self.result.append(expr)
        self.visited.add(expr)

  def _yield_kids(self, expr):
    '''
    yield all "kids" of ``expr``
    A "kid" in any other expression ``k`` such that the value of ``expr`` depends directly on ``k``
    '''
    if expr.__class__ == expression.Applied:
      yield expr.arg
    elif expr.__class__ == expression.Product:
      for key, kid in expr.items:
        yield kid
    elif expr.__class__ == expression.Input:
      return
    elif expr.__class__ == expression.Project:
      yield expr.base
    elif expr.__class__ in [expression.Constant, recorded.RecordedUser]:
      pass
    else:
      raise errors.InternalError(f"Unrecognized type of normalized expression {expr.__class__}.")
