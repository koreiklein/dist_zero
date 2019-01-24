import asyncio
import logging

from dist_zero import errors, cgen

from . import expression

logger = logging.getLogger(__name__)


class RecordedUser(expression.Expression):
  '''
  In tests, it can be helpful to generate recordings of user interactions
  and play them back against nodes in order to generate input for a distributed
  system.
  Instances of this class represent the recorded user.
  '''

  def __init__(self, name, start, type, time_action_pairs=None):
    self.name = name
    self.start = start
    self._type = type
    self._time_action_pairs = [] if time_action_pairs is None else time_action_pairs
    self._started = False
    for i in range(1, len(self._time_action_pairs)):
      if self._time_action_pairs[i - 1][0] > self._time_action_pairs[i][0]:
        raise errors.InternalError('Times are not in order.')

  @property
  def type(self):
    return self._type

  def _generate_play_recorded_transition(self, compiler):
    type = compiler.get_concrete_type(self.type)
    vGraphVoid = cgen.Void.Star().Var('graph_arg')
    data = cgen.Void.Star().Var('data')
    play_transition = compiler.program.AddFunction(
        f'play_recorded_{cgen.inc_i()}', cgen.Void, [vGraphVoid, data], predeclare=True)

    vGraph = play_transition.AddDeclaration(compiler.graph_struct.Star().Var('graph'),
                                            vGraphVoid.Cast(compiler.graph_struct.Star()))

    play_transition.AddAssignment(
        None,
        cgen.kv_push(type.c_transitions_type, compiler.transitions_rvalue(vGraph, self),
                     data.Cast(type.c_transitions_type.Star()).Deref()))

    compiler._generate_propogate_transitions(play_transition, vGraph, self)

    return play_transition

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    type = compiler.get_concrete_type(self.type)
    stateLvalue = compiler.state_lvalue(vGraph, self)
    type.generate_set_state(compiler, stateInitFunction, stateLvalue, self.start)

    play_recorded_transition = self._generate_play_recorded_transition(compiler)

    for when, python_transition in self._time_action_pairs:
      vTransition = stateInitFunction.AddDeclaration(
          type.c_transitions_type.Star().Var(f"recorded_transitions_{cgen.inc_i()}"))
      type.generate_allocate_transition(compiler, stateInitFunction, vTransition, python_transition)
      event = cgen.StructureLiteral(
          struct=cgen.BasicType('struct event'),
          key_to_expr={
              'when': cgen.Constant(when),
              'occur': play_recorded_transition.Address(),
              'data': vTransition,
          })
      (stateInitFunction.AddIf(cgen.event_queue_push(vGraph.Arrow('events').Address(), event)).consequent.AddAssignment(
          None, compiler.pyerr_from_string("Error pushing to event queue.")))

  def generate_free_state(self, compiler, block, stateRvalue):
    type = compiler.get_concrete_type(self.type)
    type.generate_free_state(compiler, block, stateRvalue)

  def generate_react_to_transitions(self, compiler, block, vGraph, maintainState):
    # No transitions should ever occur
    block.AddAssignment(None, compiler.pyerr_from_string("Recordeds do not react to transitions"))
    block.AddReturn(cgen.One)

  @property
  def actions(self):
    return [action for (t, action) in self._time_action_pairs]

  def simulate(self, controller, deliver):
    '''
    Start an asyncio task to simulate the messages recorded is self.
    Use controller.sleep_ms() to wait for the next message, and
    call deliver(m) with each message m when it arrives.
    '''
    self._started = True

    async def _loop(i):
      if i < len(self._time_action_pairs):
        t, m = self._time_action_pairs[i]
        await controller.sleep_ms(t if i == 0 else t - self._time_action_pairs[i - 1][0])
        deliver(m)
        asyncio.get_event_loop().create_task(_loop(i + 1))

    asyncio.get_event_loop().create_task(_loop(0))

  def to_json(self):
    return {
        'name': self.name,
        'start': self.start,
        'time_action_pairs': self._time_action_pairs,
    }

  @staticmethod
  def from_json(recorded_user_json):
    return RecordedUser(
        name=recorded_user_json['name'],
        start=recorded_user_json['start'],
        type=None, # Serialized Recorded instances should not ever need a type parameter
        time_action_pairs=recorded_user_json['time_action_pairs'],
    )

  def record_actions(self, time_action_pairs):
    if self._started:
      raise errors.InternalError("Can't record additional actions after RecordedUser has started playback.")

    for tm in time_action_pairs:
      if len(self._time_action_pairs) > 0:
        if self._time_action_pairs[-1][0] > tm[0]:
          raise errors.InternalError('Times are not in order.')
      self._time_action_pairs.append(tm)
