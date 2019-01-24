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

  def generate_initialize_state(self, compiler, stateInitFunction, vGraph):
    type = compiler.get_concrete_type(self.type)
    stateLvalue = compiler.state_lvalue(vGraph, self)
    type.generate_set_state(compiler, stateInitFunction, stateLvalue, self.start)

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
