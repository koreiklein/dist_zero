import logging

from dist_zero import errors

logger = logging.getLogger(__name__)

class RecordedUser(object):
  '''
  In tests, it can be helpful to generate recordings of user interactions
  and play them back against nodes in order to generate input for a distributed
  system.
  Instances of this class represent the recorded user.
  '''
  MAX_STEP_TIME_MS = 3
  def __init__(self, name, time_action_pairs=None):
    self.name = name
    self._time_action_pairs = [] if time_action_pairs is None else time_action_pairs
    self._started = False
    for i in range(1, len(self._time_action_pairs)):
      if self._time_action_pairs[i-1][0] > self._time_action_pairs[i][0]:
        raise errors.InternalError('Times are not in order.')

  def start(self):
    self.now_ms = 0
    self._index = 0
    self._started = True

  def to_json(self):
    return {
        'name': self.name,
        'time_action_pairs': self._time_action_pairs,
      }

  @staticmethod
  def from_json(recorded_user_json):
    return RecordedUser(
        name=recorded_user_json['name'],
        time_action_pairs=recorded_user_json['time_action_pairs'],
      )

  def elapse_and_get_messages(self, ms):
    '''
    :param int ms: The number of milliseconds to elapse.
    :yield: pairs (ms, msg) where ms is the amount of elapsed time, and msg is a :ref:`message` generated at that time.
    '''
    if not self._started:
      raise errors.InternalError("Can't elapse time until the RecordedUser has been started.")

    stop_time_ms = self.now_ms + ms
    while self.now_ms < stop_time_ms:
      step_time_ms = min(stop_time_ms - self.now_ms, RecordedUser.MAX_STEP_TIME_MS)
      step_stop_ms = self.now_ms + step_time_ms

      while self._index < len(self._time_action_pairs) and self._time_action_pairs[self._index][0] < step_stop_ms:
        yield step_time_ms, self._time_action_pairs[self._index][1]
        self._index += 1

      self.now_ms = step_stop_ms

  def record_actions(self, time_action_pairs):
    if self._started:
      raise errors.InternalError("Can't record additional actions after RecordedUser has started playback.")

    for tm in time_action_pairs:
      if len(self._time_action_pairs) > 0:
        if self._time_action_pairs[-1][0] > tm[0]:
          raise errors.InternalError('Times are not in order.')
      self._time_action_pairs.append(tm)


