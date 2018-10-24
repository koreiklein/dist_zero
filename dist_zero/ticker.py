import asyncio


class Ticker(object):
  '''
  Ticker class to accept the passage of time and generate 'tick's every time
  some interval of time has elapsed.
  '''

  def __init__(self, interval_ms):
    '''
    :param int interval_ms: The time between ticks.
    '''
    self._current_ms = 0
    self.interval_ms = interval_ms

  def elapse(self, ms):
    '''
    Elapse some time.

    :param int ms: The number of elapsed milliseconds
    :return: The number of ticks that have passed over these ``ms`` of time.
    :rttype: int
    '''
    self._current_ms += ms
    result = self._current_ms // self.interval_ms
    self._current_ms %= self.interval_ms
    return result
