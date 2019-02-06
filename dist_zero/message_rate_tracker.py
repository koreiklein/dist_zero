class MessageRateTracker(object):
  '''Used to track and estimate the current message rate.'''

  MIN_RATE_ESTIMATE_HZ = 3.0
  '''Minimum estimate for rate.  In hertz.'''
  MAX_WINDOW_SIZE_SEC = 1.5
  '''Once a window is becomes larger than this, we start a new window'''
  MAX_WINDOWS = 2
  '''Once there are more than this many windows, we dispense of the old ones.'''

  def __init__(self):
    # List of pairs (window_start_time_ms, number_of_messages)
    # in order of increasing window_start_time_ms
    self.windows = [(0, 0)]

  def increment(self, now_ms):
    '''Indicates to this tracker that a message was sent.'''
    self.windows[-1][1] += 1
    self._trim_windows(now_ms)

  def estimate_rate_hz(self, now_ms):
    '''
    Estimate and return the current message rate.

    :param int now_ms: The current time in milliseconds.
    :return: The message rate in hertz
    :rtype: float
    '''
    result = self._get_rate(now_ms)
    self._trim_windows(now_ms)
    return result

  def _total_messages_in_windows(self):
    return sum(n_msgs for start_time, n_msgs in self.windows)

  def _get_rate(self, now_ms):
    total_time_sec = (now_ms - self.windows[0][0]) / 1000.0
    total_time_sec = max(1.0, total_time_sec) # Avoid weird spikes when total_time_sec is too small
    return max(MessageRateTracker.MIN_RATE_ESTIMATE_HZ, float(self._total_messages_in_windows()) / total_time_sec)

  def _trim_windows(self, now_ms):
    if now_ms - self.windows[-1][0] >= MessageRateTracker.MAX_WINDOW_SIZE_SEC:
      self.windows.append((now_ms, 0))

      while len(self.windows) > MessageRateTracker.MAX_WINDOWS:
        self.windows.pop(0)
