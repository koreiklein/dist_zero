import traceback


class DistZeroError(Exception):
  '''
  Base class for all errors known to DistZero.
  '''
  pass


class InternalError(DistZeroError):
  '''
  For errors internal to dist zero.
  '''
  pass


class NoTransportError(DistZeroError):
  def __init__(self, sender_id, receiver_id):
    msg = 'No transport has been initialized between sender {} and receiver {}'.format(sender_id, receiver_id)
    super(NoTransportError, self).__init__(msg)


class SimulationError(DistZeroError):
  def __init__(self, log_lines, exc_info):
    '''
    :param list log_lines: A list strings.  Each a formatted log line form the `SimulatedSpawner`
      that generated the error.
    :param tuple exc_info: The result of a call to `sys.exc_info`
    '''
    e_type, e_value, e_tb = exc_info
    exn_lines = traceback.format_exception_only(e_type, e_value)

    super(SimulationError, self).__init__(''.join(exn_lines))
