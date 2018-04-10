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


class SimulationError(DistZeroError):
  def __init__(self, log_lines, exc_info):
    '''
    :param list log_lines: A list strings.  Each a formatted log line form the `SimulatedHardware`
      that generated the error.
    :param tuple exc_info: The result of a call to `sys.exc_info()`
    '''
    e_type, e_value, e_tb = exc_info
    tb_lines = traceback.format_tb(e_tb)
    exn_lines = traceback.format_exception_only(e_type, e_value)
    msg = "{}\nAfter simulated trace:\n\t{}\nA node raised an error:  {}".format(
        ''.join(tb_lines), '\n\t'.join(log_lines), ''.join(exn_lines))

    super(SimulationError, self).__init__(msg)
