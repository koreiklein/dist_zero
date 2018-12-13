import traceback


class DistZeroError(Exception):
  '''
  Base class for all errors known to DistZero.
  '''
  pass


class CapnpError(DistZeroError):
  '''For errors relating to capnproto.'''
  pass


class CapnpFormatError(CapnpError):
  '''
  For errors involving a badly structured capnproto file.
  '''
  pass


class CapnpCompileError(CapnpError):
  '''
  For errors compiling capnproto files.
  '''
  pass


class InternalError(DistZeroError):
  '''
  For errors internal to dist zero.
  '''
  pass


class NoRemainingAvailability(DistZeroError):
  '''
  For when there is not enough weight in the available nodes in a weighted round robin to
  assign kids to any of them.
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


class NoNodeForId(DistZeroError):
  '''For when we can't find a node on a machine for a given id.'''
  pass


class NoCapacityError(DistZeroError):
  '''
  Raised when attempting to add a data node to a tree that has no capacity for new nodes.

  NOTE: In general, data trees should always be spawning new nodes before they are about to run out of capacity.
  Ideally, the only case in which this error should be thrown is if either

  - nodes are being added faster than the tree can add capacity to accomodate them
    (e.g. the cloud provider can't provision new machines fast enough, or we're in a test in which
    the testing code neglects to let any time pass in between adding new nodes)
  - someone has erroneously attempted to add to a subtree with no capacity.  Although the entire tree
    should always be adding new capacity, subtrees are permitted to run out of capacity.
  '''

  def __init__(self):
    super(NoCapacityError, self).__init__()
