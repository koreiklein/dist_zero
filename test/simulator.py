import heapq
import random
import uuid

from dist_zero import machine

class SimulatedMachineController(machine.MachineController):
  '''
  A MachineController for use in tests.  It does not actually control a machine
  but it implements the same interface and simulates what a MachineController would do.
  '''
  def __init__(self, name, hardware):
    '''
    name -- The name of this node.
    hardware -- The SimulatedHardware instance on which this simulated machine runs.
    '''
    self.name = name
    self.hardware = hardware

    self._node_by_id = {}
    self.id = uuid.uuid4()

    self._requests = []

  def get_node(self, handle):
    return self._node_by_id[handle['id']]

  def send(self, node_handle, message, sending_node_handle=None):
    self.hardware._simulate_send(
        receiving_node=node_handle,
        sending_node=sending_node_handle,
        message=message)

  def start_node(self, node_config):
    node = machine.node_from_config(node_config, controller=self)

    self._node_by_id[node.id] = node
    return node

  def spawn_node(self, node_config, on_machine):
    machine_controller = self.hardware.get_machine_controller(on_machine)
    node = machine_controller.start_node(node_config)
    return node.handle()

  def handle(self):
    return { 'type': 'SimulatedMachineController', 'id': self.id }

  def elapse(self, ms):
    for node in self._node_by_id.values():
      node.elapse(ms)

class SimulatedHardware(object):
  '''
  A class for creating SimulatedMachineControllers to simulate distinct machines in tests.

  Tests should typically create a single instance of this factory, and use it to generate
  all their machine.MachineController instances.
  Then, this factory class will simulate the behavior of a real-life network during the test.
  '''

  # The number of milliseconds to simulate at a time
  MAX_STEP_TIME_MS = 5
  AVERAGE_SEND_TIME_MS = 20
  SEND_TIME_STDDEV_MS = 3

  def __init__(self, random_seed='random_seed'):
    self._controller_by_id = {}
    self._elapsed_time_ms = None  # None if unstarted, otherwise the number of ms simulated so far

    # A dictionary taking the id of each controller to
    # a heap (as in heapq) of tuples (ms_at_which_receipt_takes place, message_to_receive)
    self._pending_receives = defaultdict([])
    self._random = random.Random(random_seed)

  def _random_ms_for_send(self):
    return int(self._random.gauss(
      mu=SimulatedHardware.AVERAGE_SEND_TIME_MS,
      sigma=SimulatedHardware.SEND_TIME_STDDEV_MS))


  def start(self):
    '''Begin the simulation'''
    if self._elapsed_time_ms is not None:
      raise RuntimeError("Can't start the same simulation twice")

    self._elapsed_time_ms = 0

  def run_for(self, ms):
    '''
    Run the simulation for a number of milliseconds

    ms -- The number of milliseconds to run for
    '''
    if self._elapsed_time_ms is None:
      raise RuntimeError("Cannot run a simulate that is not yet started.  Please call .start() first")

    stop_time_ms = self._elapsed_time_ms + ms

    while stop_time_ms > self._elapsed_time_ms:
      step_time_ms = min(stop_time_ms - self._elapsed_time_ms, SimulatedHardware.MAX_STEP_TIME_MS)
      new_elapsed_time_ms = step_time_ms + self._elapsed_time_ms
      while len(self._pending_sends) > 0 and self._pending_sends[0][0] <= new_elapsed_time_ms:
        receive_time, send = heapq.heappop(self._pending_sends)


      for controller in self._controller_by_id.values():
        controller.elapse(step_time_ms)

      self._elapsed_time_ms = new_elapsed_time_ms

  def get_machine_controller(self, handle):
    '''
    handle -- The handle of a MachineController being simulated by this hardware simulator.
    return -- The associated SimulatedMachineController instance.
    '''
    return self._controller_by_id[handle['id']]

  def new_simulated_machine_controller(self, name):
    '''
    Create and return a new machine.MachineController instance to run Nodes in a test.

    name -- A string to use as the name of the new machine controller>
    return -- A machine.MachineController instance suitable for running tests.
    '''
    result = SimulatedMachineController(name=name, hardware=self)
    self._controller_by_id[result.id] = result
    return result

  def _simulate_send(self, sending_node, receiving_node, message):
    '''
    Simulate the event of sending a message from one node to another.

    sending_node, receiving_node -- The handles of the sending and receiving nodes respectively.
    message -- The message being sent.
    '''
    if self._elapsed_time_ms is None:
      raise RuntimeError('The hardware simulation must be started before it can send messages.')

    time = self._elapsed_time_ms + self._random_ms_for_send()
    heapq.heappush(self._pending_receives[to_controller_id], (time, {
      'from_id': from_id,
      'to_id': to_id,
      'message': message,
    }))

