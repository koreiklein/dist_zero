import heapq
import json
import logging
import random
import sys
import uuid

from dist_zero import machine, errors
from dist_zero.node import io

logger = logging.getLogger(__name__)

class _Heapitem(object):
  def __init__(self, t, value):
    self.t = t
    self.value = value

  def __lt__(self, other):
    if self.t != other.t:
      return self.t < other.t
    else:
      return json.dumps(self.value) < json.dumps(other.value)

  def tuple(self):
    return (self.t, self.value)

class SimulatedMachineController(machine.MachineController):
  '''
  A MachineController for use in tests.  It does not actually control a machine
  but it implements the same interface and simulates what a MachineController would do.
  '''
  def __init__(self, name, hardware):
    '''
    :param str name: The name of this node.
    :param `SimulatedHardware` hardware: The simulated hardware instance on which this simulated machine runs.
    '''
    self.name = name
    '''The name of the simulated machine'''
    self.hardware = hardware
    '''The `SimulatedHardware` instance on which this machine is running'''

    self._node_by_id = {}
    self.id = str(uuid.uuid4())

    self._output_node_state_by_id = {}  # dict from output node to its current state

    self._requests = []

  def get_output_state(self, node_id):
    return self._output_node_state_by_id[node_id]

  def get_node(self, handle):
    return self._node_by_id[handle['id']]

  def ip_host(self):
    return 'localhost'

  def set_transport(self, sender, receiver, transport):
    pass

  def send(self, node_handle, message, sending_node_handle=None):
    self.hardware._simulate_send(
        receiving_node=node_handle,
        sending_node=sending_node_handle,
        message=message)

  def _update_output_node_state(self, node_id, f):
    new_state = f(self._output_node_state_by_id[node_id])
    self._output_node_state_by_id[node_id] = new_state

  def start_node(self, node_config):
    if node_config['type'] == 'output_leaf':
      self._output_node_state_by_id[node_config['id']] = node_config['initial_state']
      node = io.OutputLeafNode.from_config(
          node_config=node_config,
          controller=self,
          update_state=lambda f: self._update_output_node_state(node_config['id'], f))
    else:
      node = machine.node_from_config(node_config, controller=self)

    self._node_by_id[node.id] = node
    node.initialize()
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
  AVERAGE_SEND_TIME_MS = 10
  SEND_TIME_STDDEV_MS = 3

  def __init__(self, random_seed='random_seed'):
    self._controller_by_id = {}
    self._elapsed_time_ms = None  # None if unstarted, otherwise the number of ms simulated so far

    # a heap (as in heapq) of tuples (ms_at_which_receipt_takes place, message_to_receive)
    self._pending_receives = []
    self._random = random.Random(random_seed)

    # A log of all the items pushed onto the heap in the order they were pushed.
    # This log is useful for debugging.
    self._log = []

  def _random_ms_for_send(self):
    return max(1, int(self._random.gauss(
      mu=SimulatedHardware.AVERAGE_SEND_TIME_MS,
      sigma=SimulatedHardware.SEND_TIME_STDDEV_MS)))


  def start(self):
    '''Begin the simulation'''
    if self._elapsed_time_ms is not None:
      raise RuntimeError("Can't start the same simulation twice")

    self._elapsed_time_ms = 0

  def _format_node(self, node_handle):
    '''
    Format a node handle as a human readable string to look nice in logs.
    :param node_handle: None, or a node handle.
    :type node_handle: :ref:`handle`

    :return: A human readable string represending that node handle.
    :rtype: str
    '''
    if node_handle is None:
      return "null"
    else:
      return "{}.{}".format(node_handle['type'], str(node_handle['id'])[-5:])

  def _format_log(self, log_message):
    ms, msg = log_message
    return "{} --{}--> {}".format(
        self._format_node(msg['sending_node']),
        msg['message']['type'],
        self._format_node(msg['receiving_node']),
        )

  def run_for(self, ms):
    '''
    Run the simulation for a number of milliseconds.
    Wrap exceptions thrown by the underlying nodes in a `SimulationError`.

    :param int ms: The number of milliseconds to run for
    '''
    try:
      self._run_for_throwing_inner_exns(ms)
    except RuntimeError:
      raise errors.SimulationError(
        log_lines=[self._format_log(x) for x in self._log],
        exc_info=sys.exc_info())

  def _run_for_throwing_inner_exns(self, ms):
    '''
    Run the simulation for a number of milliseconds.  Raise any
    exception thrown by the underlying nodes directly.

    :param int ms: The number of milliseconds to run for
    '''
    if self._elapsed_time_ms is None:
      raise RuntimeError("Cannot run a simulate that is not yet started.  Please call .start() first")

    stop_time_ms = self._elapsed_time_ms + ms

    while stop_time_ms > self._elapsed_time_ms:
      # The amount of time to simulate in this iteration of the loop
      step_time_ms = min(stop_time_ms - self._elapsed_time_ms, SimulatedHardware.MAX_STEP_TIME_MS)
      # The value of self._elapsed_time at the end of this iteration of the loop
      new_elapsed_time_ms = step_time_ms + self._elapsed_time_ms
      logger.debug("Simulating from %s ms to %s ms", self._elapsed_time_ms, new_elapsed_time_ms, extra={
        'start_time': self._elapsed_time_ms,
        'end_time': new_elapsed_time_ms,
      })

      # Simulate every event in the queue
      while self._pending_receives and self._pending_receives[0].t <= new_elapsed_time_ms:
        received_at, to_receive = heapq.heappop(self._pending_receives).tuple()
        # Simulate the time before this event
        for controller in self._controller_by_id.values():
          controller.elapse(received_at - self._elapsed_time_ms)

        # Simulate the event
        receiving_controller = self._controller_by_id[to_receive['receiving_node']['controller_id']]
        receiving_node = receiving_controller.get_node(to_receive['receiving_node'])
        receiving_node.receive(message=to_receive['message'], sender=to_receive['sending_node'])

        self._elapsed_time_ms = received_at

      # Simulate the rest of step_time_ms not simulated in the above loop over events
      if self._elapsed_time_ms < new_elapsed_time_ms:
        for controller in self._controller_by_id.values():
          controller.elapse(new_elapsed_time_ms - self._elapsed_time_ms)

      self._elapsed_time_ms = new_elapsed_time_ms

  def get_machine_controller(self, handle):
    '''
    :param handle: The handle of a MachineController being simulated by this hardware simulator.
    :type handle: :ref:`handle`

    :return: The associated SimulatedMachineController instance.
    '''
    return self._controller_by_id[handle['id']]

  def new_simulated_machine_controller(self, name):
    '''
    Create and return a new machine.MachineController instance to run Nodes in a test.

    :param str name: The name to use for of the new machine controller.
    :return: A machine controller suitable for running tests.
    :rtype: `MachineController`
    '''
    result = SimulatedMachineController(name=name, hardware=self)
    self._controller_by_id[result.id] = result
    return result

  def _add_to_heap(self, heapitem):
    '''
    Add a new item to the heap.

    :param tuple heapitem: A pair (ms_at_which_receipt_takes place, message_to_receive)
    '''
    self._log.append(heapitem)
    heapq.heappush(self._pending_receives, _Heapitem(*heapitem))

  def _simulate_send(self, sending_node, receiving_node, message):
    '''
    Simulate the event of sending a message from one node to another.

    sending_node, receiving_node -- The handles of the sending and receiving nodes respectively.
    message -- The message being sent.
    '''
    if self._elapsed_time_ms is None:
      raise RuntimeError('The hardware simulation must be started before it can send messages.')

    time = self._elapsed_time_ms + self._random_ms_for_send()

    self._add_to_heap((time, {
      'sending_node': sending_node,
      'receiving_node': receiving_node,
      'message': message,
    }))

