import datetime
import heapq
import json
import logging
import random
import sys

import dist_zero.logging
import dist_zero.ids
from dist_zero import machine, errors, settings, spawners
from dist_zero.node import io

from . import spawner

logger = logging.getLogger(__name__)


class SimulatedSpawner(spawner.Spawner):
  '''
  A spawner class for simulating all machines within the calling process.

  This class is designed to be used only during tests in 'simulated' mode.

  Tests should typically create a single instance of this factory, and use it to generate
  all their `MachineController` instances.
  Then, this factory class will simulate the behavior of a real-life network during the test.
  '''

  # The number of milliseconds to simulate at a time
  MAX_STEP_TIME_MS = 5
  AVERAGE_SEND_TIME_MS = 10
  SEND_TIME_STDDEV_MS = 3

  def __init__(self, system_id, random_seed='random_seed'):
    '''
    :param str system_id: The id of the overall simulated distributed system.
    :param str random_seed: A random seed for all randomness employed by this class.
    '''

    self._system_id = system_id
    self._start_datetime = datetime.datetime.now()
    self._controller_by_id = {}
    self._elapsed_time_ms = None # None if unstarted, otherwise the number of ms simulated so far

    # a heap (as in heapq) of tuples (ms_at_which_receipt_takes place, message_to_receive)
    self._pending_receives = []
    self._random = random.Random(random_seed)

    # A log of all the items pushed onto the heap in the order they were pushed.
    # This log is useful for debugging.
    self._log = []

  def mode(self):
    return spawners.MODE_SIMULATED

  def _random_ms_for_send(self):
    return max(1,
               int(
                   self._random.gauss(
                       mu=SimulatedSpawner.AVERAGE_SEND_TIME_MS, sigma=SimulatedSpawner.SEND_TIME_STDDEV_MS)))

  def start(self):
    '''Begin the simulation'''
    if self._elapsed_time_ms is not None:
      raise RuntimeError("Can't start the same simulation twice")

    self._elapsed_time_ms = 0
    logger.info("=================================================")
    logger.info("========== STARTING SIMULATOR LOGGING  ==========")
    logger.info("=================================================")
    logger.info("Simulator for system: {system_id}", extra={'system_id': self._system_id})

  def _format_node_id(self, node_id):
    '''
    Format a node id as a human readable string to look nice in logs.
    :param str node_id: None, or a node id

    :return: A human readable string represending that node handle.
    :rtype: str
    '''
    if node_id is None:
      return "null"
    else:
      return node_id[:12]

  def _format_log(self, log_message):
    ms, msg = log_message
    message = msg['message']
    if message['type'] == 'machine_deliver_to_node':
      return "{} --{}--> {}".format(
          self._format_node_id(message.get('sending_node_id', None)),
          msg['message']['type'],
          self._format_node_id(message.get('node_id', None)),
      )
    else:
      return "Control Message: {}".format(message['type'])

  def run_for(self, ms):
    '''
    Run the simulation for a number of milliseconds.
    Wrap exceptions thrown by the underlying nodes in a `SimulationError`.

    :param int ms: The number of milliseconds to run for
    '''
    try:
      self._run_for_throwing_inner_exns(ms)
    except RuntimeError:
      exc_info = sys.exc_info()
      raise errors.SimulationError(log_lines=[self._format_log(x) for x in self._log], exc_info=exc_info)

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
      step_time_ms = min(stop_time_ms - self._elapsed_time_ms, SimulatedSpawner.MAX_STEP_TIME_MS)
      # The value of self._elapsed_time at the end of this iteration of the loop
      new_elapsed_time_ms = step_time_ms + self._elapsed_time_ms

      # Even for debug logs, the below logging statement is overly verbose.
      #logger.debug(
      #    "Simulating from {start_time} ms to {end_time} ms",
      #    extra={
      #        'start_time': self._elapsed_time_ms,
      #        'end_time': new_elapsed_time_ms,
      #    })

      # Simulate every event in the queue
      while self._pending_receives and self._pending_receives[0].t <= new_elapsed_time_ms:
        received_at, to_receive = heapq.heappop(self._pending_receives).tuple()
        # Simulate the time before this event
        for controller in self._controller_by_id.values():
          controller.elapse_nodes(received_at - self._elapsed_time_ms)

        # Simulate the event
        receiving_controller = self._controller_by_id[to_receive['machine_id']]
        receiving_controller.handle_message(message=to_receive['message'])

        self._elapsed_time_ms = received_at

      # Simulate the rest of step_time_ms not simulated in the above loop over events
      if self._elapsed_time_ms < new_elapsed_time_ms:
        for controller in self._controller_by_id.values():
          controller.elapse_nodes(new_elapsed_time_ms - self._elapsed_time_ms)

      self._elapsed_time_ms = new_elapsed_time_ms

  def get_machine_controller(self, handle):
    '''
    :param handle: The handle of a MachineController being simulated by this simulator.
    :type handle: :ref:`handle`

    :return: The associated `MachineController` instance.
    '''
    return self._controller_by_id[handle['id']]

  def create_machines(self, machine_configs):
    return [self.create_machine(machine_config) for machine_config in machine_configs]

  def create_machine(self, machine_config):
    result = machine.NodeManager(
        machine_config=machine_config,
        ip_host=machine_config['id'],
        spawner=self,
        send_to_machine=self._node_manager_send_to_machine,
    )
    self._controller_by_id[result.id] = result
    return result.id

  def _add_to_heap(self, heapitem):
    '''
    Add a new item to the heap.

    :param tuple heapitem: A pair (ms_at_which_receipt_takes place, message_to_receive)
    '''
    self._log.append(heapitem)
    heapq.heappush(self._pending_receives, _Event(*heapitem))

  def now_ms(self):
    return self._elapsed_time_ms

  def simulate_send_to_machine(self, machine_id, message, sock_type='udp'):
    '''
    Simulate a send of a message to the identified `MachineController`

    :param str machine_id: The id of the `MachineController` for one of the managed machines.
    :param message: Some json serializable message to send to that machine.
    :type message: :ref:`message`
    :param str sock_type: Either 'udp' or 'tcp'.  Indicating the type of connection.

    :return: None if sock_type == 'udp'.
      If sock_type == 'tcp', then return the response from the `MachineController` tcp API.
    :rtype: object
    '''
    # Simulate the serializing and deserializing that happens in other Spawners.
    # This behavior is important so that simulated tests don't accidentally share data.
    message = json.loads(json.dumps(message))

    if self._elapsed_time_ms is None:
      raise RuntimeError('The simulation must be started before it can send messages.')

    sending_time_ms = self._random_ms_for_send()
    if sock_type == 'udp':
      self._add_to_heap((self._elapsed_time_ms + sending_time_ms, {
          'machine_id': machine_id,
          'message': message,
      }))
    elif sock_type == 'tcp':
      self.run_for(ms=sending_time_ms)
      receiving_controller = self._controller_by_id[machine_id]
      response = receiving_controller.handle_api_message(message)
      if response['status'] == 'ok':
        return response['data']
      else:
        raise RuntimeError("Bad response from api: {}".format(response['reason']))
    else:
      raise RuntimeError("Unrecognized socket type '{}'".format(sock_type))

  def _node_manager_send_to_machine(self, message, transport):
    if self._elapsed_time_ms is None:
      raise RuntimeError('The simulation must be started before it can send messages.')

    self._add_to_heap((self._elapsed_time_ms + self._random_ms_for_send(), {
        'machine_id': transport['host'],
        'message': message,
    }))


class _Event(object):
  '''
  Instances of _Event represent events in the simulation.
  This class is here just to define the ordering on events.
  '''

  def __init__(self, t, value):
    self.t = t
    self.value = value

  def __lt__(self, other):
    if self.t != other.t:
      return self.t < other.t
    else:
      # Return a consistent answer when comparing events that occur at the exact same time.
      return json.dumps(self.value) < json.dumps(other.value)

  def tuple(self):
    return (self.t, self.value)
