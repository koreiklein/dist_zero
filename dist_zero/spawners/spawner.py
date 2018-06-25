class Spawner(object):
  '''
  Abstract base class for classes that can spawn and communicate with Machines.
  Each subclass should define a way to create and track machines, and to communicate
  with them once they have started running.
  '''

  @staticmethod
  def from_spawner_json(self, spawner_config):
    '''
    Build a new `Spawner` instance from spawner configuration json.

    :param object j: A python object, deserialized from spawner json
    :return: A spawner instance, configured from ``j``.
    :rtype: `Spawner`
    '''
    raise RuntimeError("Abstract Superclass")

  def mode(self):
    '''
    This spawner's mode.

    :return: The mode in which this spawner runs.
    :rtype: str
    '''
    raise RuntimeError("Abstract Superclass")

  def create_machine(self, machine_config):
    '''
    Start up a new machine and run a `MachineController` instance on it.

    :param object machine_config: A machine configuration object.

    :return: The id of the new `MachineController`
    :rtype: str
    '''
    raise RuntimeError("Abstract Superclass")

  def create_machines(self, machine_configs):
    '''
    Start up new machines and run `MachineController` instances on them.

    :param list machine_configs: A list of machine configuration objects.

    :return: The list of ids of the new `MachineController` in the same order as the matching 
    :rtype: list[str]
    '''
    raise RuntimeError("Abstract Superclass")

  def send_to_machine(self, machine_id, message, sock_type='udp'):
    '''
    Send a message from the current process to a `MachineController` listening
    on a port on a spawned machine.

    :param str machine_id: The id of the `MachineController` for one of the managed machines.
    :param message: Some json serializable message to send to that machine.
    :type message: :ref:`message`
    :param str sock_type: Either 'udp' or 'tcp'.  Indicating the type of connection.

    :return: None if sock_type == 'udp'.
      If sock_type == 'tcp', then return the response from the `MachineController` tcp API.
    :rtype: object
    '''
    raise RuntimeError("Abstract Superclass")
