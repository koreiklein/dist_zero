from dist_zero import machine, settings, errors, messages


class SystemController(object):
  '''
  Class to manage the entire distributed system for tests.
  '''

  # NOTE(KK): This class is entirely for tests at the moment.
  # The current plan is that in production, these kind of features will
  # be available on authorized MachineController instances.

  def __init__(self, spawner):
    '''
    :param spawner: The underlying Spawner subclass that spawns new machines.
    :type spawner: `Spawner`
    '''
    self._spawner = spawner

    self._node_id_to_machine_handle = {}
    '''For nodes spawned by this instance, map the node id to the handle of the machine it was spawned on.'''

  def create_kid_config(self, internal_node, new_node_name, machine_controller_handle):
    '''
    :param internal_node: The :ref:`handle` of the parent internalnode.
    :type internal_node: :ref:`handle`
    :param str new_node_name: The name to use for the new node.
    :param machine_controller_handle: The :ref:`handle` of the machine on which the new node will run.
    :type machine_controller_handle: :ref:`handle`

    :return: A node_config for creating the new kid node.
    :rtype: :ref:`message`
    '''
    machine_handle = self._node_id_to_machine_handle[internal_node['id']]
    return self._spawner.send_to_machine(
        machine=machine_handle,
        message=messages.api_create_kid_config(
            internal_node=internal_node,
            new_node_name=new_node_name,
            machine_controller_handle=machine_controller_handle,
        ),
        sock_type='tcp')

  def create_kid(self, parent_node, new_node_name, machine_controller_handle, recorded_user=None):
    node_config = self.create_kid_config(
        internal_node=parent_node,
        new_node_name=new_node_name,
        machine_controller_handle=machine_controller_handle,
    )
    if recorded_user is not None:
      node_config['recorded_user_json'] = recorded_user.to_json()
    return self.spawn_node(on_machine=machine_controller_handle, node_config=node_config)

  def spawn_node(self, node_config, on_machine):
    '''
    Start a node on a particular machine's container.

    :param node_config: A node config for a new node.
    :type node_config: :ref:`message`
    :param on_machine: The handle for a `MachineController`
    :type on_machine: :ref:`handle`

    :return: The node :ref:`handle` of the spawned node.
    '''
    node_id = node_config['id']
    self._spawner.send_to_machine(machine=on_machine, message=messages.machine_start_node(node_config))
    self._node_id_to_machine_handle[node_id] = on_machine
    return {'type': node_config['type'], 'id': node_id, 'controller_id': on_machine['id']}

  def create_transport_for(self, sender, receiver):
    '''
    Get and return a transport instance allowing sender to send to receiver.

    :param sender: The :ref:`handle` of a sending node.
    :type sender: :ref:`handle`
    :param receiver: The :ref:`handle` of a sending node.
    :type receiver: :ref:`handle`

    :return: A :ref:`transport` authorizing sender to send to receiver.
    :rtype: :ref:`transport`
    '''
    # Must get the transport from the intended receiver.
    return self._spawner.send_to_machine(
        machine=self._node_id_to_machine_handle[receiver['id']],
        sock_type='tcp',
        message=messages.api_new_transport(sender, receiver))

  def create_machine(self, machine_config):
    '''
    Start up a new machine and run a `MachineController` instance on it.

    :param object machine_config: A machine configuration object.

    :return: The :ref:`handle` of the new `MachineController`
    :rtype: :ref:`handle`
    '''
    return self._spawner.create_machine(machine_config)

  def get_output_state(self, output_node):
    '''
    Get the state associated with an output node.

    :param output_node: The :ref:`handle` of a output node.
    :type output_node: :ref:`handle`

    :return: The state of that node at about the current time.
    '''
    machine_handle = self._node_id_to_machine_handle[output_node['id']]
    return self._spawner.send_to_machine(
        machine=machine_handle, message=messages.api_get_output_state(node=output_node), sock_type='tcp')

  def send_to_node(self, node_handle, message, sending_node_handle=None):
    '''
    Send a message to a node.

    :param node_handle: The handle of some node.
    :type node_handle: :ref:`handle`

    :param message: A message for that node.
    :type message: :ref:`message`

    :param sending_node_handle: The :ref:`handle` of the sending node, or None if no node sent the message.
    :type sending_node_handle: :ref:`handle`
    '''
    machine_handle = self._node_handle_to_machine_handle(node_handle)
    machine_message = messages.machine_deliver_to_node(
        node=node_handle, message=message, sending_node=sending_node_handle)
    self._spawner.send_to_machine(machine=machine_handle, message=machine_message)

  def _node_handle_to_machine_handle(self, node_handle):
    return self._node_id_to_machine_handle[node_handle['id']]
