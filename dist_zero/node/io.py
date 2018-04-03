import uuid

from dist_zero import environment, messages, errors, recorded

class InputLeafNode(object):
  '''
  A leaf input node
  '''
  def __init__(self, controller, receivers, recorded_user=None):
    '''
    :param `MachineController` controller: the controller for this node's machine.
    :param list receivers: A list of :ref:`handle`s for the nodes that should receive from this input.
    :param `RecordedUser` recorded_user: In tests, this parameter may be not null to indicate that this input
      node should playback the actions of an attached recorded_user instance.
    '''
    self._controller = controller
    self.id = str(uuid.uuid4())
    self._receivers = receivers
    self._recorded_user = recorded_user

  def receive(self, message, sender):
    for receiver in self._receivers:
      self._controller.send(receiver, message, self.handle())

  def id(self):
    return self.id

  @staticmethod
  def _init_recorded_user_from_config(recorded_user_json):
    '''
    Initialize a recorded_user instance from json.

    :param recroded_user_json: None or a `RecordedUser` serialized to json.
    :return: None or the deserialized and started `RecordedUser` instance.
    '''
    if recorded_user_json is not None:
      recorded_user = recorded.RecordedUser.from_json(recorded_user_json)
      recorded_user.start()
      return recorded_user
    else:
      return None

  @staticmethod
  def from_config(node_config, controller):
    return InputLeafNode(
        controller=controller,
        receivers=node_config['receivers'],
        recorded_user=InputLeafNode._init_recorded_user_from_config(node_config['recorded_user_json']))

  def handle(self):
    return { 'type': 'InputLeafNode', 'id': self.id, 'controller_id': self._controller.id }

  def elapse(self, ms):
    if self._recorded_user is not None:
      for t, msg in self._recorded_user.elapse_and_get_messages(ms):
        self.receive(msg, sender=None)

class OutputLeafNode(object):
  '''A leaf output node'''
  def __init__(self, controller):
    self._controller = controller
    self.id = str(uuid.uuid4())

    self._state = 0

  def get_state(self):
    return self._state

  def receive(self, message, sender):
    if message['type'] == 'increment':
      self._state += message['amount']
    else:
      raise RuntimeError("Unrecognized type {}".format(message['type']))

  @staticmethod
  def from_config(node_config, controller):
    return OutputLeafNode(controller=controller)

  def handle(self):
    return { 'type': 'OutputLeafNode', 'id': self.id, 'controller_id': self._controller.id }

  def elapse(self, ms):
    pass

class InputNode(object):
  '''
  Represents a tree of inputs
  '''
  def __init__(self, controller, receivers=None):
    self._controller = controller
    self.id = str(uuid.uuid4())
    self._receivers = [] if receivers is None else receivers

  def send_to(self, node_handle):
    self._receivers.append(node_handle)

  @staticmethod
  def from_config(node_config, controller):
    return InputNode(controller=controller)

  def handle(self):
    return { 'type': 'InputNode', 'id': self.id, 'controller_id': self._controller.id }

  def elapse(self, ms):
    pass

  def _parse_recorded_user(self, recorded_user):
    '''
    Parse a recorded user object, throw appropriate errors, and return as json.

    :param `RecordedUser` recorded_user: None, or a `RecordedUser`
    :return: Serialized json to use for this recorded user.
    '''
    if recorded_user is not None:
      if not environment.TESTING:
        raise errors.DistZeroError("recorded_users are only allowed when in testing mode")
      else:
        return recorded_user.to_json()
    else:
      return None

  def add_kid(self, machine_controller_handle, recorded_user=None):
    '''
    Add a new kid to this list of inputs.

    machine_controller_handle -- The handle of a machine controller on which to create the kid.
    return -- The handle of the newly created kid node.
    '''
    new_node_handle = self._controller.spawn_node(
        node_config=messages.add_input_leaf(
          parent=self.handle(),
          receivers=self._receivers,
          recorded_user_json=self._parse_recorded_user(recorded_user),
          ),
        on_machine=machine_controller_handle)

    for receiver in self._receivers:
      self._controller.send(receiver, messages.add_sender(new_node_handle), self.handle())

    return new_node_handle

class OutputNode(object):
  '''
  Represents a tree of outputs
  '''
  def __init__(self, controller, senders=None):
    self._controller = controller
    self.id = str(uuid.uuid4())

    self._senders = [] if senders is None else senders

  def receive(self, message, sender):
    raise RuntimeError("Not Yet Implemented")

  def receive_from(self, node_handle):
    self._senders.append(node_handle)

  @staticmethod
  def from_config(node_config, controller):
    return OutputNode(controller=controller)

  def elapse(self, ms):
    pass

  def handle(self):
    return { 'type': 'OutputNode', 'id': self.id, 'controller_id': self._controller.id }

  def add_kid(self, machine_controller_handle):
    new_node_handle = self._controller.spawn_node(
        node_config=messages.add_output_leaf(parent=self.handle(), senders=self._senders),
        on_machine=machine_controller_handle)

    for sender in self._senders:
      self._controller.send(sender, messages.add_receiver(new_node_handle))

    return new_node_handle

