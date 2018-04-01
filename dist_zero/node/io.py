import uuid

from dist_zero import messages

class InputLeafNode(object):
  '''
  A leaf input node
  '''
  def __init__(self, controller, receivers):
    self._controller = controller
    self.id = str(uuid.uuid4())
    self._receivers = receivers

  def receive(self, message, sender):
    for receiver in self._receivers:
      self._controller.send(receiver, message, self.handle())

  def id(self):
    return self.id

  @staticmethod
  def from_config(node_config, controller):
    return InputLeafNode(controller=controller, receivers=node_config['receivers'])

  def handle(self):
    return { 'type': 'InputLeafNode', 'id': self.id, 'controller_id': self._controller.id }

  def elapse(self, ms):
    pass

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

  def add_kid(self, machine_controller_handle):
    '''
    Add a new kid to this list of inputs.

    machine_controller_handle -- The handle of a machine controller on which to create the kid.
    return -- The handle of the newly created kid node.
    '''
    new_node_handle = self._controller.spawn_node(
        node_config=messages.add_input_leaf(parent=self.handle(), receivers=self._receivers),
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

