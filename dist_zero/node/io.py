import uuid

from dist_zero import messages

class InputLeafNode(object):
  '''
  A leaf input node
  '''
  def __init__(self, controller):
    self._controller = controller
    self.id = uuid.uuid4()

  def receive(self, message, sender):
    pass

  def id(self):
    return self.id

  @staticmethod
  def from_config(node_config, controller):
    return InputLeafNode(controller=controller)

  def handle(self):
    return { 'type': 'InputLeafNode', 'id': self.id, 'controller_id': self._controller.id }

  def elapse(self, ms):
    pass

class OutputLeafNode(object):
  '''A leaf output node'''
  def __init__(self, controller):
    self._controller = controller
    self.id = uuid.uuid4()

  def get_state(self):
    return 0

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
  def __init__(self, controller):
    self._controller = controller
    self.id = uuid.uuid4()

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
    return self._controller.spawn_node(
        node_config=messages.input_leaf(self.handle()),
        on_machine=machine_controller_handle)

class OutputNode(object):
  '''
  Represents a tree of outputs
  '''
  def __init__(self, controller):
    self._controller = controller
    self.id = uuid.uuid4()

  @staticmethod
  def from_config(node_config, controller):
    return OutputNode(controller=controller)

  def elapse(self, ms):
    pass

  def handle(self):
    return { 'type': 'OutputNode', 'id': self.id, 'controller_id': self._controller.id }

  def add_kid(self, machine_controller_handle):
    return self._controller.spawn_node(
        node_config=messages.output_leaf(self.handle()),
        on_machine=machine_controller_handle)

