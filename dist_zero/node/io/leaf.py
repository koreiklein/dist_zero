import logging

import dist_zero.ids
from dist_zero import settings, messages, errors, recorded, importer, exporter
from dist_zero.node.node import Node

logger = logging.getLogger(__name__)


class LeafNode(Node):
  '''
  Represents a leaf in a tree of input or output nodes.

  The leaf nodes are meant to correspond to the code closest to the actual physical input or output devices.

  For example, if a laptop or desktop computer is interfacing with a user, it should have a leaf node running on it
  to interact with the hardware.

  Leaf nodes function
  
  * either with ``variant == 'input'`` in which case the node is reading messages from an input device.
  * or with ``variant == 'output'`` in which case the node is writing output messages to an output device.

  A single device that functions as both an input and an output should run 2 separate nodes for input and output.


  Input and output leaves are designed to be abstract with respect to where the input goes to or where the output comes
  from.  Practically, this design decision motivates two behaviors:

  * From the perspective of an input node, each input message will be sent via a single exporter.
  * From the perspective of an output node, each output message will be received via a single importer.

  so leaves are designed never to have to manage complex sets of senders or receivers.
  '''

  def __init__(self, node_id, parent, controller, variant, update_state, recorded_user=None):
    '''
    :param `MachineController` controller: the controller for this node's machine.
    :param parent: The :ref:`handle` of the parent `InternalNode` of this node.
    :type parent: :ref:`handle`
    :param str variant: 'input' or 'output'
    :param func update_state: A function.
      Call it with a function that takes the current state and returns a new state.
    :param `RecordedUser` recorded_user: In tests, this parameter may be not null to indicate that this
      node should playback the actions of an attached `RecordedUser` instance.
    '''
    self._controller = controller
    self.id = node_id
    self._exporter = None
    self._importer = None
    self._variant = variant

    self._recorded_user = recorded_user

    self._now_ms = 0

    # To look more like InternalNode
    self._kids = {}

    self._parent = parent

    # Messages received before becoming active.
    self._pre_active_messages = []

    self._update_state = update_state

    super(LeafNode, self).__init__(logger)

  @property
  def height(self):
    return 0

  def _set_input(self, node):
    if self._importer is not None:
      raise errors.InternalError("LeafNodes have only a single input node."
                                 "  Can not add a new one once an input already exists")

    self._importer = self.linker.new_importer(node)

  def _set_output(self, node):
    if self._exporter is not None:
      raise errors.InternalError("LeafNodes have only a single output node."
                                 "  Can not add a new one once an output already exists")

    self._exporter = self.linker.new_exporter(node)

  def receive(self, message, sender_id):
    if message['type'] == 'connect_node':
      if message['direction'] == 'receiver':
        self._set_output(message['node'])
      elif message['direction'] == 'sender':
        self._set_input(message['node'])
      else:
        raise errors.InternalError('Unrecognized direction "{}"'.format(message['direction']))

      # Handle the postponed messages now.
      self.logger.info("Activating Leaf Node {node_id}", extra={'node_id': self.id})
      for m in self._pre_active_messages:
        self._receive_input_action(m)

    elif message['type'] == 'input_action':
      self._receive_input_action(message)
    elif message['type'] == 'adopt':
      self.send(self._parent, messages.io.goodbye_parent())
      self._parent = message['new_parent']
      self._send_hello_parent()
    else:
      super(LeafNode, self).receive(message=message, sender_id=sender_id)

  def _receive_input_action(self, message):
    if self._variant != 'input':
      raise errors.InternalError("Only 'input' variant nodes may receive input actions")

    if self._exporter is not None:
      self.logger.debug("Forwarding input message via exporter")
      self._exporter.export_message(message=message, sequence_number=self.linker.advance_sequence_number())
    else:
      self.logger.debug("Leaf node is postponing an input_action message send since it does not yet have an exporter.")
      self._pre_active_messages.append(message)

  def deliver(self, message, sequence_number, sender_id):
    if self._variant != 'output':
      raise errors.InternalError("Only 'output' variant nodes may receive output actions")

    increment = message['number']
    self.logger.debug("Output incrementing state by {increment}", extra={'increment': increment})
    self._update_state(lambda amount: amount + increment)

    self.linker.advance_sequence_number()

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
  def from_config(node_config, controller, update_state):
    return LeafNode(
        controller=controller,
        node_id=node_config['id'],
        parent=node_config['parent'],
        update_state=update_state,
        variant=node_config['variant'],
        recorded_user=LeafNode._init_recorded_user_from_config(node_config['recorded_user_json']))

  def initialize(self):
    self._send_hello_parent()

  def _send_hello_parent(self):
    self.logger.info("leaf node sending new 'hello_parent' message to parent")
    self.send(self._parent, messages.io.hello_parent(self.new_handle(self._parent['id'])))

  def elapse(self, ms):
    self._now_ms += ms

    self.linker.elapse(ms)

    if self._recorded_user is not None:
      for t, msg in self._recorded_user.elapse_and_get_messages(ms):
        self.logger.info("Simulated user generated a message", extra={'recorded_message': msg})
        self.receive(msg, sender_id=None)

  def is_data(self):
    return True

  def handle_api_message(self, message):
    if message['type'] == 'get_output_state':
      return self._controller.get_output_state(self.id)
    elif message['type'] == 'kill_node':
      self.send(self._parent, messages.io.goodbye_parent())
      self._controller.terminate_node(self.id)
    else:
      return super(LeafNode, self).handle_api_message(message)
