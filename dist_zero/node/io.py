import logging

import dist_zero.ids
from dist_zero import settings, messages, errors, recorded, importer, exporter
from .node import Node

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

  * From the perspective of an input node, each input message will be sent to a single ``adjacent`` node.
  * From the perspective of an output node, each output message will be received from a single ``adjacent`` node.

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
    self._adjacent = None
    self._exporter = None
    self._importer = None
    self._variant = variant

    self._recorded_user = recorded_user

    self._now_ms = 0

    self.parent = parent

    # Messages received before becoming active.
    self._pre_active_messages = []

    self._update_state = update_state

    super(LeafNode, self).__init__(logger)

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
    if message['type'] == 'connect_internal':
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
    elif message['type'] == 'sequence_message':
      self.linker.receive_sequence_message(message['value'], sender_id)
    else:
      raise RuntimeError("Unrecognized message type {}".format(message['type']))

  def _receive_input_action(self, message):
    if self._variant != 'input':
      raise errors.InternalError("Only 'input' variant nodes may receive input actions")

    if self._exporter is not None:
      self.logger.debug("Forwarding input message via exporter")
      self._exporter.export_message(message=message, sequence_number=self.linker.advance_sequence_number())
    else:
      self.logger.debug("Leaf node is postponing an input_action message send since it does not yet have an exporter.")
      self._pre_active_messages.append(message)

  def deliver(self, message):
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
    self.logger.info("leaf node sending 'added_leaf' message to parent")
    self.send(self.parent, messages.io.added_leaf(self.new_handle(self.parent['id'])))

  def elapse(self, ms):
    self._now_ms += ms

    self.linker.elapse(ms)

    if self._recorded_user is not None:
      for t, msg in self._recorded_user.elapse_and_get_messages(ms):
        self.logger.info("Simulated user generated a message", extra={'recorded_message': msg})
        self.receive(msg, sender_id=None)


class InternalNode(Node):
  '''
  The root of a tree of `LeafNode` instances of the same ``variant``.

  Each `InternalNode` instance is responsible for keeping track of the state of its subtree, and for growing
  or shrinking it as necessary.  In particular, when new leaves are created, `InternalNode.create_kid_config` must
  be called on the desired immediate parent to generate the node config for starting that child.
  '''

  def __init__(self, node_id, variant, adjacent, initial_state, controller):
    '''
    :param str node_id: The id to use for this node
    :param str variant: 'input' or 'output'
    :param adjacent: The :ref:`handle` of the adjacent node.  It must be provided when this internal node starts.
    :type adjacent: :ref:`handle`
    :param `MachineController` controller: The controller for this node.
    :param object initial_state: A json serializeable starting state for all leaves spawned from this node.
      This state is important for output leaves that update that state over time.
    '''
    self._controller = controller
    self._variant = variant
    self.id = node_id
    self._kids = {} # A map from kid node id to either 'pending' or 'active'
    self._initial_state = initial_state
    self._adjacent = adjacent
    super(InternalNode, self).__init__(logger)

  def get_adjacent_id(self):
    return None if self._adjacent is None else self._adjacent['id']

  def initialize(self):
    if self._variant == 'input':
      self.logger.info("internal node sending 'set_input' message to adjacent node")
      self.send(self._adjacent, messages.io.set_input(self.new_handle(self._adjacent['id'])))
    elif self._variant == 'output':
      self.logger.info("internal node sending 'set_output' message to adjacent node")
      self.send(self._adjacent, messages.io.set_output(self.new_handle(self._adjacent['id'])))
    else:
      raise errors.InternalError("Unrecognized variant {}".format(self._variant))

  def receive(self, message, sender_id):
    if message['type'] == 'sequence_message':
      self.linker.receive_sequence_message(message['value'], sender_id)
    elif message['type'] == 'added_leaf':
      self.added_leaf(message['kid'])
    else:
      self.logger.error("Unrecognized message {bad_msg}", extra={'bad_msg': message})

  @staticmethod
  def from_config(node_config, controller):
    return InternalNode(
        node_id=node_config['id'],
        controller=controller,
        adjacent=node_config['adjacent'],
        variant=node_config['variant'],
        initial_state=node_config['initial_state'])

  def elapse(self, ms):
    pass

  def create_kid_config(self, name, machine_id):
    '''
    Generate a config for a new child leaf node, and mark it as a pending child on this parent node.

    :param str name: The name to use for the new node.

    :param str machine_id: The id of the MachineController which will run the new node.
    :return: A config for the new child node.
    :rtype: :ref:`message`
    '''
    node_id = dist_zero.ids.new_id('LeafNode')
    self.logger.info(
        "Registering a new leaf node config for an internal node. name='{node_name}'",
        extra={
            'internal_node_id': self.id,
            'leaf_node_id': node_id,
            'node_name': name
        })
    self._kids[node_id] = 'pending'

    return messages.io.leaf_config(
        node_id=node_id,
        name=name,
        parent=self.new_handle(node_id),
        variant=self._variant,
        initial_state=self._initial_state,
    )

  def added_leaf(self, kid):
    '''
    :param kid: The :ref:`handle` of the leaf node that was just added.
    :type kid: :ref:`handle`
    '''
    if kid['id'] not in self._kids:
      self.logger.error(
          "added_leaf: Could not find node matching id {missing_child_node_id}",
          extra={'missing_child_node_id': kid['id']})
    elif self._adjacent is None:
      self.logger.error(
          "added_leaf: No adjacent was set in time.  Unable to forward an added_leaf message to the adjacent.")
    else:
      self._kids[kid['id']] = 'active'

      self.send(self._adjacent,
                messages.io.added_adjacent_leaf(
                    kid=self.transfer_handle(handle=kid, for_node_id=self._adjacent['id']), variant=self._variant))
