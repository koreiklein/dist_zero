import logging

import dist_zero.ids
from dist_zero import settings, messages, errors, recorded
from .node import Node

logger = logging.getLogger(__name__)


class InputLeafNode(Node):
  '''
  A leaf input node
  '''

  def __init__(self, node_id, parent, parent_transport, controller, receiver_config, recorded_user=None):
    '''
    :param `MachineController` controller: the controller for this node's machine.
    :param receiver_config: A configuration to spawn the unique receiver for this node.
    :type receiver_config: :ref:`message`
    :param parent: The :ref:`handle` of the parent `InputNode` of this node.
    :type parent: :ref:`handle`
    :param parent_transport: A :ref:`transport` for talking to this node's parent.
    :type parent_transport: :ref:`transport`
    :param `RecordedUser` recorded_user: In tests, this parameter may be not null to indicate that this input
      node should playback the actions of an attached `RecordedUser` instance.
    '''
    self._controller = controller
    self.id = node_id
    self._receiver_config = receiver_config
    self._receiver = None
    self._recorded_user = recorded_user

    self.parent = parent
    self._parent_transport = parent_transport

    # Messages received before becoming active.
    self._pre_active_messages = []

    super(InputLeafNode, self).__init__(logger)

  def _receive_added_link(self, sender, transport):
    '''
    Called when a new sender has been added.

    :param sender: The :ref:`handle` of the sending node.
    :type sender: :ref:`handle`
    '''
    if self._receiver is not None:
      raise errors.InternalError("InputLeafNodes have only a single receiver."
                                 "  Can not add a new link once a receiver exists")
    self.set_transport(sender, transport)
    self._receiver = sender

    self.logger.info("Activating Input Leaf Node {node_id}", extra={'node_id': self.id})
    # Process all the postponed messages now
    for m in self._pre_active_messages:
      self._receive_increment_message(m)

  def receive(self, message, sender):
    if sender is not None and message['type'] == 'added_link':
      self._receive_added_link(sender, message['transport'])
    elif message['type'] == 'increment':
      self._receive_increment_message(message)
    else:
      raise RuntimeError("Unrecognized message type {}".format(message['type']))

  def _receive_increment_message(self, message):
    if self._receiver is not None:
      self.logger.debug("Forwarding input message to receiver {receiver}", extra={'receiver': self._receiver})
      self.send(self._receiver, message)
    else:
      # Postpone message till later
      self.logger.debug("Input leaf is postponing a message send since not all receivers are active.")
      self._pre_active_messages.append(message)

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
        node_id=node_config['id'],
        parent=node_config['parent'],
        parent_transport=node_config['parent_transport'],
        receiver_config=node_config['receiver_config'],
        recorded_user=InputLeafNode._init_recorded_user_from_config(node_config['recorded_user_json']))

  def handle(self):
    return {'type': 'InputLeafNode', 'id': self.id, 'controller_id': self._controller.id}

  def initialize(self):
    self.logger.info("Input leaf node sending 'added_leaf' message to parent")
    self.set_transport(self.parent, self._parent_transport)
    self.send(self.parent, messages.added_leaf(self.handle(), transport=self.new_transport_for(self.parent['id'])))

    # The node config is deliberately missing a receiver for self.  Add it now before spawing the node.
    self._receiver_config['input_node'] = self.handle()
    self._receiver_config['input_transport'] = self.new_transport_for(self._receiver_config['id'])
    self.logger.info(
        "Spawning a receiver {adjacent_type} node adjacent to {cur_node}",
        extra={'adjacent_type': self._receiver_config['type']})
    self._controller.spawn_node(node_config=self._receiver_config)

  def elapse(self, ms):
    if self._recorded_user is not None:
      for t, msg in self._recorded_user.elapse_and_get_messages(ms):
        self.logger.info("Simulated user generated a message", extra={'recorded_message': msg})
        self.receive(msg, sender=None)


class OutputLeafNode(Node):
  '''A leaf output node'''

  def __init__(self, node_id, parent, parent_transport, controller, sender_config, update_state):
    '''
    :param str node_id: The id of this  node.
    :param parent: The :ref:`handle` of the parent node.
    :type parent: :ref:`handle`
    :param `MachineController` controller: The `MachineController` that manages this node.
    :param sender_config: A node configuration for the unique sender to this node.
    :type sender_config: :ref:`message`
    :param func update_state: A function.
      Call it with a function that takes the current state and returns a new state.
    '''
    self._controller = controller
    self.id = node_id

    self.parent = parent
    self._parent_transport = parent_transport

    self._sender = None # The unique sender to this node.
    self._sender_config = sender_config

    self._update_state = update_state

    # Messages received before becoming active.
    self._pre_active_messages = []

    super(OutputLeafNode, self).__init__(logger)

  def _receive_increment_message(self, message):
    if self._sender is not None:
      if message['type'] == 'increment':
        increment = message['amount']
        self.logger.debug("Output incrementing state by {increment}", extra={'increment': increment})
        self._update_state(lambda amount: amount + increment)
      else:
        raise RuntimeError("Unrecognized type {}".format(message['type']))
    else:
      self._pre_active_messages.append(message)

  def _receive_added_link(self, sender, transport):
    '''
    Called when a new receiver has been added.

    :param receiver: The :ref:`handle` of the receiving node.
    :type receiver: :ref:`handle`
    '''
    if self._sender is not None:
      raise errors.InternalError("OutputLeafNodes have only a single sender."
                                 "  Can not add a new link once a sender exists")
    self.set_transport(sender, transport)
    self._sender = sender
    self.logger.info("Activating Output Leaf Node {node_id}", extra={'node_id': self.id})
    # Process all the postponed messages now
    for m in self._pre_active_messages:
      self._receive_increment_message(m)

  def receive(self, message, sender):
    if sender is not None and message['type'] == 'added_link':
      self._receive_added_link(sender, message['transport'])
    elif message['type'] == 'increment':
      self._receive_increment_message(message)
    else:
      raise RuntimeError("Unrecognized message type {}".format(message['type']))

  @staticmethod
  def from_config(node_config, controller, update_state):
    return OutputLeafNode(
        node_id=node_config['id'],
        parent=node_config['parent'],
        parent_transport=node_config['parent_transport'],
        controller=controller,
        sender_config=node_config['sender_config'],
        update_state=update_state,
    )

  def handle(self):
    return {'type': 'OutputLeafNode', 'id': self.id, 'controller_id': self._controller.id}

  def elapse(self, ms):
    pass

  def initialize(self):
    self.logger.info("Output leaf node sending 'added_leaf' message to parent")
    self.set_transport(self.parent, self._parent_transport)
    self.send(self.parent, messages.added_leaf(self.handle(), transport=self.new_transport_for(self.parent['id'])))

    # The node config is deliberately missing a receiver for self.  Add it now before spawing the node.
    self._sender_config['output_node'] = self.handle()
    self._sender_config['output_transport'] = self.new_transport_for(self._sender_config['id'])
    self.logger.info(
        "Spawning a sender {adjacent_type} node adjacent to {cur_node}",
        extra={'adjacent_type': self._sender_config['type']})
    self._controller.spawn_node(node_config=self._sender_config)


class InputNode(Node):
  '''
  Represents a tree of inputs
  '''

  def __init__(self, node_id, controller, receivers=None):
    '''
    :param str node_id: The id to use for this node
    :param `MachineController` controller: The controller for this node.
    :param template: The template node config to use as a base for spawning nodes adjacent to new leaves.
    :param list receivers: A list of nodes that receive from this node.
    '''
    self._controller = controller
    self.id = node_id
    self._template = None
    self._kids = {} # A map from kid node id to either 'pending' or 'active'
    self._receivers = [] if receivers is None else receivers
    super(InputNode, self).__init__(logger)

  def receive(self, message, sender):
    if message['type'] == 'start_sending_to':
      self.start_sending_to(message['node'], transport=message['transport'], template=message['template'])
    elif message['type'] == 'added_leaf':
      self.added_leaf(message['kid'], message['transport'])
    else:
      self.logger.error("Unrecognized message {bad_msg}", extra={'bad_msg': message})

  def start_sending_to(self, node_handle, transport, template):
    self._template = template
    self.set_transport(node_handle, transport)
    self._receivers.append(node_handle)

  @staticmethod
  def from_config(node_config, controller):
    return InputNode(node_id=node_config['id'], controller=controller)

  def handle(self):
    return {'type': 'InputNode', 'id': self.id, 'controller_id': self._controller.id}

  def elapse(self, ms):
    pass

  def _serialize_recorded_user(self, recorded_user):
    '''
    Parse a recorded user object, throw appropriate errors, and return as json.

    :param `RecordedUser` recorded_user: None, or a `RecordedUser`
    :return: Serialized json to use for this recorded user.
    '''
    if recorded_user is not None:
      if not settings.TESTING:
        raise errors.DistZeroError("recorded_users are only allowed when in testing mode.  Current mode is {}".format(
            settings.DIST_ZERO_ENV))
      else:
        return recorded_user.to_json()
    else:
      return None

  def create_kid_config(self, name, machine_controller_handle):
    '''
    Generate a config for a new child leaf node, and mark it as pending.

    :param str name: The name to use for the new node.

    :param machine_controller_handle: The :ref:`handle` of the MachineController which will run the new node.
    :type machine_controller_handle: :ref:`handle`
    :return: A config for the new child node.
    :rtype: :ref:`message`
    '''
    node_id = dist_zero.ids.new_id()
    self.logger.info(
        "Registering a new leaf input node config for an internal node. name='{node_name}'",
        extra={
            'internal_node': self.handle(),
            'leaf_node_id': node_id,
            'node_name': name
        })
    self._kids[node_id] = 'pending'

    return messages.input_leaf_config(
        node_id=node_id,
        name=name,
        parent=self.handle(),
        parent_transport=self.new_transport_for(node_id),
        receiver_config=self._new_adjacent_node_config(),
    )

  def _new_adjacent_node_config(self):
    if self._template is None:
      return None
    else:
      receiver_config = dict(self._template)
      adjacent_node_id = dist_zero.ids.new_id()
      receiver_config['id'] = adjacent_node_id
      receiver_config['receivers'] = self._receivers
      receiver_config['receiver_transports'] = [
          self.convert_transport_for(sender_id=adjacent_node_id, receiver_id=receiver['id'])
          for receiver in self._receivers
      ]
      return receiver_config

  def added_leaf(self, kid, transport):
    '''
    :param kid: The :ref:`handle` of the leaf input node that was just added.
    :type kid: :ref:`handle`
    '''
    if kid['id'] not in self._kids:
      self.logger.error(
          "added_leaf: Could not find node matching id {missing_child_node_id}",
          extra={'missing_child_node_id': kid['id']})
    else:
      self._kids[kid['id']] = 'active'
      self.set_transport(kid, transport)


class OutputNode(Node):
  '''
  Represents a tree of outputs
  '''

  def __init__(self, node_id, controller, initial_state, senders=None):
    '''
    :param str node_id: The id to use for this node
    :param `MachineController` controller: The controller for this node.
    :param object initial_state: A json serializeable starting state for all output leaves spawned from this node.
    :param template: The template node config to use as a base for spawning nodes adjacent to new leaves.
    :param list senders: A list of nodes that send to this node.
    '''
    self._controller = controller
    self.id = node_id

    self._template = None

    self._initial_state = initial_state

    self._senders = [] if senders is None else senders

    self._kids = {} # A map from kid node id to either 'pending' or 'active'
    super(OutputNode, self).__init__(logger)

  def receive(self, message, sender):
    if message['type'] == 'start_receiving_from':
      self.receive_from(message['node'], transport=message['transport'], template=message['template'])
    elif message['type'] == 'added_leaf':
      self.added_leaf(message['kid'], message['transport'])
    else:
      self.logger.error("Unrecognized message {bad_msg}", extra={'bad_msg': message})

  def receive_from(self, node_handle, transport, template):
    self._template = template
    self.set_transport(node_handle, transport)
    self._senders.append(node_handle)

  @staticmethod
  def from_config(node_config, controller):
    return OutputNode(node_id=node_config['id'], initial_state=node_config['initial_state'], controller=controller)

  def elapse(self, ms):
    pass

  def handle(self):
    return {'type': 'OutputNode', 'id': self.id, 'controller_id': self._controller.id}

  def create_kid_config(self, name, machine_controller_handle):
    '''
    Generate a config for a new child leaf node, and mark it as pending.

    :param str name: The name to use for the new node.

    :param machine_controller_handle: The :ref:`handle` of the MachineController which will run the new node.
    :type machine_controller_handle: :ref:`handle`
    :return: A config for the new child node.
    :rtype: :ref:`message`
    '''
    node_id = dist_zero.ids.new_id()
    self.logger.info(
        "Registering a new leaf output node config with an internal node. name='{node_name}'",
        extra={
            'internal_node': self.handle(),
            'leaf_node_id': node_id,
            'node_name': name
        })
    self._kids[node_id] = 'pending'
    return messages.output_leaf_config(
        node_id=node_id,
        name=name,
        parent=self.handle(),
        parent_transport=self.new_transport_for(node_id),
        sender_config=self._new_adjacent_node_config(),
        initial_state=self._initial_state,
    )

  def _new_adjacent_node_config(self):
    if self._template is None:
      return None
    else:
      adjacent_node_id = dist_zero.ids.new_id()
      sender_config = dict(self._template)
      sender_config['id'] = adjacent_node_id
      sender_config['senders'] = self._senders
      sender_config['sender_transports'] = [
          self.convert_transport_for(sender_id=adjacent_node_id, receiver_id=sender['id']) for sender in self._senders
      ]
      return sender_config

  def added_leaf(self, kid, transport):
    '''
    :param kid: The :ref:`handle` of the leaf output node that was just added.
    :type kid: :ref:`handle`
    '''
    if kid['id'] not in self._kids:
      self.logger.error(
          "added_leaf: Could not find node matching id {missing_child_node_id}",
          extra={'missing_child_node_id': kid['id']})
    else:
      self._kids[kid['id']] = 'active'
      self.set_transport(kid, transport)
