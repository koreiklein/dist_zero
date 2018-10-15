import asyncio
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

  def __init__(self, node_id, parent, controller, variant, initial_state, state_updater, recorded_user=None):
    '''
    :param `MachineController` controller: the controller for this node's machine.
    :param parent: The :ref:`handle` of the parent `InternalNode` of this node.
    :type parent: :ref:`handle`
    :param str variant: 'input' or 'output'
    :param object initial_state: The initial state of the value mantained by this leaf node.
    :param str state_updater: 'sum' or 'collect'.  This parameter defines how this leaf updates its state
      upon delivery of a new message.
    :param `RecordedUser` recorded_user: In tests, this parameter may be not null to indicate that this
      node should playback the actions of an attached `RecordedUser` instance.
    '''
    self._controller = controller
    self.id = node_id
    self._exporter = None
    self._importer = None
    self._variant = variant

    self._state_updater = state_updater

    self._recorded_user = recorded_user

    self._now_ms = 0

    # To look more like InternalNode
    self._kids = {}

    self._parent = parent

    self._current_state = initial_state

    # Messages received before becoming active.
    self._pre_active_messages = []

    super(LeafNode, self).__init__(logger)

  def checkpoint(self, before=None):
    pass

  def switch_flows(self, migration_id, old_exporters, new_exporters, new_receivers):
    if self._variant == 'input':
      if len(new_receivers) != 1:
        raise errors.InternalError("switch_flows should be called on a leaf node only when there is a unique receiver.")
      self._set_output(new_receivers[0])
      self._activate()
    elif self._variant == 'output':
      raise errors.InternalError("An input LeafNode should never function as a source node in a migration.")
    else:
      raise errors.InternalError('Unrecognized variant "{}"'.format(self._variant))

  @property
  def current_state(self):
    return self._current_state

  def sink_swap(self, deltas, old_sender_ids, new_senders, new_importers, linker):
    if self._variant == 'output':
      if len(new_senders) != 1:
        raise errors.InternalError("sink_swap should be called on a leaf node only when there is a unique new sender.")
      self._set_input(new_senders[0])
    elif self._variant == 'input':
      raise errors.InternalError("An input LeafNode should never function as a sink node in a migration.")
    else:
      raise errors.InternalError('Unrecognized variant "{}"'.format(self._variant))

  @property
  def height(self):
    return 0

  def _set_input(self, node):
    if self._importer is not None:
      raise errors.InternalError("LeafNodes have only a single input node."
                                 "  Can not add a new one once an input already exists")
    if self._variant != 'output':
      raise errors.InternalError("Only output LeafNodes can set their input.")

    self._importer = self.linker.new_importer(node)

  def _set_output(self, node):
    if self._exporter is not None:
      if node['id'] != self._exporter.receiver_id:
        raise errors.InternalError("LeafNodes have only a single output node."
                                   "  Can not add a new one once an output already exists")
    else:
      if self._variant != 'input':
        raise errors.InternalError("Only input LeafNodes can set their output.")
      self._exporter = self.linker.new_exporter(node)

  def receive(self, message, sender_id):
    if message['type'] == 'input_action':
      self._receive_input_action(message)
    elif message['type'] == 'adopt':
      self.send(self._parent, messages.io.goodbye_parent())
      self._parent = message['new_parent']
      self._send_hello_parent()
    elif message['type'] == 'configure_right_parent':
      # No need to do anything here
      pass
    elif message['type'] == 'configure_new_flow_left':
      for left_config in message['left_configurations']:
        node = left_config['node']
        if left_config['state']:
          self._current_state = left_config['state']
        self._set_input(node)
    elif message['type'] == 'added_sender':
      node = message['node']
      self.send(node,
                messages.migration.configure_new_flow_right(None, [
                    messages.migration.right_configuration(
                        n_kids=None,
                        parent_handle=self.new_handle(node['id']),
                        height=-1,
                        is_data=True,
                        connection_limit=1,
                    )
                ]))
    elif message['type'] == 'configure_new_flow_right':
      if self._variant != 'input':
        raise errors.InternalError("Only 'input' leaves may be given a new right node.")
      right_configs = message['right_configurations']
      if len(right_configs) != 1:
        raise errors.InternalError("'input' leaves must be configured with a unique receiver.")
      right_config, = right_configs
      node = right_config['parent_handle']
      self._set_output(node)
      self._activate()
      self.send(node,
                messages.migration.configure_new_flow_left(
                    migration_id=None,
                    left_configurations=[
                        messages.migration.left_configuration(
                            height=-1, is_data=True, node=self.new_handle(node['id']), kids=[])
                    ]))
    else:
      super(LeafNode, self).receive(message=message, sender_id=sender_id)

  def _activate(self):
    for message in self._pre_active_messages:
      self.receive(message, sender_id=None)
    self._pre_active_messages = None

  def _receive_input_action(self, message):
    if self._variant != 'input':
      raise errors.InternalError("Only 'input' variant nodes may receive input actions")

    if self._exporter is not None:
      self.logger.debug(
          "Leaf node is forwarding input_action of {number} via exporter", extra={'number': message['number']})
      self._exporter.export_message(message=message, sequence_number=self.linker.advance_sequence_number())
    else:
      self.logger.warning(
          "Leaf node is postponing an input_action message send since it does not yet have an exporter.")
      self._pre_active_messages.append(message)

  def deliver(self, message, sequence_number, sender_id):
    if self._variant != 'output':
      raise errors.InternalError("Only 'output' variant nodes may receive output actions")

    self._update_current_state(message)

    self.linker.advance_sequence_number()

  def _update_current_state(self, message):
    if self._state_updater == 'sum':
      increment = message['number']
      self.logger.debug("Output incrementing state by {increment}", extra={'increment': increment})
      self._current_state += increment
    elif self._state_updater == 'collect':
      import ipdb
      ipdb.set_trace()
    else:
      raise errors.InternalError(f"Unrecognized state_updater {self._state_updater}")

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
    return LeafNode(
        controller=controller,
        node_id=node_config['id'],
        parent=node_config['parent'],
        state_updater=node_config['state_updater'],
        variant=node_config['variant'],
        initial_state=node_config['initial_state'],
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

  def stats(self):
    return {
        'height': -1,
        'n_retransmissions': self.linker.n_retransmissions,
        'n_reorders': self.linker.n_reorders,
        'n_duplicates': self.linker.n_duplicates,
        'sent_messages': self.linker.least_unused_sequence_number,
        'acknowledged_messages': self.linker.least_unacknowledged_sequence_number(),
    }

  def handle_api_message(self, message):
    if message['type'] == 'get_output_state':
      return self._controller.get_output_state(self.id)
    elif message['type'] == 'kill_node':
      self.send(self._parent, messages.io.goodbye_parent())
      self._controller.terminate_node(self.id)
    elif message['type'] == 'get_senders':
      if self._importer:
        if self._variant == 'input':
          return {}
        elif self._variant == 'output':
          return {self._importer.sender_id: self._importer.sender}
        else:
          raise errors.InternalError('Unrecognized variant "{}"'.format(self._variant))
      else:
        return {}
    elif message['type'] == 'get_kids':
      return self._kids
    elif message['type'] == 'get_receivers':
      if self._exporter:
        if self._variant == 'output':
          return {}
        elif self._variant == 'input':
          return {self._exporter.receiver_id: self._exporter.receiver}
        else:
          raise errors.InternalError('Unrecognized variant "{}"'.format(self._variant))
      else:
        return {}
    else:
      return super(LeafNode, self).handle_api_message(message)
