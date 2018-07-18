import logging

import dist_zero.ids
from dist_zero import settings, messages, errors, recorded, importer, exporter
from dist_zero.node.node import Node

logger = logging.getLogger(__name__)


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

  def handle_api_message(self, message):
    if message['type'] == 'create_kid_config':
      return self.create_kid_config(name=message['new_node_name'], machine_id=message['machine_id'])
    else:
      return super(InternalNode, self).handle_api_message(message)

  def create_kid_config(self, name, machine_id):
    '''
    Generate a config for a new child leaf node, and mark it as a pending child on this parent node.

    :param str name: The name to use for the new node.

    :param str machine_id: The id of the MachineController which will run the new node.
    :return: A config for the new child node.
    :rtype: :ref:`message`
    '''
    node_id = dist_zero.ids.new_id('LeafNode_{}'.format(name))
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

  def deliver(self, message, sequence_number, sender_id):
    raise errors.InternalError("Messages should not be delivered to internal nodes.")
