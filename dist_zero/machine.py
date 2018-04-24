from .node import io
from .node.sum import SumNode


def node_output_file(node_id):
  '''
  :param str node_id: The id of an output leaf node
  :return: The filename of the file to which we might write the output state of that node.
  '''
  return '{}.state.json'.format(node_id)


def node_from_config(node_config, controller):
  '''
  :param JSON node_config: A node config message
  :return: The node specified in that config.
  '''
  if node_config['type'] == 'input_leaf':
    return io.InputLeafNode.from_config(node_config, controller=controller)
  elif node_config['type'] == 'output_leaf':
    return io.OutputLeafNode.from_config(node_config, controller=controller)
  elif node_config['type'] == 'start_input':
    return io.InputNode.from_config(node_config, controller=controller)
  elif node_config['type'] == 'start_output':
    return io.OutputNode.from_config(node_config, controller=controller)
  elif node_config['type'] == 'sum':
    return SumNode.from_config(node_config, controller=controller)
  else:
    raise RuntimeError("Unrecognized type {}".format(node_config['type']))


class MachineController(object):
  '''
  Instances of MachineController will correspond one-to-one with machines participating in
  the network.  Each MachineController will manage a set of Nodes, running their code,
  storing their data delivering messages to them and sending messages on their behalf
  '''

  def set_transport(self, sender, receiver, transport):
    '''
    Set the transport for messages from sender to receiver.

    :param sender: The :ref:`handle` of the sending node. It must be managed by self.
    :type sender: :ref:`handle`

    :param receiver: The :ref:`handle` of the receiving node.
    :type receiver: :ref:`handle`
    '''
    raise RuntimeError("Abstract Superclass")

  def send(self, node_handle, message, sending_node_handle, transport=None):
    '''
    Send a message to a node either managed by self, or linked to self.

    :param handle node_handle: The handle of a node
    :type node_handle: :ref:`handle`
    :param message message: A message for that node
    :type message: :ref:`message`
    :param sending_node_handle: The handle of the sending node.
    :type sending_node_handle: :ref:`handle`
    :param object transport: If the receipient is already linked, transport should be `None`.
      Otherwise, it should give transport information for setting up a link to the recipient.
    '''
    raise RuntimeError("Abstract Superclass")

  def ip_host(self):
    '''
    Get the hostname for this controller.
    '''
    raise RuntimeError("Abstract Superclass")

  def spawn_node(self, node_config, on_machine):
    '''
    Start creating a new node on a linked machine.

    :param json node_config: A JSON serializable message that describes how to run the node.
    :param on_machine: The handle of a :any:`MachineController`.
    :type on_machine: :ref:`handle`
    :return: The handle of the newly created node.
    :rtype: :ref:`handle`
    '''
    raise RuntimeError("Abstract Superclass")

  def get_node(self, handle):
    '''
    Get a node running on self by its handle

    :param handle: The handle of a node running on self
    :type handle: :ref:`handle`
    :return: The Node
    '''
    raise RuntimeError("Abstract Superclass")

  def start_node(self, node_config, node_id=None):
    '''
    Start running a node on this machine.

    :param node_config: A :ref:`message` that describes how to run the node.
    :type node_config: :ref:`message`
    :param str node_id: An optional id to use for the new node.
    :return: The newly created node.
    :rtype: `Node`
    '''
    raise RuntimeError("Abstract Superclass")
