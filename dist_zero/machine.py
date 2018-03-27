from .node import io

def node_from_config(node_config, controller):
  '''
  :param json node_config: A node config message
  :return: The node specified in that config.
  '''
  if node_config['type'] == 'input_leaf':
    return io.InputLeafNode.from_config(node_config, controller)
  elif node_config['type'] == 'output_leaf':
    return io.OutputLeafNode.from_config(node_config, controller)
  else:
    raise RuntimeError("Unrecognized type {}".format(node_config['type']))


class MachineController(object):
  '''
  Instances of MachineController will correspond one-to-one with machines participating in
  the network.  Each MachineController will manage a set of Nodes, running their code,
  storing their data delivering messages to them and sending messages on their behalf
  '''
  def send(self, node_handle, message, sending_node_handle=None):
    '''
    Send a message to a node either managed by self, or linked to self.

    :param handle node_handle: The handle of a node
    :type node_handle: :ref:`handle`
    :param message message: A message for that node
    :type message: :ref:`message`
    :param sending_node_handle: If the message is user input, then `None`.
        Otherwise the handle of the sending node.
    :type sending_node_handle: :ref:`handle`
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

  def start_node(self, node_config):
    '''
    Start running a node on this machine.

    node_config -- A JSON serializeable message that describes how to run the node.
    return -- The newly created node.
    '''
    raise RuntimeError("Abstract Superclass")
