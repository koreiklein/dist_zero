from .node import io

def node_from_config(node_config, controller):
  '''
  node_config -- A node config message
  return -- The node specified in that config.
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

    node_handle -- The handle of a node
    message -- A message for that node
    sending_node_handle -- If the message is user input, the None.  Otherwise the handle of the sending
      node.
    '''
    raise RuntimeError("Abstract Superclass")

  def spawn_node(self, node_config, on_machine):
    '''
    Start creating a new node on a linked machine.

    node_config -- A JSON serializable message that describes how to run the node.
    on_machine -- The handle of a machine_controller.
    return -- The handle of the newly created node.
    '''
    raise RuntimeError("Abstract Superclass")

  def get_node(self, handle):
    '''
    Get a node running on self by its handle
    '''
    raise RuntimeError("Abstract Superclass")

  def start_node(self, node_config):
    '''
    Start running a node on this machine.

    node_config -- A JSON serializeable message that describes how to run the node.
    return -- The newly created node.
    '''
    raise RuntimeError("Abstract Superclass")
