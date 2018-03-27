'''
Functions to build standard messages.
'''

# Node configs

def input_node_config():
  '''
  A node config for creating a node to manage a new list of inputs.
  '''
  return {'type': 'start_input'}

def output_node_config():
  '''
  A node config for creating a node to manage a new list of outputs.
  '''
  return {'type': 'start_output'}

def sum_node_config(senders, receivers):
  '''
  A node config for creating a node to accept increments from a set of senders, sum them
  together and pass all increments to every receiver.

  :param list senders: A list of :ref:`handle`s for sending nodes.
  :param list receivers: A list of :ref:`handle`s for receiving nodes.
  '''
  return {'type': 'sum', 'senders': senders, 'receivers': receivers}

def input_leaf(parent):
  '''
  :param parent: The handle of the parent node.
  :type parent: :ref:`handle`
  '''
  return {'type': 'input_leaf', 'parent': parent}

def output_leaf(parent):
  '''
  :param parent: The handle of the parent node.
  :type parent: :ref:`handle`
  '''
  return {'type': 'output_leaf', 'parent': parent}


# Actions

def increment(amount):
  '''
  :param int amount: An integer amount by which to increment.
  '''
  return {'type': 'increment', 'amount': amount}

