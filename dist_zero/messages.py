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

def add_input_leaf(parent, receivers, recorded_user_json):
  '''
  Add a new leaf node to an InputNode list.

  :param parent: The handle of the parent node.
  :type parent: :ref:`handle`
  :param list receivers: The list of handles of receiver nodes the new node should send to.
  :param json recorded_user_json: json for a recorded user instance to initialize on the new node.
  '''
  return {'type': 'add_input_leaf', 'parent': parent, 'receivers': receivers, 'recorded_user_json': recorded_user_json}

def add_output_leaf(parent, senders):
  '''
  Add a new leaf node to an OutputNode list.

  :param parent: The handle of the parent node.
  :type parent: :ref:`handle`

  :param list senders: The list of handles of senders nodes the new node should receive from.
  :type parent: A list of :ref:`handle`
  '''
  return {'type': 'add_output_leaf', 'parent': parent, 'senders': senders}

def add_sender(sender):
  '''
  Inform a node that it now has a new sender.

  :param sender: The handle of the new sender node
  :type sender: :ref:`handle`
  '''
  return {'type': 'add_sender', 'sender': sender}

def add_receiver(receiver):
  '''
  Inform a node that it now has a new receiver.

  :param receiver: The handle of the new receiver node
  :type receiver: :ref:`handle`
  '''
  return {'type': 'add_receiver', 'receiver': receiver}

# Actions

def increment(amount):
  '''
  :param int amount: An integer amount by which to increment.
  '''
  return {'type': 'increment', 'amount': amount}


