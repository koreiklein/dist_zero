'''
Functions to build standard messages.
'''

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

def increment(amount):
  '''
  :param int amount: An integer amount by which to increment.
  '''
  return {'type': 'increment', 'amount': amount}

