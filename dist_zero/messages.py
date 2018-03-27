'''
Functions to build standard messages.
'''

def input_leaf(parent):
  '''
  parent -- The handle of the parent node.
  '''
  return {'type': 'input_leaf', 'parent': parent}

def output_leaf(parent):
  '''
  parent -- The handle of the parent node.
  '''
  return {'type': 'output_leaf', 'parent': parent}

def increment(amount):
  '''
  amount -- An integer amount by which to increment.
  '''
  return {'type': 'increment', 'amount': amount}

