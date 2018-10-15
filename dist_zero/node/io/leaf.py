import asyncio
import logging

import dist_zero.ids
from dist_zero import settings, messages, errors, recorded, importer, exporter
from dist_zero.node.node import Node

logger = logging.getLogger(__name__)


class Leaf(object):
  '''Class for managing the state associated with a leaf node'''

  @staticmethod
  def from_config(leaf_config):
    if leaf_config['type'] == 'sum_leaf_config':
      return SumLeaf(leaf_config['initial_state'])
    elif leaf_config['type'] == 'collect_leaf_config':
      return CollectLeaf()
    else:
      raise errors.InternalError(f"Unrecognized leaf type '{leaf_config['type']}'.")

  @property
  def state(self):
    raise RuntimeError("Abstract Superclass")

  def set_state(self, state):
    '''Set the state of this leaf'''
    raise RuntimeError("Abstract Superclass")

  def update_current_state(self, message):
    '''Update the state of this leaf using the data delivered by the message'''
    raise RuntimeError("Abstract Superclass")


class SumLeaf(Leaf):
  def __init__(self, initial_state):
    self._current_state = initial_state

  @property
  def state(self):
    return self._current_state

  def set_state(self, state):
    self._current_state = state

  def update_current_state(self, message):
    increment = message['number']
    logger.debug("Output incrementing state by {increment}", extra={'increment': increment})
    self._current_state += increment


class CollectLeaf(Leaf):
  def update_current_state(self, message):
    import ipdb
    ipdb.set_trace()
