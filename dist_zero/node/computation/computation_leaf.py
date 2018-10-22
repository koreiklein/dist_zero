'''
Classes to perform work done only on leaf computation nodes.
'''

from dist_zero import errors, messages


class ComputationLeaf(object):
  '''
  Abstract base class for all the ways to perform work at a leaf node in a computation network.
  '''

  def process_increment(self, state, delta_messages):
    '''
    Process incomming messages, and use them to update the state.
    '''
    raise RuntimeError("Abstract Superclass")


class SumComputationLeaf(ComputationLeaf):
  def __init__(self, node):
    self._node = node

  def process_increment(self, state, delta_messages):
    sequence_number = self._node.linker.advance_sequence_number()

    increment = 0
    for delta_message in delta_messages:
      if delta_message['type'] == 'increment':
        updated = True
        increment += delta_message['amount']
      elif delta_message['type'] == 'input_action':
        updated = True
        increment += delta_message['number']
      else:
        raise errors.InternalError('Unrecognized message type "{}"'.format(delta_message['type']))

    self._send_increment(increment, sequence_number)
    return state + increment

  def _send_increment(self, increment, sequence_number):
    self._node.logger.debug("Sending new increment of {increment} from {cur_node_id}.", extra={'increment': increment})
    if self._node.right_is_data:
      exporter, = self._node._exporters.values()
      exporter.export_message(
          message=messages.io.output_action(increment),
          sequence_number=sequence_number,
      )
    else:
      for exporter in self._node._exporters.values():
        exporter.export_message(
            message=messages.sum.increment(amount=increment),
            sequence_number=sequence_number,
        )


class ForwardToAnyComputationLeaf(ComputationLeaf):
  def __init__(self, node):
    self._node = node

  def process_increment(self, state, delta_messages):
    sequence_number = self._node.linker.advance_sequence_number()
    if self._node._exporters:
      for message in delta_messages:
        exporter = self._node._controller.random.choice(list(self._node._exporters.values()))
        exporter.export_message(message=message, sequence_number=sequence_number)


def from_config(leaf_config, node):
  if leaf_config['type'] == 'sum_computation_leaf':
    return SumComputationLeaf(node)
  elif leaf_config['type'] == 'forward_to_any_computation_leaf':
    return ForwardToAnyComputationLeaf(node)
  else:
    raise errors.InternalError(f"Unrecognized computation leaf type '{leaf_config['type']}'")
