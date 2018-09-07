from dist_zero import errors, topology_picker, network_graph, ids


class Connector(object):
  def __init__(self, height, left_is_data, right_is_data, left_configurations, right_configurations, max_outputs,
               max_inputs):
    self._height = height
    self._left_is_data = left_is_data
    self._right_is_data = right_is_data
    self._left_configurations = left_configurations
    self._right_configurations = right_configurations

    self._max_outputs = max_outputs
    self._max_inputs = max_inputs

    self._name_prefix = 'InternalNode' if self._height > 0 else 'SumNode'

    # If left is not data, the left nodes are identically the picker left nodes,
    # and this variable must be `None`.
    self._left_node_to_picker_left_node = None

    self._lefts = None
    self._rights = None
    self._picker = None

    self._fill_in()

  def _initialize_picker_lefts(self):
    '''
    Called once at startup to initialize self._left_node_to_picker_left_node and return the
    list of left ids to be used by the picker.
    '''
    if self._left_is_data:
      self._left_node_to_picker_left_node = {}
      for left in self._lefts:
        adjacent_id = ids.new_id("{}_adjacent".format(self._name_prefix))
        self._left_node_to_picker_left_node[left['id']] = adjacent_id
      return list(self._left_node_to_picker_left_node.values())
    else:
      self._left_node_to_picker_left_node = None
      return [left['id'] for left in self._lefts]

  def _initialize_picker_rights(self):
    '''
    Called once at startup to initialize self._right_to_parent_handle and return the
    list of right ids to be used by the picker.
    '''
    self._right_to_parent_handle = {}
    if self._right_is_data:
      for right_config in self._right_configurations.values():
        n_kids = right_config['n_kids']
        if n_kids is None:
          raise errors.InternalError("When the right nodes are data nodes, the right configs must contain"
                                     " a not None 'n_kids' parameter.")
        for i in range(n_kids):
          adjacent_id = ids.new_id("{}_adjacent".format(self._name_prefix))
          self._right_to_parent_handle[adjacent_id] = right_config['parent_handle']
    else:
      for right_config in self._right_configurations.values():
        right_id = ids.new_id("{}_rightmost".format(self._name_prefix))
        self._right_to_parent_handle[right_id] = right_config['parent_handle']

    return list(self._right_to_parent_handle.keys())

  def _fill_in(self):
    self._lefts = [kid['handle'] for left_config in self._left_configurations.values() for kid in left_config['kids']]

    picker_lefts = self._initialize_picker_lefts()
    picker_rights = self._initialize_picker_rights()

    self._picker = topology_picker.TopologyPicker(
        graph=network_graph.NetworkGraph(),
        lefts=picker_lefts,
        rights=picker_rights,
        max_outputs=self._max_outputs,
        max_inputs=self._max_inputs,
        name_prefix=self._name_prefix)
