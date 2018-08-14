def computation_node_config(node_id, depth, parent, senders, receivers, migrator):
  '''
  A node config for creating an internal `ComputationNode` in a network of computation nodes.

  :param str node_id: The id to use for the new node.
  :param parent: The :ref:`handle` of this node's parent.
  :type parent: :ref:`handle`
  :param int depth: The depth of the new computation node in its tree.
  :param list senders: A list of :ref:`handle`s of sending nodes.
  :param list receivers: A list of :ref:`handle`s of receiving nodes.
  :param migrator: The migrator config for the new node if it is being started as part of a migration.
  '''
  return {
      'type': 'ComputationNode',
      'id': node_id,
      'parent': parent,
      'depth': depth,
      'senders': senders,
      'receivers': receivers,
      'migrator': migrator
  }
