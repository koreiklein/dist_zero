def computation_node_config(node_id, height, parent, senders, receivers, migrator, adoptees=None):
  '''
  A node config for creating an internal `ComputationNode` in a network of computation nodes.

  :param str node_id: The id to use for the new node.
  :param parent: The :ref:`handle` of this node's parent.
  :type parent: :ref:`handle`
  :param int height: The height of the new computation node in its tree.
  :param list senders: A list of :ref:`handle` s of sending nodes.
  :param list receivers: A list of :ref:`handle` s of receiving nodes.
  :param migrator: The migrator config for the new node if it is being started as part of a migration.
  :param list adoptees: A list of :ref:`handle` of the nodes to adopt upon spawn.
  '''
  return {
      'type': 'ComputationNode',
      'id': node_id,
      'parent': parent,
      'height': height,
      'senders': senders,
      'receivers': receivers,
      'migrator': migrator,
      'adoptees': adoptees,
  }
