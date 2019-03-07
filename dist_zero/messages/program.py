def program_node_config(node_id):
  '''
  Node config for spawning a new `ProgramNode`

  :param str node_id: The id of the new node.
  :param list datasets: List of `dataset_config` objects.
  :param list links: List of `link_config` config objects.
  '''
  return {
      'type': 'ProgramNode',
      'id': node_id,
  }


def dataset_config(node_id, dataset_program_config):
  '''
  Configuration information for a distributed dataset.

  :param str node_id: The id to use for the root `DataNode` for the dataset.
  :param dataset_program_config: A config defining the dataset program this dataset will run.
  '''
  return {
      'type': 'dataset_program_config',
      'id': node_id,
      'dataset_program_config': dataset_program_config,
  }


def link_config(node_id, link_key, src_dataset_id, tgt_dataset_id):
  '''
  Configuration information for a link between two distributed datasets.

  :param str node_id: The id to use for the root `LinkNode` for the link.
  :param str link_key: The link key for this link.
  :param str src_dataset_id: The id of the root `DataNode` of the source dataset of this link.
  :param str tgt_dataset_id: The id of the root `DataNode` of the target dataset of this link.
  '''
  return {
      'type': 'link_config',
      'id': node_id,
      'link_key': link_key,
      'src_dataset_id': src_dataset_id,
      'tgt_dataset_id': tgt_dataset_id,
  }
