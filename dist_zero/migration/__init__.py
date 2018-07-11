from dist_zero import errors

from . import source_migrator, sink_migrator, removal_migrator, insertion_migrator


def migrator_from_config(migrator_config, node):
  '''
  Create and return a new `Migrator` subclass from a migrator config.

  :param dict migrator_config: Configuration dictionary for the `Migrator` instance.
  :param node: The `Node` instance on which the new `Migrator` will run.
  :type node: `Node`

  :return: The appropriate `Migrator` instance.
  :rtype: `Migrator`
  '''
  if migrator_config['type'] == 'source_migrator':
    return source_migrator.SourceMigrator.from_config(migrator_config=migrator_config, node=node)
  elif migrator_config['type'] == 'sink_migrator':
    return sink_migrator.SinkMigrator.from_config(migrator_config=migrator_config, node=node)
  elif migrator_config['type'] == 'removal_migrator':
    return removal_migrator.RemovalMigrator.from_config(migrator_config=migrator_config, node=node)
  elif migrator_config['type'] == 'insertion_migrator':
    return insertion_migrator.InsertionMigrator.from_config(migrator_config=migrator_config, node=node)
  else:
    raise errors.InternalError('Unrecognized migrator type "{}"'.format(migrator_config['type']))
