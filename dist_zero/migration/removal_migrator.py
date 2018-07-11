from dist_zero import errors, messages

from . import migrator


class RemovalMigrator(migrator.Migrator):
  @staticmethod
  def from_config(migrator_config, node):
    '''
    Create and return a new `RemovalMigrator` from a config.

    :param dict migrator_config: Configuration dictionary for the `Migrator` instance.
    :param node: The `Node` instance on which the new `Migrator` will run.
    :type node: `Node`

    :return: The appropriate `RemovalMigrator` instance.
    :rtype: `RemovalMigrator`
    '''
    return RemovalMigrator(migration=migrator_config['migration'], node=node)

  def __init__(self, migration, node):
    '''
    :param migration: The :ref:`handle` of the `MigrationNode` running the migration.
    :type migration: :ref:`handle`
    :param node: The `Node` on which this migrator runs.
    :type node: `Node`
    '''
    self._migration = migration
    self._node = node

  def receive(self, sender_id, message):
    # FIXME(KK): Write tests that require a complete implementation of this method, and then implement it.
    if False:
      pass
    elif message['type'] == 'terminate_migrator':
      self._node.remove_migrator(self.migration_id)
      self._node.send(self._migration, messages.migration.migrator_terminated())
    else:
      raise errors.InternalError('Unrecognized migration message type "{}"'.format(message['type']))

  @property
  def migration_id(self):
    return self._migration['id']

  def initialize(self):
    self._node.deltas_only = True
    self.send(self._migration, messages.migration.attached_migrator())
