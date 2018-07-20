class Migrator(object):
  '''
  Abstract base class for the different kinds of migrators.

  Each `Migrator` instance will be initialized on a `Node` and added to that
  node's set of active migrators.

  Each `Migrator` is also associated with a unique `MigrationNode` identified
  by `Migrator.migration_id`
  '''

  @property
  def migration_id(self):
    '''
    The unique id of the relevant migration
    '''
    raise RuntimeError("Abstract Superclass")

  def receive(self, sender_id, message):
    '''
    Receive a migration message.

    :param str sender_id: The id of the node that sent the message.
    :param message:  A migration message.
    :type message: :ref:`message`
    '''
    raise RuntimeError("Abstract Superclass")

  def elapse(self, ms):
    '''
    Subclasses may override elapse.  It will be called as time passes.
    :param int ms: The number of elapsed milliseconds since the last call to elapse, or since initialization.
    '''
    pass

  def initialize(self):
    '''
    Subclasses may override initialize.  It will be called when the migrator is first created.
    '''
    pass
