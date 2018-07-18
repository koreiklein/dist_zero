from dist_zero import errors

from .state import State


class FinishedState(State):
  STATE = State.FINISHED

  def __init__(self, migration, controller, migration_config):
    self._migration = migration
    self._controller = controller

  def initialize(self):
    pass

  def receive(self, message, sender_id):
    pass
