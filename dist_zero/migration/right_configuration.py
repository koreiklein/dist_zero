from dist_zero import errors


class RightConfigurationReceiver(object):
  def __init__(self, wait_for_parents):
    self.configs = {}

    self._waiting_for_parents = wait_for_parents

    self._desired_parents = set()

    self._received_parents = set()

  @property
  def ready(self):
    return not self._waiting_for_parents and \
        self._desired_parents == self._received_parents and \
        all(val is not None for val in self.configs.values())

  def got_configuration(self, sender_id, right_config):
    self.configs[sender_id] = right_config

  def set_parents(self, parent_ids):
    self._waiting_for_parents = False
    self._desired_parents.update(parent_ids)

  def got_parent_configuration(self, sender_id, parent_ids, kid_ids):
    self._received_parents.add(sender_id)
    self._desired_parents.update(parent_ids)
    for kid_id in kid_ids:
      if kid_id not in self.configs:
        self.configs[kid_id] = None
