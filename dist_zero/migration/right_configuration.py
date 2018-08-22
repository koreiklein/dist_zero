from dist_zero import errors


class RightConfigurationReceiver(object):
  def __init__(self, has_parents):
    self.configs = {}
    self._has_parents = has_parents

    if has_parents:
      self._desired_parents = None
    else:
      self._desired_parents = set()

    self._received_parents = set()

  @property
  def ready(self):
    return self._desired_parents is not None and \
        self._desired_parents == self._received_parents and \
        all(val is not None for val in self.configs.values())

  def got_configuration(self, sender_id, right_config):
    self.configs[sender_id] = right_config

  def set_parents(self, parent_ids):
    if self._desired_parents is not None:
      import ipdb
      ipdb.set_trace()
      raise errors.InternalError("Parents for RightConfigurationReceiver have already been set.")

    desired_parents = set(parent_ids)
    if self._received_parents - desired_parents:
      raise errors.InternalError("Already received a message from a parent that is now not recognized.")

    self._desired_parents = desired_parents

  def got_parent_configuration(self, sender_id, kid_ids):
    if self._desired_parents is not None and sender_id not in self._desired_parents:
      raise errors.InternalError("Got a parent configuration for an unrecognized parent.")

    self._received_parents.add(sender_id)
    for kid_id in kid_ids:
      if kid_id not in self.configs:
        self.configs[kid_id] = None
