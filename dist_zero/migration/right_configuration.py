from dist_zero import errors


class RightConfigurationReceiver(object):
  def __init__(self, configs=None):
    '''
    :param dict[str, object] configs: An optional preset dictionary of right_configurations.
      When passed in, this receiver will jump immediately into a ready state with those configurations.
    '''
    if configs is None:
      # Whether we're waiting for a node to set the parents.
      self._waiting_for_parents = True

      # Parents which must send parent configs before this receiver is ready
      self._expected_parents = set()
      # The parents which have sent configs already
      self._received_parents = set()
      # The children configs.  Maps each id to
      #  if we have received a config from that kid, then the config.
      #  otherwise, None.
      self.configs = {}
    else:
      self._waiting_for_parents = False
      self._expected_parents = set()
      self._received_parents = set()
      self.configs = configs

  @property
  def ready(self):
    return not self._waiting_for_parents and \
        self._expected_parents == self._received_parents and \
        all(val is not None for val in self.configs.values())

  def got_configuration(self, right_config):
    sender_id = right_config['parent_handle']['id']
    self.configs[sender_id] = right_config

  def set_parents(self, parent_ids):
    self._waiting_for_parents = False
    self._expected_parents.update(parent_ids)

  def substitute_right_parent(self, sender_id, new_parent_id):
    self._received_parents.add(sender_id)
    self._expected_parents.add(new_parent_id)

  def got_parent_configuration(self, sender_id, kid_ids):
    self._received_parents.add(sender_id)
    for kid_id in kid_ids:
      if kid_id not in self.configs:
        self.configs[kid_id] = None
