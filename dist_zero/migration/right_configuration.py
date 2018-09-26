from dist_zero import errors, messages


class ConfigurationReceiver(object):
  def __init__(self, node, configure_right_parent_ids, left_ids):
    self._node = node
    self._right_config_receiver = RightConfigurationReceiver()
    self._right_config_receiver.set_parents(configure_right_parent_ids)
    self._left_configurations = {nid: None for nid in left_ids}

    self._fully_configured = False

  def initialize(self):
    self._maybe_has_left_and_right_configurations()

  def _maybe_has_left_and_right_configurations(self):
    if not self._fully_configured and \
        self._right_config_receiver.ready and \
        all(val is not None for val in self._left_configurations.values()):
      self._fully_configured = True
      self._node.has_left_and_right_configurations(
          left_configurations=self._left_configurations,
          right_configurations=self._right_config_receiver.configs,
      )

  @property
  def logger(self):
    return self._node.logger

  def receive(self, message, sender_id):
    if self._fully_configured:
      if message['type'] == 'configure_right_parent':
        # This message should not be important.  configure_new_flow_right messages should arrive later and be
        # processed normally
        pass
      elif message['type'] == 'substitute_right_parent':
        # FIXME(KK): Implement this
        import ipdb
        ipdb.set_trace()
      elif message['type'] == 'configure_new_flow_left':
        for left_config in message['left_configurations']:
          self._left_configurations[left_config['node']['id']] = left_config
        self._node.new_left_configurations(message['left_configurations'])
      elif message['type'] == 'add_left_configuration':
        left_config = message['left_configuration']
        self._left_configurations[left_config['node']['id']] = left_config
        self._node.add_left_configuration(left_config)
      elif message['type'] == 'configure_new_flow_right':
        for right_config in message['right_configurations']:
          self._right_config_receiver.got_configuration(right_config)
        self._node.new_right_configurations(message['right_configurations'])
      else:
        return False
    elif message['type'] == 'configure_right_parent':
      self._right_config_receiver.got_parent_configuration(sender_id, kid_ids=message['kid_ids'])
      self._maybe_has_left_and_right_configurations()
    elif message['type'] == 'connect_to_receiver':
      self._node.send(message['receiver'],
                      messages.migration.add_left_configuration(
                          self._node.generate_new_left_configuration(message['receiver'])))
    elif message['type'] == 'substitute_right_parent':
      self._right_config_receiver.substitute_right_parent(sender_id, new_parent_id=message['new_parent_id'])
      self._maybe_has_left_and_right_configurations()
    elif message['type'] == 'configure_new_flow_left':
      self.logger.info("Received 'configure_new_flow_left'", extra={'sender_id': sender_id})
      for left_configuration in message['left_configurations']:
        self._left_configurations[left_configuration['node']['id']] = left_configuration
      self._maybe_has_left_and_right_configurations()
    elif message['type'] == 'configure_new_flow_right':
      self.logger.info("Received 'configure_new_flow_right'", extra={'sender_id': sender_id})

      for right_configuration in message['right_configurations']:
        self._right_config_receiver.got_configuration(right_configuration)

      self._maybe_has_left_and_right_configurations()
    else:
      return False
    return True


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

  def max_height(self):
    return max(config['height'] for config in self.configs.values())

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
