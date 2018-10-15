from dist_zero import errors, messages, deltas, linker, settings

from . import migrator


class SinkMigrator(migrator.Migrator):
  NEW_FLOW_ACKNOWLEDGEMENT_INTERNAL_MS = 30
  '''
  The number of milliseconds between points in time when the `SinkMigrator` acknowledges messages in the new flow.
  '''

  def __init__(self, new_flow_senders, old_flow_sender_ids, migration, node, will_sync):
    '''
    :param migration: The :ref:`handle` of the `MigrationNode` running the migration.
    :type migration: :ref:`handle`
    :param node: The `Node` on which this migrator runs.
    :type node: `Node`
    :param bool will_sync: True iff this migrator will need to sync as part of the migration
    '''
    self._migration = migration

    self._node = node

    self._substituted_left_configs = set()

    self._waiting_for_swap = False

    self._kids_switched = {}
    self._kid_migrator_is_terminated = {}

    self._now_ms = 0

    self._swapped = False
    '''True once this migrator has swapped to the new flow.'''

    self._kids_ready_for_switch = None

    self._deltas = deltas.Deltas()

    self._new_flow_senders = {}
    self._left_configurations = {}
    self._new_sender_id_to_first_flow_sequence_number = {}
    self._new_importers = {}
    '''Sender id to the new importer'''

    self._old_sender_id_to_first_swapped_sequence_number = {sender_id: None for sender_id in old_flow_sender_ids}
    '''Maps each old sender_id to the first sequence number after it has swapped to the new flow.'''
    self._new_sender_id_to_first_swapped_sequence_number = {}
    '''Maps each new sender_id to the first sequence number after it has swapped to the new flow.'''

    if new_flow_senders is not None:
      for sender in new_flow_senders:
        self._add_sender(sender)

    self._kid_started_flows = {kid_id: False for kid_id in self._node._kids.keys()}
    self._kid_has_migrator = {kid_id: False for kid_id in self._node._kids.keys()}

    self._old_sender_id_to_first_flow_sequence_number = {sender_id: None for sender_id in old_flow_sender_ids}
    '''
    Once the flow has started, this map will assign to each old sender_id its first sequence_number
    that reflects the new flow.
    '''

    self._linker = linker.Linker(
        self._node,
        logger=self._node.logger,
        deliver=lambda message, sequence_number, sender_id: self._deltas.add_message(
          sender_id=sender_id, sequence_number=sequence_number, message=message)
        )

    self._flow_is_started = False
    '''True iff the new flow has started'''

    self._will_sync = will_sync

    self._start_syncing_message = None # A start_syncing message received by self if one exists.

    self._sync_target_to_status = None
    '''
    This will eventually be a map taking each target syncing node to either
      'pending' if the node is not yet synced
      'synced' if it is
    '''

  def _add_sender(self, sender):
    self._new_flow_senders[sender['id']] = sender
    self._left_configurations[sender['id']] = None
    self._new_sender_id_to_first_flow_sequence_number[sender['id']] = None
    self._new_importers[sender['id']] = None
    self._new_sender_id_to_first_swapped_sequence_number[sender['id']] = None
    self._deltas.add_sender(sender['id'])

  @property
  def parent(self):
    if self._node._parent is not None:
      return self._node._parent
    else:
      return self._migration

  @staticmethod
  def from_config(migrator_config, node):
    '''
    Create and return a new `SinkMigrator` from a config.

    :param dict migrator_config: Configuration dictionary for the `Migrator` instance.
    :param node: The `Node` instance on which the new `Migrator` will run.
    :type node: `Node`

    :return: The appropriate `SinkMigrator` instance.
    :rtype: `SinkMigrator`
    '''
    return SinkMigrator(
        new_flow_senders=migrator_config['new_flow_senders'],
        old_flow_sender_ids=migrator_config['old_flow_sender_ids'],
        migration=migrator_config['migration'],
        will_sync=migrator_config['will_sync'],
        node=node)

  def _maybe_complete_flow(self):
    if all(val is not None for val in self._new_sender_id_to_first_flow_sequence_number.values()) \
        and all(val is not None for val in self._old_sender_id_to_first_flow_sequence_number.values()):
      self._node.logger.info("SinkMigrator completed flow.", extra={'migration_id': self.migration_id})
      self._flow_is_started = True
      self._node.logger.info("Sink migrator has completed the flow.")
      if not self._will_sync:
        self._node.deltas_only.remove(self.migration_id)
      else:
        pass # Leave the node in deltas only mode so that it can sync

      # deltas_only mode at this point.
      sequence_number = self._node.send_forward_messages()
      self._node.send(self._migration, messages.migration.completed_flow(sequence_number=sequence_number))

  def _got_start_flow(self):
    self._node.logger.info(
        "Sending configure_new_flow_right", extra={'receiver_ids': list(self._new_flow_senders.keys())})
    for sender in self._new_flow_senders.values():
      n_kids = len(self._node._kids)
      connection_limit = n_kids
      self._node.send(sender,
                      messages.migration.configure_new_flow_right(self.migration_id, [
                          messages.migration.right_configuration(
                              n_kids=n_kids,
                              parent_handle=self._node.new_handle(sender['id']),
                              height=self._node.height,
                              connection_limit=connection_limit,
                              is_data=self._node.is_data(),
                          )
                      ]))

  def receive(self, sender_id, message):
    if message['type'] == 'start_flow':
      self._got_start_flow()
    elif message['type'] == 'attached_migrator':
      self._kid_has_migrator[sender_id] = True
      self._maybe_send_attached_migrator()
    elif message['type'] == 'started_flow':
      self._node.logger.info("Received 'started_flow'")
      self._kid_started_flows[sender_id] = True
      self._maybe_flow_is_started()
    elif message['type'] == 'substitute_left_configuration':
      if sender_id not in self._left_configurations:
        raise errors.InternalError("'substitute_left_configuration' should only be received from a node "
                                   "for which we do not expect a left_configuration")

      left_config = self._left_configurations.pop(sender_id)
      self._substituted_left_configs.add(sender_id)
      if left_config is not None:
        raise errors.InternalError("'substitute_left_configuration' should only be received from a node "
                                   "that has not sent (and will never send) a left configuration")

      self._left_configurations[message['new_node_id']] = None
    elif message['type'] == 'configure_new_flow_left':
      self._node.logger.info("Received 'configure_new_flow_left'")
      for left_configuration in message['left_configurations']:
        left_parent_id = left_configuration['node']['id']
        if left_parent_id not in self._substituted_left_configs:
          if left_parent_id not in self._new_flow_senders:
            self._add_sender(left_configuration['node'])
          self._left_configurations[left_parent_id] = left_configuration
      self._maybe_has_left_configurations()
      self._maybe_flow_is_started()
    elif message['type'] == 'replacing_flow':
      self._node.logger.info("Received 'replacing_flow'")
      self._old_sender_id_to_first_flow_sequence_number[sender_id] = message['sequence_number']
      self._maybe_complete_flow()
    elif message['type'] == 'sequence_message':
      # After the swap, sequence_messages for the migration should go directly to the node's linker.
      linker = self._node.linker if self._swapped else self._linker
      linker.receive_sequence_message(message=message['value'], sender_id=sender_id)

    elif message['type'] == 'start_syncing':
      self._node.logger.info("Received 'start_syncing'")
      self._start_syncing_message = message
      self._maybe_start_syncing()

    elif message['type'] == 'sum_total_set':
      self._node.logger.info("Received 'sum_total_set'")
      self._sync_target_to_status[sender_id] = 'synced'
      if all(status == 'synced' for status in self._sync_target_to_status.values()):
        self._node.send(self._migration, messages.migration.syncer_is_synced())

    elif message['type'] == 'prepare_for_switch':
      # Sink nodes do not go into deltas_only mode before swaps, as the old flow will not send
      # too many messages, and all messages in the new flow are collected in self._deltas.
      self._node.logger.info("Preparing for switch")
      self._kids_ready_for_switch = {}
      for kid in self._node._kids.values():
        self._kids_switched[kid['id']] = False
        self._kids_ready_for_switch[kid['id']] = False
        self._node.send(kid, messages.migration.prepare_for_switch(self.migration_id))
      self._maybe_prepared_for_switch()
    elif message['type'] == 'prepared_for_switch':
      self._node.logger.info("received 'prepared_for_switch'")
      self._kids_ready_for_switch[sender_id] = True
      self._maybe_prepared_for_switch()
    elif message['type'] == 'swapped_from_duplicate':
      self._old_sender_id_to_first_swapped_sequence_number[sender_id] = message['first_live_sequence_number']
      self._maybe_swap()
    elif message['type'] == 'swapped_to_duplicate':
      self._new_sender_id_to_first_swapped_sequence_number[sender_id] = message['first_live_sequence_number']
      self._maybe_swap()
    elif message['type'] == 'switched_flows':
      self._kids_switched[sender_id] = True
      self._maybe_swap()
    elif message['type'] == 'terminate_migrator':
      self._node.logger.info("terminating migrator")
      for kid in self._node._kids.values():
        self._kid_migrator_is_terminated[kid['id']] = False
        self._node.send(kid, messages.migration.terminate_migrator(self.migration_id))
      self._maybe_kids_are_terminated()
    elif message['type'] == 'migrator_terminated':
      self._kid_migrator_is_terminated[sender_id] = True
      self._maybe_kids_are_terminated()
    elif message['type'] == 'set_new_flow_adjacent':
      adjacent = message['adjacent']
      self._node.send(adjacent,
                      messages.migration.configure_new_flow_right(self.migration_id, [
                          messages.migration.right_configuration(
                              parent_handle=self._node.new_handle(adjacent['id']),
                              height=self._node.height,
                              is_data=self._node.is_data(),
                              n_kids=len(self._node._kids),
                              connection_limit=self._node.system_config['SUM_NODE_SENDER_LIMIT'],
                          )
                      ]))
    else:
      raise errors.InternalError('Unrecognized migration message type "{}"'.format(message['type']))

  def _maybe_kids_are_terminated(self):
    if all(self._kid_migrator_is_terminated.values()):
      if self.migration_id in self._node.deltas_only:
        self._node.deltas_only.remove(self.migration_id)
      self._node.remove_migrator(self.migration_id)
      self._node.send(self.parent, messages.migration.migrator_terminated(self.migration_id))

  def _maybe_prepared_for_switch(self):
    if all(self._kids_ready_for_switch.values()):
      self._node.logger.info("Sending 'prepared_for_switch'")
      self._node.send(self.parent, messages.migration.prepared_for_switch(self.migration_id))
      self._waiting_for_swap = True

  def _maybe_has_left_configurations(self):
    if all(val is not None for val in self._left_configurations.values()):
      my_unmatched_kids = list(self._node._kids.values())
      if not my_unmatched_kids:
        for left_config in self._left_configurations.values():
          for left_kid in left_config['kids']:
            # This kid of a the left node will not be right configured by any of self's kids.
            self._node.send(left_kid['handle'],
                            messages.migration.configure_right_parent(migration_id=self.migration_id, kid_ids=[]))
      else:
        for left_config in self._left_configurations.values():
          left_kids = left_config['kids']
          if len(left_kids) > len(my_unmatched_kids):
            raise errors.InternalError("Sink node does not have enough unmatched kids to pair with adjacents.")
          matched_kids, my_unmatched_kids = my_unmatched_kids[:len(left_kids)], my_unmatched_kids[len(left_kids):]

          for adjacent_to_kid_config, kid in zip(left_kids, matched_kids):
            adjacent_to_kid = adjacent_to_kid_config['handle']

            self._node.send(adjacent_to_kid,
                            messages.migration.configure_right_parent(
                                migration_id=self.migration_id, kid_ids=[kid['id']]))

            self._node.send(kid,
                            messages.migration.set_new_flow_adjacent(self.migration_id,
                                                                     self._node.transfer_handle(
                                                                         adjacent_to_kid, kid['id'])))
        if len(my_unmatched_kids) != 0:
          raise errors.InternalError("sink migrator has additional unmatched kids")

  def _maybe_flow_is_started(self):
    if all(val is not None for val in self._left_configurations.values()) and \
        all(self._kid_started_flows.values()):
      self._node.logger.info("Sending 'started_flow'")
      self._node.send(self.parent, messages.migration.started_flow(self.migration_id))

  def _maybe_swap(self):
    if self._waiting_for_swap and not self._swapped:
      if self._kids_switched:
        ready = all(self._kids_switched.values())
      else:
        ready = all(sn is not None for sn in self._old_sender_id_to_first_swapped_sequence_number.values()) \
            and all(sn is not None for sn in self._new_sender_id_to_first_swapped_sequence_number.values()) \
            and self._node._deltas.covers(self._old_sender_id_to_first_swapped_sequence_number) \
            and self._deltas.covers(self._new_sender_id_to_first_swapped_sequence_number)

      if ready:
        self._node.logger.info("SinkMigrator swapping flows.", extra={'migration_id': self.migration_id})
        if settings.IS_TESTING_ENV:
          self._node._TESTING_swapped_once = True
        # NOTE: Since the source nodes should have stopped sending along the old flow since their
        #   first_swapped_sequence_number, the ``before`` argument below shouldn't be necessary.
        #   Here, we pass it in anyways.  It shouldn't make a difference either way.
        self._swapped = True
        self._waiting_for_swap = False

        # Now, update the current node to listen only to the new flow.

        # Discard deltas before _new_sender_id_to_first_swapped_sequence_number, as they were exactly
        # what was represented from the old flow as of the above call to send_forward_messages.
        if self._node._height == -1:
          self._deltas.pop_deltas(
              state=self._node._leaf.state, before=self._new_sender_id_to_first_swapped_sequence_number)
        self._node.checkpoint(before=self._old_sender_id_to_first_swapped_sequence_number)

        self._node.sink_swap(
            deltas=self._deltas,
            old_sender_ids=set(self._old_sender_id_to_first_flow_sequence_number.keys()),
            new_senders=[left_config['node'] for left_config in self._left_configurations.values()],
            new_importers=self._new_importers,
            linker=self._linker,
        )

        self._node.send(self.parent, messages.migration.switched_flows(self.migration_id))

        # self._linker should no longer be used.  Null it out.
        self._linker = None

  def _maybe_start_syncing(self):
    if self._start_syncing_message is not None and \
        self._sync_target_to_status is None and \
        self._node._deltas.covers(self._old_sender_id_to_first_flow_sequence_number):

      self._node.logger.info("SinkMigrator started syncing.", extra={'migration_id': self.migration_id})
      self._node.deltas_only.remove(self.migration_id)
      self._node.send_forward_messages(before=self._old_sender_id_to_first_flow_sequence_number)
      receivers = self._start_syncing_message['receivers']
      self._sync_target_to_status = {receiver['id']: 'pending' for receiver in receivers}

      total = self._node._current_state
      total_quotient, total_remainder = total // len(receivers), total % len(receivers)

      for i, receiver in enumerate(receivers):
        # The first total_remainder receivers will get a total that is one greater than that of the other receivers.
        # This way, the totals of the receivers will add to the total of self._node
        self._node.send(receiver,
                        messages.migration.set_sum_total(
                            migration_id=self.migration_id,
                            from_node=self._node.new_handle(receiver['id']),
                            total=total_quotient + 1 if i < total_remainder else total_quotient))

  def elapse(self, ms):
    self._maybe_start_syncing()
    self._maybe_swap()
    self._now_ms += ms

    if not self._swapped and not self._waiting_for_swap:
      # Importantly, we must not elapse time on a linker while waiting for a swap, as it may
      # prematurely acknowledge
      self._linker.elapse(ms)
      if self._now_ms >= SinkMigrator.NEW_FLOW_ACKNOWLEDGEMENT_INTERNAL_MS:
        self._now_ms %= SinkMigrator.NEW_FLOW_ACKNOWLEDGEMENT_INTERNAL_MS
        self._linker.advance_sequence_number()

  @property
  def migration_id(self):
    return self._migration['id']

  def _maybe_send_attached_migrator(self):
    if all(self._kid_has_migrator.values()):
      self._node.logger.info("Sending 'attached_migrator' to parent")
      self._node.send(self._node._parent or self._migration, messages.migration.attached_migrator(self.migration_id))

  def initialize(self):
    self._node.deltas_only.add(self.migration_id)
    for kid in self._node._kids.values():
      self._kid_has_migrator[kid['id']] = False
      self._node.send(kid,
                      messages.migration.attach_migrator(
                          messages.migration.sink_migrator_config(
                              migration=self._node.transfer_handle(self._migration, kid['id']),
                              old_flow_sender_ids=self._node._graph.node_senders(kid['id']),
                              new_flow_senders=None,
                              will_sync=self._will_sync)))

    self._maybe_send_attached_migrator()
