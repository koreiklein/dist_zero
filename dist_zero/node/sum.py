import logging

from dist_zero import messages, errors, ids, importer, exporter
from .node import Node

logger = logging.getLogger(__name__)


class SumNode(Node):
  '''
  An internal node for summing all increments from its senders and forwarding the total to its receivers.
  '''

  SEND_INTERVAL_MS = 30
  '''The number of ms between sends to receivers.'''

  def __init__(self, node_id, senders, sender_transports, receivers, receiver_transports, parent, parent_transport,
               input_node, output_node, input_transport, output_transport, pending_sender_ids, controller):
    '''
    :param str node_id: The node id for this node.
    :param list senders: A list of :ref:`handle` of the nodes sending increments
    :param list receivers: A list of :ref:`handle` of the nodes to receive increments

    :param list sender_transports: A list of :ref:`transport` of the nodes sending increments
    :param list receiver_transports: A list of :ref:`transport` of the nodes to receive increments

    :param list pending_sender_ids: In the event that this node is starting via a migration, this is the list of
      senders that must be registered as sending duplicates in order for this node to decide that it is fully duplicated.

    :param input_node: The :ref:`handle` of the input node to this node if it has one.
    :type input_node: :ref:`handle` or None
    :param output_node: The :ref:`handle` of the output node to this node if it has one.
    :type output_node: :ref:`handle` or None

    :param parent: The :ref:`handle` of the parent `SumNode` of this node if it has one.
    :type parent: :ref:`handle` or None

    :param parent_transport: A :ref:`transport` for talking to this node's parent if it has a parent.
    :type parent_transport: :ref:`transport` or None
    '''
    self._controller = controller

    self.id = node_id

    self._parent = parent
    self._parent_transport = parent_transport

    self._input_node = input_node
    self._input_transport = input_transport
    self._output_node = output_node
    self._output_transport = output_transport

    self.migrator = None

    self._importers = {
        sender['id']: self._new_importer(sender, transport)
        for sender, transport in zip(senders, sender_transports)
    }
    self._exporters = {
        receiver['id']: self._new_exporter(receiver, transport)
        for receiver, transport in zip(receivers, receiver_transports)
    }

    self._pending_sender_ids = set(pending_sender_ids)

    # Invariants:
    #   At certain points in time, a increment message is sent to every receiver.
    #   self._unsent_time_ms is the number of elapsed milliseconds since the last such point in time
    #   self._sent_total is the total amount of increment sent to receivers as of that point in time
    #     (note: the amonut is always identical for every receiver)
    #   self._unsent_total is the total amonut of increment received since that point in time.
    #   None of the increment in self._unsent_total has been sent.
    self._sent_total = 0
    self._unsent_total = 0
    self._unsent_time_ms = 0

    super(SumNode, self).__init__(logger)

  def _new_importer(self, sender, transport):
    self.set_transport(sender, transport)
    return importer.Importer(node=self, sender=sender)

  def _new_exporter(self, receiver, transport):
    self.set_transport(receiver, transport)
    return exporter.Exporter(node=self, receiver=receiver)

  def migration_finished(self, remaining_sender_ids):
    '''
    Called when the current migrator has finished migrating.
    :param set remaining_sender_ids: The set of senders that are still sending.  All other senders can be removed.
    '''
    self.migrator = None
    old_importers = self._importers
    self._importers = {sender_id: i for sender_id, i in self._importers.items() if sender_id in remaining_sender_ids}
    self.logger.info(
        "Finished migration, cutting back number of senders from {n_old_senders} to {n_new_senders}",
        extra={
            'n_old_senders': len(old_importers),
            'n_new_senders': len(self._importers),
        })

  def handle(self):
    return {'type': 'SumNode', 'id': self.id, 'controller_id': self._controller.id}

  @staticmethod
  def from_config(node_config, controller):
    return SumNode(
        node_id=node_config['id'],
        senders=node_config['senders'],
        receivers=node_config['receivers'],
        sender_transports=node_config['sender_transports'],
        receiver_transports=node_config['receiver_transports'],
        pending_sender_ids=node_config['pending_sender_ids'],
        output_node=node_config['output_node'],
        input_node=node_config['input_node'],
        output_transport=node_config['output_transport'],
        input_transport=node_config['input_transport'],
        parent=node_config['parent'],
        parent_transport=node_config['parent_transport'],
        controller=controller)

  def _maybe_finish_middle_node_startup(self):
    '''
    For middle nodes that need to be migrated into their new state,
    check whether all the appropriate model state and messages have arrived, and if so inform the parent that
    this node is totally synced up.
    '''
    if not self._pending_sender_ids:
      self.send(self._parent, messages.migration.middle_node_is_duplicated())

  def receive(self, sender, message):
    if message['type'] == 'increment':
      self._unsent_total += message['amount']
    elif message['type'] == 'set_input':
      self._input_node = message['input_node']
      self.set_transport(self._input_node, message['transport'])
      self.send(self._input_node, messages.activate_input(self.handle(),
                                                          self.new_transport_for(self._input_node['id'])))
    elif message['type'] == 'set_output':
      self._output_node = message['output_node']
      self.set_transport(self._output_node, message['transport'])
      self.send(self._output_node,
                messages.activate_output(self.handle(), self.new_transport_for(self._output_node['id'])))
    elif message['type'] == 'added_input_leaf':
      if self._input_node is not None:
        node_id = ids.new_id()
        # TODO(KK): Find a way to avoid having to set_transport here.
        self.set_transport(message['kid'], message['transport'])
        self._controller.spawn_node(
            messages.sum.sum_node_config(
                node_id=node_id,
                senders=[],
                receivers=[self.handle()],
                sender_transports=[],
                receiver_transports=[self.new_transport_for(node_id)],
                input_node=message['kid'],
                input_transport=self.convert_transport_for(sender_id=node_id, receiver_id=message['kid']['id']),
                parent=self.handle(),
                parent_transport=self.new_transport_for(node_id)))
    elif message['type'] == 'added_output_leaf':
      if self._output_node is not None:
        node_id = ids.new_id()
        # TODO(KK): Find a way to avoid having to set_transport here.
        self.set_transport(message['kid'], message['transport'])
        self._controller.spawn_node(
            messages.sum.sum_node_config(
                node_id=node_id,
                senders=[self.handle()],
                receivers=[],
                sender_transports=[self.new_transport_for(node_id)],
                receiver_transports=[],
                output_node=message['kid'],
                output_transport=self.convert_transport_for(sender_id=node_id, receiver_id=message['kid']['id']),
                parent=self.handle(),
                parent_transport=self.new_transport_for(node_id)))
    elif message['type'] == 'finished_duplicating':
      self.migrator.finished_duplicating(sender)
    elif message['type'] == 'connect_internal':
      node = message['node']
      direction = message['direction']
      transport = message['transport']

      if direction == 'sender':
        self._importers[node['id']] = self._new_importer(sender=node, transport=transport)
      elif direction == 'receiver':
        self._exporters[node['id']] = self._new_exporter(receiver=node, transport=transport)
      else:
        raise errors.InternalError("Unrecognized direction parameter '{}'".format(direction))

      self.set_transport(node, transport)

      if self._parent is not None:
        # Doing a migration
        if sender['id'] in self._pending_sender_ids:
          self._pending_sender_ids.remove(sender['id'])
        self._maybe_finish_middle_node_startup()
    elif message['type'] == 'sum_node_started':
      self._importers[sender['id']] = self._new_importer(sender, message['transport'])
      if self.migrator:
        self.migrator.middle_node_started(sender)
    elif message['type'] == 'set_sum_total':
      # FIXME(KK): Think through how to set these parameters appropriately during startup.
      self._sent_total = message['total']
      self._unsent_total = 0
      self._unsent_time_ms = 0
      self.send(self._parent, messages.migration.middle_node_is_live())
    elif message['type'] == 'middle_node_is_duplicated':
      self.migrator.middle_node_duplicated(sender)
    elif message['type'] == 'middle_node_is_live':
      self.migrator.middle_node_live(sender)
    elif message['type'] == 'start_duplicating':
      old_receiver_id = message['old_receiver_id']
      new_receiver = message['receiver']
      self.logger.info(
          "Starting duplication phase for {cur_node_id} . {new_receiver_id} will now receive duplicates from {old_receiver_id}.",
          extra={
              'new_receiver_id': new_receiver['id'],
              'old_receiver_id': old_receiver_id,
          })
      exporter = self._exporters[old_receiver_id]
      exporter.duplicate([self._new_exporter(new_receiver, message['transport'])])
    elif message['type'] == 'finish_duplicating':
      receiver = message['receiver']
      exporter = self._exporters[receiver['id']]
      del self._exporters[receiver['id']]
      for new_exporter in exporter.finish_duplicating():
        self._exporters[new_exporter.receiver_id] = new_exporter
    else:
      self.logger.error("Unrecognized message {bad_msg}", extra={'bad_msg': message})

  def elapse(self, ms):
    self._unsent_time_ms += ms
    if self._unsent_total > 0 and self._unsent_time_ms > SumNode.SEND_INTERVAL_MS:
      self.logger.info("current n_senders = {n_senders}", extra={'n_senders': len(self._importers)})

      SENDER_LIMIT = 15
      if len(self._importers) >= SENDER_LIMIT:
        self.logger.info("Hit sender limit of {sender_limit} senders", extra={'sender_limit': SENDER_LIMIT})
        if self.migrator is None:
          self._hit_sender_limit()
      self._send_to_all()

  def _hit_sender_limit(self):
    self.migrator = SumNodeSenderSplitMigrator(self)
    self.migrator.start()

  def _send_to_all(self):
    self.logger.debug(
        "Sending new increment of {unsent_total} to all {n_receivers} receivers",
        extra={
            'unsent_total': self._unsent_total,
            'n_receivers': len(self._exporters)
        })
    for exporter in self._exporters.values():
      message = messages.sum.increment(self._unsent_total)
      exporter.export(message)
    if self._output_node and self._output_node['type'] == 'OutputLeafNode':
      message = messages.sum.increment(self._unsent_total)
      self.send(self._output_node, message)

    self._unsent_time_ms = 0
    self._sent_total += self._unsent_total
    self._unsent_total = 0

  def initialize(self):
    self.logger.info(
        'Starting sum node {sum_node_id}. input={input_node}, output={output_node}',
        extra={
            'sum_node_id': self.id,
            'input_node': self._input_node,
            'output_node': self._output_node,
        })
    if self._parent:
      self.set_transport(self._parent, self._parent_transport)
      self.send(self._parent, messages.sum.sum_node_started(transport=self.new_transport_for(self._parent['id'])))

    if self._output_node:
      self.set_transport(self._output_node, self._output_transport)
      self.send(self._output_node,
                messages.io.set_output_sender(
                    node=self.handle(), transport=self.new_transport_for(self._output_node['id'])))

    if self._input_node:
      self.set_transport(self._input_node, self._input_transport)
      self.send(self._input_node,
                messages.io.set_input_receiver(
                    node=self.handle(), transport=self.new_transport_for(self._input_node['id'])))

    for importer in self._importers.values():
      importer.initialize()

    for receiver_id, exporter in self._exporters.items():
      # FIXME(KK): Try to remove.
      if self._parent and receiver_id == self._parent['id']:
        continue

      exporter.initialize()


class SumNodeSenderSplitMigrator(object):
  '''
  For managing the way in which a sum node migrates to having new nodes between it and its senders.

  Terminology:

  The initial state before the migration:
   (large set of leftmost sender nodes) --sending_to--> current node
  The final state after the migration:
   (large set of sender nodes) --sending_to--> (smaller set of middle nodes) --sending_to--> current node

  There are thus three sets of nodes involved, the 'leftmost' sender nodes, the 'middle' nodes and the 'current' node

  The job of this migrator is to spin up an appropriate set of middle nodes in between the leftmost nodes
  and the current node.
  '''

  STATE_NEW = 'NEW'
  '''The initial state of the migrator.'''

  STATE_INITIALIZING_NEW_NODES = 'INITIALIZING_NEW_NODES'
  '''In this state, the migrator is waiting for new nodes to start running.'''

  STATE_DUPLICATING_INPUTS = 'DUPLICATING_INPUTS'
  '''In this state, all the new nodes are confirmed to have started running,
  and the migrator is waiting for the inputs to start duplicating to them.'''

  STATE_SYNCING_NEW_NODES = 'SYNCING_NEW_NODES'
  '''In this state, all the inputs have been confirmed to be duplicating their messages,
  and the migrator is waiting for the new nodes to sync up with the current node they will be replacing.'''

  # NOTE(KK): One might imagine including the state: SWAPPING_OUTPUTS for migrators that involve outputs.

  STATE_TRIMMING_INPUTS = 'TRIMMING_INPUTS'
  '''In this state, the entire downstream system is listening to the new models,
  and the migrator is waiting for the inputs to stop sending their old messages and report that they
  are sending only to the new models.'''

  STATE_FINISHED = 'FINISHED'
  '''In this state, none of the inputs are sending their original messages, and send only to the new nodes.
  The migrator's job is finished.'''

  def __init__(self, sum_node):
    '''
    :param sum_node: The underlying sum node
    :type sum_node: `SumNode`
    '''
    self.node = sum_node
    self._state = SumNodeSenderSplitMigrator.STATE_NEW

    self._middle_nodes = []
    '''Handles of the middle nodes'''

    self._totals = {}
    '''A map from middle node id to its starting total'''

    self._partition = {}
    '''A map from middle node id to a list of the importers that will be moved to it'''
    self._duplicating_input_ids = set()
    '''The set of ids of input nodes that have been told to duplicate, and have not yet confirmed that they
    are done duplicating.'''

    self._middle_node_states = {}
    '''
    A map from middle node id to its state.  One of 
      'new' -- The config was generated, but the node has not yet been heard from.
      'started' -- This node has received a first initialization message from the newly started node.
      'duplicated' -- This node is now getting duplicated messages from all the appropriate senders.
      'live' -- This node has received its model and is getting up to date messages from its input nodes.
    '''

  def _transition_state(self, from_state, to_state):
    '''Move from one state to another.'''
    if self._state != from_state:
      raise RuntimeError("Must be in state {} to transition".format(from_state))
    self.logger.info(
        "Migration transition {from_state} -> {to_state} for {cur_node_id}",
        extra={
            'from_state': from_state,
            'to_state': to_state,
        })
    self._state = to_state

  def start(self):
    '''Start the migration.'''
    self._transition_to_initializing_middle_nodes()

  def _transition_to_initializing_middle_nodes(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_NEW,
                           SumNodeSenderSplitMigrator.STATE_INITIALIZING_NEW_NODES)

    for node_config in self._new_sender_configs():
      self.controller.spawn_node(node_config=node_config)

  def _transition_to_duplicating(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_INITIALIZING_NEW_NODES,
                           SumNodeSenderSplitMigrator.STATE_DUPLICATING_INPUTS)

    for middle_node in self._middle_nodes:
      for importer in self._partition[middle_node['id']]:
        self._duplicating_input_ids.add(importer.sender_id)
        importer.duplicate_paired_exporters_to(middle_node)

  def _transition_to_syncing(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_DUPLICATING_INPUTS,
                           SumNodeSenderSplitMigrator.STATE_SYNCING_NEW_NODES)

    for middle_node in self._middle_nodes:
      self.node.send(middle_node, messages.sum.set_sum_total(self._totals[middle_node['id']]))

  def _transition_to_trimming(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_SYNCING_NEW_NODES,
                           SumNodeSenderSplitMigrator.STATE_TRIMMING_INPUTS)

    for importers in self._partition.values():
      for i in importers:
        i.finish_duplicating_paired_exporters()

  def _transition_to_finished(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_TRIMMING_INPUTS, SumNodeSenderSplitMigrator.STATE_FINISHED)

    self.node.migration_finished(set(self._partition.keys()))

  def middle_node_live(self, middle_node_handle):
    '''Called when a middle node has been confirmed to be live.'''
    self.logger.info("Marking middle node as now being live", extra={'middle_node_id': middle_node_handle['id']})
    self._middle_node_states[middle_node_handle['id']] = 'live'

    if all(state == 'live' for state in self._middle_node_states.values()):
      self._transition_to_trimming()

  def middle_node_started(self, middle_node_handle):
    '''
    Called when a middle node is confirmed to be running.

    :param middle_node_handle: The :ref:`handle` of the middle node.
    :type middle_node_handle: :ref:`handle`
    '''
    self.logger.info("Marking middle node as having started", extra={'middle_node_id': middle_node_handle['id']})
    self._middle_nodes.append(middle_node_handle)
    self._middle_node_states[middle_node_handle['id']] = 'started'
    if all(state == 'started' for state in self._middle_node_states.values()):
      self._transition_to_duplicating()

  def middle_node_duplicated(self, middle_node_handle):
    '''
    Called when a middle node has confirmed that it is receiving all the proper duplicated inputs.

    :param middle_node_handle: The :ref:`handle` of the middle node.
    :type middle_node_handle: :ref:`handle`
    '''
    self.logger.info("Marking middle node as now being duplicated", extra={'middle_node_id': middle_node_handle['id']})
    self._middle_node_states[middle_node_handle['id']] = 'duplicated'

    if all(state == 'duplicated' for state in self._middle_node_states.values()):
      self._transition_to_syncing()

  def finished_duplicating(self, sender):
    '''
    Called when a sender sum node is sending messages only according the structure at the end of migration, and not
    the duplicate messages from the beginning of the migration.

    :param sender: The :ref:`handle` of the sender node that has finished duplicating.
    :type sender: :ref:`handle`
    '''
    self._duplicating_input_ids.remove(sender['id'])
    if not self._duplicating_input_ids:
      self._transition_to_finished()

  def _new_sender_configs(self):
    '''
    Calculate which middle nodes to spawn in order to protect the current node against a high load from the leftmost
    sender nodes, and return the middle nodes' configs.

    Side effect: update self._partition to map each middle node id to the leftmost nodes that will send to it
      once we reach the terminal state.
    '''
    n_new_nodes = 2
    n_senders_per_node = len(self.node._importers) // n_new_nodes

    total = self.node._sent_total
    total_quotient, total_remainder = total % n_new_nodes, total // n_new_nodes

    importers = list(self.node._importers.values())

    for i in range(n_new_nodes):
      new_id = ids.new_id()

      # The first total_remainder nodes start with a slightly larger total
      self._totals[new_id] = total_quotient + 1 if i < total_remainder else total_quotient

      self._partition[new_id] = importers[i * n_senders_per_node:(i + 1) * n_senders_per_node]
      self._middle_node_states[new_id] = 'new'

    new_node_ids = list(self._partition.keys())

    # allocate the remaining senders as evenly as possible
    for j, extra_importer in enumerate(importers[n_new_nodes * n_senders_per_node:]):
      self._partition[new_node_ids[j]].append(extra_importer)

    return [
        messages.sum.sum_node_config(
            node_id=node_id,
            pending_sender_ids=[i.sender_id for i in importers],
            senders=[],
            sender_transports=[],
            receivers=[self.node.handle()],
            receiver_transports=[self.node.new_transport_for(node_id)],
            parent=self.node.handle(),
            parent_transport=self.node.new_transport_for(node_id),
        ) for node_id, importers in self._partition.items()
    ]

  @property
  def logger(self):
    return self.node.logger

  @property
  def controller(self):
    return self.node._controller
