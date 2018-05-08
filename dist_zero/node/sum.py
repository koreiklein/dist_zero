import logging

from dist_zero import messages, errors, ids
from .node import Node

logger = logging.getLogger(__name__)


class SumNode(Node):
  SEND_INTERVAL_MS = 30 # Number of ms between sends to receivers.
  '''
  An internal node for summing all increments from its senders and forwarding the total to its receivers.
  '''

  def __init__(self, node_id, senders, receivers, parent, parent_transport, controller):
    '''
    :param str node_id: The node id for this node.
    :param list senders: A list of :ref:`handle` of the nodes sending increments
    :param list receivers: A list of :ref:`handle` of the nodes to receive increments
    :param parent: The :ref:`handle` of the parent `SumNode` of this node if it has one.
    :type parent: :ref:`handle` or None
    :param parent_transport: A :ref:`transport` for talking to this node's parent if it has a parent.
    :type parent_transport: :ref:`transport` or None
    '''
    self._senders = senders
    self._receivers = receivers
    self._controller = controller
    self.id = node_id

    self._parent = parent
    self._parent_transport = parent_transport

    self.migrator = None

    self._pending_sender_ids = set(sender['id'] for sender in self._senders)

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

  def migration_finished(self, remaining_sender_ids):
    '''
    Called when the current migrator has finished migrating.
    :param set remaining_sender_ids: The set of senders that are still sending.  All other senders can be removed.
    '''
    self.migrator = None
    old_senders = self._senders
    self._senders = [sender for sender in self._senders if sender['id'] in remaining_sender_ids]
    self.logger.info(
        "Finished migration, cutting back number of senders from {n_old_senders} to {n_new_senders}",
        extra={
            'n_old_senders': len(old_senders),
            'n_new_senders': len(self._senders),
        })

  def handle(self):
    return {'type': 'SumNode', 'id': self.id, 'controller_id': self._controller.id}

  @staticmethod
  def from_config(node_config, controller):
    return SumNode(
        node_id=node_config['id'],
        senders=node_config['senders'],
        receivers=node_config['receivers'],
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
      self.send(self._parent, messages.middle_node_is_duplicated())

  def receive(self, sender, message):
    if message['type'] == 'increment':
      self._unsent_total += message['amount']
    elif message['type'] == 'add_link':
      node = message['node']
      direction = message['direction']
      transport = message['transport']

      if direction == 'sender':
        self._senders.append(node)
      elif direction == 'receiver':
        self._receivers.append(node)
      else:
        raise errors.InternalError("Unrecognized direction parameter '{}'".format(direction))

      self.set_transport(node, transport)
      self.send(node, messages.added_link(self.new_transport_for(node['id'])))
    elif message['type'] == 'added_link':
      if self._parent is None:
        raise RuntimeError("parent should not be None."
                           "  This node received 'added_link' and must therefore be a migrating middle node")
      self.set_transport(sender, message['transport'])
      if sender['id'] in self._pending_sender_ids:
        self._pending_sender_ids.remove(sender['id'])
      self._maybe_finish_middle_node_startup()
    elif message['type'] == 'sum_node_started':
      self._senders.append(sender)
      self.set_transport(sender, message['transport'])
      self.migrator.middle_node_started(sender)
    elif message['type'] == 'set_sum_total':
      # FIXME(KK): Think through how to set these parameters appropriately during startup.
      self._sent_total = message['total']
      self._unsent_total = 0
      self._unsent_time_ms = 0
      self.send(self._parent, messages.middle_node_is_live())
    elif message['type'] == 'middle_node_is_duplicated':
      self.migrator.middle_node_duplicated(sender)
    elif message['type'] == 'middle_node_is_live':
      self.migrator.middle_node_live(sender)
    else:
      self.logger.error("Unrecognized message {bad_msg}", extra={'bad_msg': message})

  def elapse(self, ms):
    self._unsent_time_ms += ms
    if self._unsent_total > 0 and self._unsent_time_ms > SumNode.SEND_INTERVAL_MS:
      self.logger.info("current n_senders = {n_senders}", extra={'n_senders': len(self._senders)})

      SENDER_LIMIT = 15
      if len(self._senders) >= SENDER_LIMIT:
        self.logger.info("Hit sender limit of {sender_limit} senders", extra={'sender_limit': SENDER_LIMIT})
        if self.migrator is None:
          self._hit_sender_limit()
      self._send_to_all()

  def _hit_sender_limit(self):
    self.migrator = SumNodeSenderSplitMigrator(self)
    self.migrator.start()

  def _send_to_all(self):
    self.logger.debug(
        "Sending new increment of {unsent_total} to all receivers", extra={'unsent_total': self._unsent_total})
    for receiver in self._receivers:
      message = messages.increment(self._unsent_total)
      self.send(receiver, message)
    self._unsent_time_ms = 0
    self._sent_total += self._unsent_total
    self._unsent_total = 0

  def initialize(self):
    self.logger.info('Starting sum node {sum_node_id}', extra={'sum_node_id': self.id})
    if self._parent:
      self.set_transport(self._parent, self._parent_transport)
      self.send(self._parent, messages.sum_node_started(transport=self.new_transport_for(self._parent['id'])))


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
  STATE_INITIALIZING_MIDDLE_NODES = 'INITIALIZING_MIDDLE_NODES'
  STATE_DUPLICATING_INPUTS = 'DUPLICATING_INPUTS'
  STATE_SYNCING_MODELS = 'SYNCING_MODELS'
  STATE_FINISHED = 'FINISHED'

  def __init__(self, sum_node):
    self.node = sum_node
    self._state = SumNodeSenderSplitMigrator.STATE_NEW

    self._middle_nodes = []
    '''Handles of the middle nodes'''

    self._totals = {}
    '''A map from middle node id to its starting total'''

    self._partition = {}
    '''A map from middle node id to a list of its leftmost senders' ids'''

    self._middle_node_states = {}
    '''
    A map from middle node id to its state.  One of 
      'new' -- The config was generated, but the node has not yet been heard from.
      'started' -- This node has received a first initialization message from the newly started node.
      'duplicated' -- This node is now getting duplicated messages from all the appropriate senders.
      'live' -- This node has received its model and is getting up to date messages from its input nodes.
    '''

  def _transition_state(self, from_state, to_state):
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
    self._transition_to_initializing_middle_nodes()

  def _transition_to_initializing_middle_nodes(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_NEW,
                           SumNodeSenderSplitMigrator.STATE_INITIALIZING_MIDDLE_NODES)

    for node_config in self._new_sender_configs():
      self.controller.spawn_node(node_config=node_config)

  def _transition_to_duplicating(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_INITIALIZING_MIDDLE_NODES,
                           SumNodeSenderSplitMigrator.STATE_DUPLICATING_INPUTS)

    for middle_node in self._middle_nodes:
      for sender in self._partition[middle_node['id']]:
        self.node.send(sender,
                       messages.start_duplicating(
                           receiver=middle_node,
                           transport=self.node.convert_transport_for(sender=sender, receiver=middle_node)))

  def _transition_to_syncing(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_DUPLICATING_INPUTS,
                           SumNodeSenderSplitMigrator.STATE_SYNCING_MODELS)

    for middle_node in self._middle_nodes:
      self.node.send(middle_node, messages.set_sum_total(self._totals[middle_node['id']]))

  def _transition_to_finished(self):
    self._transition_state(SumNodeSenderSplitMigrator.STATE_SYNCING_MODELS, SumNodeSenderSplitMigrator.STATE_FINISHED)

    self.node.migration_finished(set(self._partition.keys()))

  def middle_node_live(self, middle_node_handle):
    self.logger.info("Marking middle node as now being live", extra={'middle_node_id': middle_node_handle['id']})
    self._middle_node_states[middle_node_handle['id']] = 'live'

    if all(state == 'live' for state in self._middle_node_states.values()):
      for sender in self._partition[middle_node_handle['id']]:
        self.node.send(sender, messages.finish_duplicating())

      self._transition_to_finished()

  def middle_node_duplicated(self, middle_node_handle):
    self.logger.info("Marking middle node as now being duplicated", extra={'middle_node_id': middle_node_handle['id']})
    self._middle_node_states[middle_node_handle['id']] = 'duplicated'

    if all(state == 'duplicated' for state in self._middle_node_states.values()):
      self._transition_to_syncing()

  def middle_node_started(self, middle_node_handle):
    self.logger.info("Marking middle node as having started", extra={'middle_node_id': middle_node_handle['id']})
    self._middle_nodes.append(middle_node_handle)
    self._middle_node_states[middle_node_handle['id']] = 'started'
    if all(state == 'started' for state in self._middle_node_states.values()):
      self._transition_to_duplicating()

  def _new_sender_configs(self):
    '''
    Calculate which middle nodes to spawn in order to protect the current node against a high load from the leftmost
    sender nodes, and return the middle nodes' configs.

    Side effect: update self._partition to map each middle node id to the leftmost nodes that will send to it
      once we reach the terminal state.
    '''
    n_new_nodes = 2
    n_senders_per_node = len(self.node._senders) // n_new_nodes

    total = self.node._sent_total
    total_quotient, total_remainder = total % n_new_nodes, total // n_new_nodes

    for i in range(n_new_nodes):
      new_id = ids.new_id()

      # The first total_remainder nodes start with a slightly larger total
      self._totals[new_id] = total_quotient + 1 if i < total_remainder else total_quotient

      self._partition[new_id] = self.node._senders[i * n_senders_per_node:(i + 1) * n_senders_per_node]
      self._middle_node_states[new_id] = 'new'

    new_node_ids = list(self._partition.keys())

    # allocate the remaining senders as evenly as possible
    for j, extra_sender in enumerate(self.node._senders[n_new_nodes * n_senders_per_node:]):
      self._partition[new_node_ids[j]].append(extra_sender)

    for x in self._partition.values():
      for y in x:
        if y['type'] == 'SumNode':
          import ipdb
          ipdb.set_trace()

    return [
        messages.sum_node_config(
            node_id=node_id,
            senders=senders,
            receivers=[self.node.handle()],
            parent=self.node.handle(),
            parent_transport=self.node.new_transport_for(node_id),
        ) for node_id, senders in self._partition.items()
    ]

  @property
  def logger(self):
    return self.node.logger

  @property
  def controller(self):
    return self.node._controller
