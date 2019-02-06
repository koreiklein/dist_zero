import logging

from dist_zero import messages, ids, errors, network_graph, connector, settings
from dist_zero import topology_picker
from dist_zero.migration.right_configuration import ConfigurationReceiver

from ..node import Node
from . import transactions
from . import link_leaf

logger = logging.getLogger(__name__)

# FIXME(KK): Old code.  Remove this entire module in favor of dist_zero/node/link/link.py


class LinkNode(Node):
  '''
  Class representing the standard state of a LinkNode.
  '''

  SEND_INTERVAL_MS = 100
  '''The number of ms between sends to their receivers by the leaves of the tree.'''

  def __init__(
      self,
      node_id,
      left_is_data,
      right_is_data,
      parent,
      senders,
      controller,
      configure_right_parent_ids,
      is_mid_node,
      left_ids,
      receiver_ids,
      migrator_config,
      connector_type,
      connector_json,
      leaf_config,
      height,
  ):

    self.id = node_id
    self._controller = controller

    self._is_mid_node = is_mid_node

    self._connector_type = connector_type
    self._initial_connector_json = connector_json
    self._leaf_config = leaf_config

    self._added_sender_respond_tos = {}

    # Later, these will be initialized to booleans
    self._left_gap = None
    self._right_gap = None

    self.left_is_data = left_is_data
    self.right_is_data = right_is_data

    self._kids_are_adopted = False

    self.parent = parent
    self.height = height
    self.kids = {}

    self._exporters = {}

    super(LinkNode, self).__init__(logger)

    self._importers = {}

    self._initial_migrator_config = migrator_config
    self._initial_migrator = None # It will be initialized later.

    self._receivers = {receiver_id: None for receiver_id in receiver_ids} if receiver_ids is not None else None

    self.left_ids = left_ids

    self._configuration_receiver = ConfigurationReceiver(
        node=self, configure_right_parent_ids=configure_right_parent_ids, left_ids=self.left_ids)
    self._configure_right_parent_ids = configure_right_parent_ids
    self._right_configurations_are_sent = False

    # FIXME(KK): Move this into a class specific to summing.
    self._current_state = 0
    '''For when sum link nodes (of height 0) track their internal state'''

    self._transaction = None
    '''The currently transaction instance if there is one.'''
    self._blocked_messages = []
    '''
    Any messages not processed by a currently running transaction will be placed in _blocked_messages until
    the transaction ends, at which point they'll all be processed in the order received.
    '''

    for sender in senders:
      self.import_from_node(sender)

    self._connector = None

  def is_data(self):
    return False

  def checkpoint(self, before=None):
    pass

  def activate_swap(self, migration_id, kids, use_output=False, use_input=False):
    self.kids.update(kids)

  def initialize(self):
    self.logger.info(
        'Starting internal link node {link_node_id}', extra={
            'link_node_id': self.id,
        })
    if self._initial_migrator_config:
      self._initial_migrator = self.attach_migrator(self._initial_migrator_config)

    if self.parent:
      self._send_hello_parent()

    if self._is_mid_node:
      self.send(self.parent, messages.hourglass.mid_node_up(self.new_handle(self.parent['id'])))

    self._configuration_receiver.initialize()

    if self.height == 0:
      self._leaf = link_leaf.from_config(leaf_config=self._leaf_config, node=self)
      self._controller.periodically(LinkNode.SEND_INTERVAL_MS,
                                    lambda: self._maybe_send_forward_messages(LinkNode.SEND_INTERVAL_MS))
      self._send_configure_right_to_left()
    else:
      self._leaf = None
      if not self._is_mid_node:
        self._send_configure_right_to_left()

    self.linker.initialize()

  def _maybe_send_forward_messages(self, ms):
    '''Called periodically to give leaf nodes an opportunity to send their messages.'''
    if not self.deltas_only and \
        self._deltas.has_data():

      self.send_forward_messages()

    for migrator in self.migrators.values():
      migrator.elapse(ms)

    self.linker.elapse(ms)

  def _send_hello_parent(self):
    self.send(self.parent, messages.io.hello_parent(self.new_handle(self.parent['id'])))

  @staticmethod
  def from_config(node_config, controller):
    return LinkNode(
        node_id=node_config['id'],
        left_is_data=node_config['left_is_data'],
        right_is_data=node_config['right_is_data'],
        leaf_config=node_config['leaf_config'],
        configure_right_parent_ids=node_config['configure_right_parent_ids'],
        parent=node_config['parent'],
        height=node_config['height'],
        senders=node_config['senders'],
        left_ids=node_config['left_ids'],
        is_mid_node=node_config['is_mid_node'],
        receiver_ids=node_config['receiver_ids'],
        connector_type=node_config['connector_type'],
        connector_json=node_config['connector'],
        migrator_config=node_config['migrator'],
        controller=controller)

  def elapse(self, ms):
    pass

  def deliver(self, message, sequence_number, sender_id):
    self._deltas.add_message(sender_id=sender_id, sequence_number=sequence_number, message=message)

  def send_forward_messages(self, before=None):
    delta_messages = self._deltas.pop_deltas(before=before)

    if not delta_messages:
      return self.least_unused_sequence_number
    else:
      self._current_state = self._leaf.process_increment(self._current_state, delta_messages)
      return self.least_unused_sequence_number

  def export_to_node(self, receiver):
    if receiver['id'] in self._exporters:
      raise errors.InternalError(f"Already exporting to external node {receiver['id']}.")
    self._exporters[receiver['id']] = self.linker.new_exporter(receiver=receiver)

  def _send_configure_left_to_right(self):
    self.logger.info("Sending configure_new_flow_left", extra={'receiver_ids': list(self._receivers.keys())})

    receiver_to_kids = self._receiver_to_kids()

    for receiver in self._receivers.values():
      message = messages.migration.configure_new_flow_left(self.migration_id, [
          messages.migration.left_configuration(
              node=self.new_handle(receiver['id']),
              height=self.height,
              is_data=False,
              state=self._current_state,
              kids=[{
                  'handle': self.transfer_handle(self.kids[kid_id], receiver['id']),
                  'connection_limit': self.system_config['SUM_NODE_RECEIVER_LIMIT']
              } for kid_id in receiver_to_kids.get(receiver['id'], [])],
          )
      ])

      self.send(receiver, message)

  def spawn_kid(self, layer_index, node_id, senders, configure_right_parent_ids, left_ids, migrator, is_mid_node=False):
    self.kids[node_id] = None
    self._controller.spawn_node(
        messages.link.link_node_config(
            node_id=node_id,
            configure_right_parent_ids=configure_right_parent_ids,
            leaf_config=self._leaf_config,
            parent=self.new_handle(node_id),
            left_ids=left_ids,
            is_mid_node=is_mid_node,
            height=self.height - 1,
            left_is_data=self.left_is_data and layer_index == 1,
            # TODO(KK): This business about right_map here is very ugly.
            #   Think hard and try to come up with a way to avoid it.
            right_is_data=self.right_is_data and layer_index + 1 == len(self._connector.layers)
            and bool(self._connector.right_to_parent_ids[node_id]),
            senders=senders,
            receiver_ids=None,
            connector_type=self._connector_type,
            migrator=migrator))

  def _get_new_layers_edges_and_hourglasses(self, is_left, new_layers, last_edges, hourglasses):
    if new_layers or last_edges or hourglasses:
      if hourglasses and self.height > 2:
        # FIXME(KK): Ultimately, we should be able to create hourglasses for LinkNodes of height > 2,
        #   but it will be complex to orchestrate, as it involves reassigning all the receivers recursively of the
        #   rightmost kids of the nodes on the left of the hourglass.
        import ipdb
        ipdb.set_trace()
        hourglasses = []
      self.start_transaction(
          transactions.IncrementalSpawnerTransaction(
              new_layers=new_layers,
              last_edges=last_edges,
              hourglasses=hourglasses,
              connector=self._connector,
              is_left=is_left,
              node=self))

  def _update_right_configuration(self, message):
    if self._right_gap:
      # FIXME(KK): Test this, and implement by forwarding the update_right_configuration to the proper child.
      import ipdb
      ipdb.set_trace()
    else:
      if self._connector is None:
        raise errors.InternalError("self._connector must be initialized before an update_right_configuration "
                                   "can be received")

      new_layers, last_edges, hourglasses = self._connector.add_kids_to_right_configuration(
          [(message['parent_id'], kid) for kid in message['new_kids']])
      self._get_new_layers_edges_and_hourglasses(
          is_left=False, new_layers=new_layers, last_edges=last_edges, hourglasses=hourglasses)

  def _update_left_configuration(self, message):
    if self._left_gap:
      # FIXME(KK): Test this, and implement by forwarding the update_left_configuration message to the proper child.
      import ipdb
      ipdb.set_trace()
    else:
      if self._connector is None:
        raise errors.InternalError("self._connector must be initialized before an update_left_configuration "
                                   "can be received")

      self._get_new_layers_edges_and_hourglasses(
          True,
          *self._connector.add_kids_to_left_configuration([(message['parent_id'], kid) for kid in message['new_kids']]))

  @property
  def migration_id(self):
    if self._initial_migrator is not None:
      return self._initial_migrator.migration_id
    else:
      return None

  def _send_configure_right_to_left(self):
    if not self._right_configurations_are_sent:
      self.logger.info("Sending configure_new_flow_right", extra={'receiver_ids': list(self._importers.keys())})
      for importer in self._importers.values():
        self.send(
            importer.sender,
            messages.migration.configure_new_flow_right(self.migration_id, [
                messages.migration.right_configuration(
                    n_kids=None,
                    parent_handle=self.new_handle(importer.sender_id),
                    height=self.height,
                    is_data=False,
                    connection_limit=self.system_config['SUM_NODE_SENDER_LIMIT'],
                )
            ]))
      self._right_configurations_are_sent = True

  @property
  def fully_configured(self):
    return self._connector is not None

  def _set_receivers_from_right_configurations(self, right_configurations):
    if self._receivers is None:
      self._receivers = {}
      for right_config in right_configurations.values():
        parent = right_config['parent_handle']
        self.export_to_node(parent)
        self._receivers[parent['id']] = parent
    else:
      for right_config in right_configurations.values():
        parent = right_config['parent_handle']
        if parent['id'] in self._receivers:
          self.export_to_node(parent)
          self._receivers[parent['id']] = parent

  def has_left_and_right_configurations(self, left_configurations, right_configurations):
    self.logger.info("Insertion migrator has received all Left and Right configurations. Ready to spawn.")

    self._set_receivers_from_right_configurations(right_configurations)

    if self._connector is not None:
      raise errors.InternalError(
          "self._connector may not be initialized hen has_left_and_right_configurations is called.")

    if self.height == 0:
      total_state = 0
      for left_config in left_configurations.values():
        if left_config['state'] is not None:
          total_state += left_config['state']
      self._current_state = total_state
      self._send_configure_left_to_right()
    elif not self._kids_are_adopted:
      self._connector = connector.new_connector(
          self._connector_type,
          left_configurations=left_configurations,
          right_configurations=right_configurations,
          link_node=self)
      self.height = self._connector.max_height()
      self._connector.fill_in()
      self.start_transaction(transactions.SpawnerTransaction(node=self, connector=self._connector))
    else:
      # Since this node was adopting, all its eventual kids are already up and running and do not need spawning.
      # We should have received an appropriate Connector instance in the config.
      self._connector = connector.from_json(
          self._initial_connector_json,
          height=self.height,
          left_configurations=left_configurations,
          left_is_data=self.left_is_data,
          right_is_data=self.right_is_data,
          right_configurations=right_configurations,
          max_outputs=self.system_config['SUM_NODE_RECEIVER_LIMIT'],
          max_inputs=self.system_config['SUM_NODE_SENDER_LIMIT'])
      self.height = self._connector.max_height()
      self._send_configure_left_to_right()

  def set_initial_kids(self, kids):
    if self.kids:
      raise errors.InternalError("This LinkNode already has kids.")

    self.kids = kids
    self._kids_are_adopted = True

  def import_from_node(self, node, first_sequence_number=0):
    '''
    Start importing from node.

    :param node: The :ref:`handle` of a `Node` that should now be sending to self.
    :type node: :ref:`handle`
    '''
    if node['id'] in self._importers:
      raise errors.InternalError("Already importing from this node.", extra={'existing_node_id': node['id']})
    self._importers[node['id']] = self.linker.new_importer(sender=node, first_sequence_number=first_sequence_number)
    self._deltas.add_sender(node['id'])

  def new_left_configurations(self, left_configurations):
    '''For when a fully configured node gets new left_configurations'''
    if self.height > 0:
      self._get_new_layers_edges_and_hourglasses(True, *self._connector.add_left_configurations(left_configurations))

    for left_config in left_configurations:
      node = left_config['node']
      node_id = node['id']
      if node_id in self._added_sender_respond_tos:
        respond_to = self._added_sender_respond_tos.pop(node_id)
        self.send(respond_to, messages.migration.finished_adding_sender(sender_id=node_id))

  def new_right_configurations(self, right_configurations):
    '''For when a fully configured node gets new right_configurations'''
    right_parent_ids = []
    for right_config in right_configurations:
      node = right_config['parent_handle']
      right_parent_ids.append(node['id'])
      self._receivers[node['id']] = node

    if self.height > 0:
      self._connector.add_right_configurations(right_configurations)
      receiver_to_kids = self._receiver_to_kids()

    for right_parent_id in right_parent_ids:
      receiver = self._receivers[right_parent_id]
      message = messages.migration.configure_new_flow_left(None, [
          messages.migration.left_configuration(
              node=self.new_handle(receiver['id']),
              height=self.height,
              is_data=False,
              state=self._current_state,
              kids=[] if self.height == 0 else [{
                  'handle': self.transfer_handle(self.kids[kid_id], receiver['id']),
                  'connection_limit': self.system_config['SUM_NODE_RECEIVER_LIMIT']
              } for kid_id in receiver_to_kids.get(receiver['id'], [])],
          )
      ])

      self.send(receiver, message)

  def end_transaction(self):
    if self._transaction is None:
      raise errors.InternalError("No transaction in currently running.")
    else:
      self._transaction = None
      messages = self._blocked_messages
      self._blocked_messages = []
      for message, sender_id in messages:
        self.receive(message=message, sender_id=sender_id)

  def receive(self, message, sender_id):
    if self._transaction is not None:
      if self._transaction.receive(message, sender_id):
        pass
      else:
        self._blocked_messages.append((message, sender_id))
    elif self._configuration_receiver.receive(message=message, sender_id=sender_id):
      if self._is_mid_node and message['type'] == 'configure_new_flow_left':
        if all(val is not None for val in self._configuration_receiver._left_configurations.values()):
          self.send(self.parent, messages.hourglass.mid_node_ready(node_id=self.id))
    elif message['type'] == 'added_sender':
      node = message['node']
      self.import_from_node(node)
      self.send(
          node,
          messages.migration.configure_new_flow_right(None, [
              messages.migration.right_configuration(
                  n_kids=None,
                  parent_handle=self.new_handle(node['id']),
                  height=self.height,
                  is_data=False,
                  connection_limit=self.system_config['SUM_NODE_SENDER_LIMIT'],
              )
          ]))
      self._added_sender_respond_tos[node['id']] = message['respond_to']
    elif message['type'] == 'adopt':
      if self.parent is None:
        raise errors.InternalError("Root nodes may not adopt a new parent.")
      self.send(self.parent, messages.io.goodbye_parent())
      self.parent = message['new_parent']
      self._send_hello_parent()
    elif message['type'] == 'hello_parent':
      self.kids[sender_id] = message['kid']
    elif message['type'] == 'goodbye_parent':
      if sender_id not in self.kids:
        raise errors.InternalError(
            "Got a goodbye_parent from a node that is not a kid of self.", extra={'kid_id': sender_id})
      self.kids.pop(sender_id)
    elif message['type'] == 'bumped_height':
      self.start_transaction(
          transactions.BumpHeightTransaction(
              node=self, proxy=message['proxy'], kid_ids=message['kid_ids'], variant=message['variant']))
    elif message['type'] == 'update_left_configuration':
      self._update_left_configuration(message)
    elif message['type'] == 'update_right_configuration':
      self._update_right_configuration(message)
    elif message['type'] == 'start_hourglass':
      self.send_forward_messages()
      mid_node = message['mid_node']
      if self.height > 0:
        # FIXME(KK): Set kids appropriately in this case.
        #self._connector.set_right_parent_ids(kid_ids=message['receiver_ids'], parent_ids=[mid_node['id']])
        import ipdb
        ipdb.set_trace()
      self.send(
          mid_node,
          messages.migration.configure_new_flow_left(None, [
              messages.migration.left_configuration(
                  node=self.new_handle(mid_node['id']),
                  height=self.height,
                  is_data=False,
                  state=self._current_state,
                  kids=[])
          ]))
      for receiver_id in message['receiver_ids']:
        exporter = self._exporters.pop(receiver_id)
        self._receivers.pop(receiver_id)
        self.send(
            exporter.receiver,
            messages.hourglass.hourglass_swap(
                mid_node_id=mid_node['id'],
                sequence_number=exporter.internal_sequence_number,
            ))
      self.export_to_node(mid_node)
    elif message['type'] == 'hourglass_swap':
      t = transactions.StartHourglassTransaction(node=self, mid_node_id=message['mid_node_id'])
      self.start_transaction(t)
      t.receive(message=message, sender_id=sender_id)
    elif message['type'] == 'hourglass_receive_from_mid_node':
      t = transactions.StartHourglassTransaction(node=self, mid_node_id=message['mid_node']['id'])
      self.start_transaction(t)
      t.receive(message=message, sender_id=sender_id)
    else:
      super(LinkNode, self).receive(message=message, sender_id=sender_id)

  def start_transaction(self, transaction):
    if self._transaction is not None:
      raise errors.InternalError("Can't start a transaction while a separate transaction is running.")
    self._transaction = transaction
    transaction.start()

  def _receiver_to_kids(self):
    if self._connector is not None:
      result = {parent_id: [] for parent_id in self._connector._right_configurations.keys()}
      for right_node_id, receiver_ids in self._connector.right_to_parent_ids.items():
        for receiver_id in receiver_ids:
          result[receiver_id].append(right_node_id)
      return result
    else:
      return {receiver_id: [] for receiver_id in self._receivers.keys()}

  def stats(self):
    return {
        'height': self.height,
        'n_retransmissions': self.linker.n_retransmissions,
        'n_reorders': self.linker.n_reorders,
        'n_duplicates': self.linker.n_duplicates,
        'sent_messages': self.linker.least_unused_sequence_number,
        'acknowledged_messages': self.linker.least_unacknowledged_sequence_number(),
    }

  def handle_api_message(self, message):
    if message['type'] == 'get_kids':
      return {key: value for key, value in self.kids.items() if value is not None}
    elif message['type'] == 'get_senders':
      return {importer.sender_id: importer.sender for importer in self._importers.values()}
    elif message['type'] == 'get_receivers':
      return self._receivers if self._receivers is not None else {}
    else:
      return super(LinkNode, self).handle_api_message(message)
