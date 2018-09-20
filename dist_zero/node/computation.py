import logging

from dist_zero import messages, ids, errors, network_graph, connector
from dist_zero import topology_picker
from dist_zero.connector import proxy_spawner
from dist_zero.migration.right_configuration import ConfigurationReceiver

from .node import Node

logger = logging.getLogger(__name__)


class ComputationNode(Node):
  def __init__(self, node_id, configure_right_parent_ids, left_is_data, right_is_data, parent, height, senders,
               left_ids, receiver_ids, migrator_config, connector_json, adoptees, controller):
    self.id = node_id
    self._controller = controller

    self._initial_connector_json = connector_json

    # Later, these will be initialized to booleans
    self._left_gap = None
    self._right_gap = None

    self.left_is_data = left_is_data
    self.right_is_data = right_is_data

    self.parent = parent
    self.height = height
    self.kids = {}

    self._adoptees = adoptees
    self._pending_adoptees = None

    self._proxy_spawner = None

    self._exporters = {}

    self._senders = {sender['id']: sender for sender in senders}

    self._initial_migrator_config = migrator_config
    self._initial_migrator = None # It will be initialized later.

    self._receivers = {receiver_id: None for receiver_id in receiver_ids} if receiver_ids is not None else None

    self.left_ids = left_ids

    self._configuration_receiver = ConfigurationReceiver(
        node=self, configure_right_parent_ids=configure_right_parent_ids, left_ids=self.left_ids)
    self._configure_right_parent_ids = configure_right_parent_ids
    self._right_configurations_are_sent = False
    self._left_configurations_are_sent = False

    super(ComputationNode, self).__init__(logger)

    for sender_id in self._senders.keys():
      self._deltas.add_sender(sender_id)

    self._connector = None
    self._spawner = None
    self._incremental_spawner = None

  def is_data(self):
    return False

  def checkpoint(self, before=None):
    pass

  def activate_swap(self, migration_id, kids, use_output=False, use_input=False):
    self.kids.update(kids)

  def initialize(self):
    self.logger.info(
        'Starting internal computation node {computation_node_id}', extra={
            'computation_node_id': self.id,
        })
    if self._adoptees is not None:
      self._pending_adoptees = {adoptee['id'] for adoptee in self._adoptees}
      for adoptee in self._adoptees:
        self.send(adoptee, messages.io.adopt(self.new_handle(adoptee['id'])))

    if self._initial_migrator_config:
      self._initial_migrator = self.attach_migrator(self._initial_migrator_config)

    if self._adoptees is None and self.parent and not self.kids:
      self._send_hello_parent()

    self._send_configure_right_to_left()
    self._configuration_receiver.initialize()

    self.linker.initialize()

  def _send_hello_parent(self):
    self.send(self.parent, messages.io.hello_parent(self.new_handle(self.parent['id'])))

  @staticmethod
  def from_config(node_config, controller):
    return ComputationNode(
        node_id=node_config['id'],
        left_is_data=node_config['left_is_data'],
        right_is_data=node_config['right_is_data'],
        configure_right_parent_ids=node_config['configure_right_parent_ids'],
        parent=node_config['parent'],
        height=node_config['height'],
        senders=node_config['senders'],
        left_ids=node_config['left_ids'],
        receiver_ids=node_config['receiver_ids'],
        adoptees=node_config['adoptees'],
        connector_json=node_config['connector'],
        migrator_config=node_config['migrator'],
        controller=controller)

  def elapse(self, ms):
    pass

  def deliver(self, message, sequence_number, sender_id):
    pass

  def send_forward_messages(self, before=None):
    return 1 + self._linker.advance_sequence_number()

  def export_to_node(self, receiver):
    if receiver['id'] in self._exporters:
      raise errors.InternalError("Already exporting to this node.", extra={'existing_node_id': receiver['id']})
    self._exporters[receiver['id']] = self.linker.new_exporter(receiver=receiver)

  def _pick_new_receivers_for_kid(self):
    '''
    Return a list of nodes in self._graph that should function as receivers for a newly added kid.
    or None if no list is appropriate.
    '''
    if self.left_is_data:
      if len(self._connector.layers) >= 3:
        return [self.kids[node_id] for node_id in self._picker.get_layer(2)]
      else:
        return None
    else:
      if len(self._connector.layers) >= 2:
        return [self.kids[node_id] for node_id in self._picker.get_layer(1)]
      else:
        return None

  def _pick_new_sender_for_kid(self):
    '''
    Return a list of nodes in self._graph that should function as senders for a newly added kid.
    or None if no list is appropriate.
    '''
    if self.right_is_data:
      if len(self._connector.layers) >= 2:
        return [self.kids[node_id] for node_id in self._connector.layers[len(self._connector.layers) - 1]]
      else:
        return None
    else:
      if len(self._connector.layers) >= 1:
        return [self.kids[node_id] for node_id in self._picker.layers[len(self._connector.layers) - 1]]
      else:
        return None

  def all_incremental_kids_are_spawned(self):
    self._incremental_spawner = None

  def all_kids_are_spawned(self, left_gap, right_gap):
    self._spawner = None
    if left_gap and right_gap:
      raise errors.InternalError("There may not be both a left and a right gap.")
    self._left_gap = left_gap
    self._right_gap = right_gap
    if self._left_configurations_are_sent:
      raise errors.InternalError("all_kids_are_spawned should only be called before left_configurations are sent.")

    self._send_configure_left_to_right()
    if self.parent:
      self._send_hello_parent()

  def _send_configure_left_to_right(self):
    self._left_configurations_are_sent = True

    self.logger.info("Sending configure_new_flow_left", extra={'receiver_ids': list(self._receivers.keys())})

    receiver_to_kids = self._receiver_to_kids()

    for receiver in self._receivers.values():
      message = messages.migration.configure_new_flow_left(self.migration_id, [
          messages.migration.left_configuration(
              node=self.new_handle(receiver['id']),
              height=self.height,
              is_data=False,
              kids=[{
                  'handle': self.transfer_handle(self.kids[kid_id], receiver['id']),
                  'connection_limit': self.system_config['SUM_NODE_RECEIVER_LIMIT']
              } for kid_id in receiver_to_kids.get(receiver['id'], [])],
          )
      ])

      self.send(receiver, message)

  def spawn_kid(self, layer_index, node_id, senders, configure_right_parent_ids, left_ids, migrator):
    self.kids[node_id] = None
    if self.height == 0:
      self._controller.spawn_node(
          messages.sum.sum_node_config(
              node_id=node_id,
              left_is_data=self.left_is_data and layer_index == 1,
              # TODO(KK): This business about right_map here is very ugly.
              # Think hard and try to come up with a way to avoid it.
              right_is_data=self.right_is_data and layer_index + 1 == len(self._connector.layers)
              and bool(self._connector.right_to_parent_ids[node_id]),
              senders=senders,
              receivers=[],
              configure_right_parent_ids=configure_right_parent_ids,
              parent=self.new_handle(node_id),
              migrator=migrator,
          ))
    else:
      self._controller.spawn_node(
          messages.computation.computation_node_config(
              node_id=node_id,
              configure_right_parent_ids=configure_right_parent_ids,
              parent=self.new_handle(node_id),
              left_ids=left_ids,
              height=self.height - 1,
              left_is_data=self.left_is_data and layer_index == 1,
              # TODO(KK): This business about right_map here is very ugly.
              #   Think hard and try to come up with a way to avoid it.
              right_is_data=self.right_is_data and layer_index + 1 == len(self._connector.layers)
              and bool(self._connector.right_to_parent_ids[node_id]),
              senders=senders,
              receiver_ids=None,
              migrator=migrator))

  def _get_new_layers_edges_and_hourglasses(self, new_layers, last_edges, hourglasses):
    self._incremental_spawner = connector.IncrementalSpawner(
        new_layers=new_layers, last_edges=last_edges, hourglasses=hourglasses, connector=self._connector, node=self)
    self._incremental_spawner.start_spawning()

  def _update_left_configuration(self, message):
    if self._left_gap:
      # FIXME(KK): Test this, and implement by forwarding the update_left_configuration message to the proper child.
      import ipdb
      ipdb.set_trace()
    else:
      if self._connector is None:
        raise errors.InternalError("self._connector must be initialized before an update_left_configuration "
                                   "can be received")

      self._get_new_layers_edges_and_hourglasses(*self._connector.add_kids_to_left_configuration([(
          message['parent_id'], kid) for kid in message['new_kids']]))

  @property
  def migration_id(self):
    if self._initial_migrator is not None:
      return self._initial_migrator.migration_id
    else:
      return None

  def _send_configure_right_to_left(self):
    if not self._right_configurations_are_sent:
      self.logger.info("Sending configure_new_flow_right", extra={'receiver_ids': list(self._senders.keys())})
      for sender in self._senders.values():
        self.send(sender,
                  messages.migration.configure_new_flow_right(self.migration_id, [
                      messages.migration.right_configuration(
                          n_kids=None,
                          parent_handle=self.new_handle(sender['id']),
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

    if not self._adoptees:
      self._connector = connector.Connector(
          height=self.height,
          left_configurations=left_configurations,
          left_is_data=self.left_is_data,
          right_is_data=self.right_is_data,
          right_configurations=right_configurations,
          max_outputs=self.system_config['SUM_NODE_RECEIVER_LIMIT'],
          max_inputs=self.system_config['SUM_NODE_SENDER_LIMIT'],
      )
      self.height = self._connector.max_height()
      self._connector.fill_in()
      self._spawner = connector.Spawner(node=self, connector=self._connector)
      self._spawner.start_spawning()
    else:
      # Since this node was adopting, all its eventual kids are already up and running and do not need spawning.
      # We should have received an appropriate Connector instance in the config.
      self._connector = connector.Connector.from_json(
          self._initial_connector_json,
          height=self.height,
          left_configurations=left_configurations,
          left_is_data=self.left_is_data,
          right_is_data=self.right_is_data,
          right_configurations=right_configurations,
          max_outputs=self.system_config['SUM_NODE_RECEIVER_LIMIT'],
          max_inputs=self.system_config['SUM_NODE_SENDER_LIMIT'])
      self.height = self._connector.max_height()
      self._spawner = None
      self._send_configure_left_to_right()

  def new_left_configurations(self, left_configurations):
    '''For when a fully configured node gets new left_configurations'''
    self._get_new_layers_edges_and_hourglasses(*self._connector.add_left_configurations(left_configurations))

  def new_right_configurations(self, right_configurations):
    '''For when a fully configured node gets new right_configurations'''
    #FIXME(KK): Implement this
    import ipdb
    ipdb.set_trace()

  def receive(self, message, sender_id):
    if self._configuration_receiver.receive(message=message, sender_id=sender_id):
      return
    elif message['type'] == 'added_sender':
      node = message['node']
      self._senders[node['id']] = node
      self.send(node,
                messages.migration.configure_new_flow_right(None, [
                    messages.migration.right_configuration(
                        n_kids=None,
                        parent_handle=self.new_handle(node['id']),
                        height=self.height,
                        is_data=False,
                        connection_limit=self.system_config['SUM_NODE_SENDER_LIMIT'],
                    )
                ]))
    elif message['type'] == 'adopt':
      if self.parent is None:
        raise errors.InternalError("Root nodes may not adopt a new parent.")
      self.send(self.parent, messages.io.goodbye_parent())
      self.parent = message['new_parent']
      self._send_hello_parent()
    elif message['type'] == 'hello_parent':
      if self._pending_adoptees is not None and sender_id in self._pending_adoptees:
        self._pending_adoptees.remove(sender_id)
        if not self._pending_adoptees:
          self._pending_adoptees = None
          self._send_hello_parent()
      self.kids[sender_id] = message['kid']
      if self._spawner is not None:
        self._spawner.spawned_a_kid(message['kid'])
      if self._incremental_spawner is not None:
        self._incremental_spawner.spawned_a_kid(message['kid'])
      if self._proxy_spawner is not None:
        self._proxy_spawner.spawned_a_kid(message['kid'])
    elif message['type'] == 'goodbye_parent':
      if sender_id not in self.kids:
        raise errors.InternalError(
            "Got a goodbye_parent from a node that is not a kid of self.", extra={'kid_id': sender_id})
      self.kids.pop(sender_id)
    elif message['type'] == 'bumped_height':
      self._proxy_spawner = proxy_spawner.ProxySpawner(node=self)
      self._proxy_spawner.respond_to_bumped_height(
          proxy=message['proxy'], kid_ids=message['kid_ids'], variant=message['variant'])
    elif message['type'] == 'update_left_configuration':
      self._update_left_configuration(message)
    else:
      super(ComputationNode, self).receive(message=message, sender_id=sender_id)

  def bumped_to_new_connector(self, left_configurations, left_id, right_id):
    self._proxy_spawner = None
    self._connector = connector.Connector(
        height=self.height,
        left_configurations=left_configurations,
        left_is_data=self.left_is_data,
        right_is_data=self.right_is_data,
        right_configurations=self._connector._right_configurations,
        max_outputs=self.system_config['SUM_NODE_RECEIVER_LIMIT'],
        max_inputs=self.system_config['SUM_NODE_SENDER_LIMIT'],
    )
    self._connector.fill_in(new_node_ids=[left_id, right_id])
    self.height = self._connector.max_height()

  def _receiver_to_kids(self):
    if self._connector is not None:
      result = {}
      for right_node_id, receiver_ids in self._connector.right_to_parent_ids.items():
        for receiver_id in receiver_ids:
          if receiver_id not in result:
            result[receiver_id] = [right_node_id]
          else:
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
      return self.kids
    elif message['type'] == 'get_senders':
      return self._senders
    elif message['type'] == 'get_receivers':
      return self._receivers
    else:
      return super(ComputationNode, self).handle_api_message(message)
