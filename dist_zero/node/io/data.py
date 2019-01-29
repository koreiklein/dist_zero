import logging

from dist_zero import settings, messages, errors, recorded, importer, exporter, misc, ids
from dist_zero.network_graph import NetworkGraph
from dist_zero.node.node import Node
from dist_zero.node.io import leaf_html
from dist_zero.node.io import leaf

logger = logging.getLogger(__name__)


class DataNode(Node):
  '''
  The root of a tree of leaf instances of the same ``variant``.

  Each `DataNode` instance is responsible for keeping track of the state of its subtree, and for growing
  or shrinking it as necessary.  In particular, when new leaves are created, `DataNode.create_kid_config` must
  be called on the desired immediate parent to generate the node config for starting that child.

  Each `DataNode` will have an associated height.  The assignment of heights to data nodes is the unique
  minimal assignment such that n.height+1 == n.parent.height for every node n that has a parent.
  '''

  def __init__(self, node_id, parent, controller, variant, leaf_config, height, recorded_user_json):
    '''
    :param str node_id: The id to use for this node
    :param parent: If this node is the root, then `None`.  Otherwise, the :ref:`handle` of its parent `Node`.
    :type parent: :ref:`handle` or `None`
    :param str variant: 'input' or 'output'
    :param int height: The height of the node in the tree.  See `DataNode`
    :param `MachineController` controller: The controller for this node.
    :param objcect leaf_config: Configuration information for how to run a leaf.
    :param object recorded_user_json: None, or configuration for a recorded user.  Only allowed if this is a height -1 Node.
    '''
    self._controller = controller
    self._parent = parent
    self._sent_hello = False
    self._variant = variant
    self._height = height
    self._leaf_config = leaf_config
    if self._height == -1:
      self._leaf = leaf.Leaf.from_config(leaf_config)
    else:
      self._leaf = None

    self.id = node_id
    self._kids = {}
    self._kid_summaries = {}

    self._added_sender_respond_to = None

    self._exporter = None
    self._importer = None

    self._updated_summary = True
    '''Set to true when the currenty summary may have changed.'''
    self._last_kid_summary = None

    self._domain_name = None
    self._routing_kids_listener = None

    self._pending_spawned_kids = set()

    self._load_balancer_frontend = None
    '''
    DataNodes with height > 0 will manager a `LoadBalancerFrontend` when
    they start routing.
    '''

    if recorded_user_json is None:
      self._recorded_user = None
    else:
      self._recorded_user = recorded.RecordedUser.from_json(recorded_user_json)

    self._dns_controller = None
    '''
    Root DataNodes will create a `DNSController` instance when they start routing,
    and use it configure the appropriate DNS mapping.
    '''

    self._leaving_kids = None
    '''
    None if this node is not merging with another node.
    Otherwise, the set of kids that must leave this node before it has lost all its kids and it's safe to terminate.
    '''

    self._merging_kid_ids = set()
    '''Set of kid ids of kids that are in process of merging with another kid.'''

    self._graph = NetworkGraph()

    self._root_proxy_id = None
    '''
    While in the process of bumping its height, the root node sets this to the id of the node that will take over as its
    proxy.
    '''
    self._kids_for_proxy_to_adopt = None
    '''
    While in the process of bumping its height, the root node sets this to the list of handles of the kids that the
    proxy will be taking.
    '''

    self._root_consuming_proxy_id = None
    '''
    While in the process of decreasing its height, the root node sets this to the id of the proxy node that it is
    consuming.
    '''

    # To limit excessive warnings regarding being at low capacity.
    self._warned_low_capacity = False

    # If this node is spawned at too great a height, it must spawn a kid before it's ready to do anything else.
    # In case there is such a kid, self._startup_kid gives its id.
    self._startup_kid = None

    self._http_server_for_adding_leaves = None
    '''
    Height 0 `DataNode` instances when bound to a domain name will bind an http server
    to a port on their machine and that server will respond to http GET requests
    by creating new leaf configs and sending back appropriate html for running
    the leaf.
    '''

    super(DataNode, self).__init__(logger)

    CHECK_INTERVAL = self.system_config['KID_SUMMARY_INTERVAL']
    self._stop_recorded_user = None
    if self._recorded_user is not None:
      self._recorded_user.simulate(self._controller, self._receive_input_action)

    self._stop_checking_limits = self._controller.periodically(CHECK_INTERVAL,
                                                               lambda: self._check_limits(CHECK_INTERVAL))

    self._time_since_no_mergable_kids_ms = 0
    self._time_since_no_consumable_proxy = 0

  def _receive_input_action(self, message):
    if self._variant != 'input':
      raise errors.InternalError("Only 'input' variant nodes may receive input actions")

    if self._exporter is not None:
      self.logger.debug(
          "Leaf node is forwarding input_action of {number} via exporter", extra={'number': message['number']})
      self._exporter.export_message(message=message, sequence_number=self.linker.advance_sequence_number())
    else:
      self.logger.warning(
          "Leaf node is not generating an input_action message send since it does not yet have an exporter.")

  def is_data(self):
    return True

  @property
  def current_state(self):
    if self._height != -1:
      raise errors.InternalError("Non-leaf DataNodes do not maintain a current_state.")
    else:
      return self._leaf.state

  @property
  def height(self):
    return self._height

  @property
  def _adjacent(self):
    if self._variant == 'input':
      if self._exporter is None:
        return None
      else:
        return self._exporter.receiver
    elif self._variant == 'output':
      if self._importer is None:
        return None
      else:
        return self._importer.sender
    else:
      raise errors.InternalError(f"Unrecognized variant {self._variant}")

  def checkpoint(self, before=None):
    pass

  def sink_swap(self, deltas, old_sender_ids, new_senders, new_importers, linker):
    if self._variant == 'output':
      if len(new_senders) != 1:
        raise errors.InternalError(
            "sink_swap should be called on an edge data node only when there is a unique new sender.")
      self._set_input(new_senders[0])
    elif self._variant == 'input':
      raise errors.InternalError("An input DataNode should never function as a sink node in a migration.")
    else:
      raise errors.InternalError('Unrecognized variant "{}"'.format(self._variant))

  def switch_flows(self, migration_id, old_exporters, new_exporters, new_receivers):
    self._updated_summary = True
    if self._variant == 'input':
      for receiver in new_receivers:
        self._set_output(receiver)
    elif self._variant == 'output':
      raise errors.InternalError("Output DataNode should never function as a source migrator in a migration.")
    else:
      raise errors.InternalError('Unrecognized variant "{}"'.format(self._variant))

    for kid in self._kids.values():
      self.send(kid, messages.migration.switch_flows(migration_id))

  def initialize(self):
    if self._height > 0 and len(self._kids) == 0:
      # unless we are height 0, we must have a new kid.
      self._startup_kid = self._spawn_kid()
    else:
      if self._parent is not None:
        self._send_hello_parent()

  def _send_hello_parent(self):
    if not self._sent_hello:
      self._sent_hello = True
      self.send(self._parent, messages.io.hello_parent(self.new_handle(self._parent['id'])))
    else:
      raise errors.InternalError("Already sent hello")

  def _spawn_kid(self):
    if self._height == 0:
      raise errors.InternalError("height 0 DataNode instances can not spawn kids")
    elif self._root_proxy_id is not None:
      raise errors.InternalError("Root nodes may not spawn new kids while their are bumping their height.")
    elif self._root_consuming_proxy_id is not None:
      raise errors.InternalError("Root nodes may not spawn new kids while their are decreasing their height "
                                 "by consuming a proxy.")
    else:
      node_id = ids.new_id("DataNode_kid")
      self._pending_spawned_kids.add(node_id)
      self._kid_summaries[node_id] = messages.io.kid_summary(
          size=0, n_kids=0, availability=self._leaf_availability * self._kid_capacity_limit)
      self._updated_summary = True
      self.logger.info("DataNode is spawning a new kid", extra={'new_kid_id': node_id})
      self._controller.spawn_node(
          messages.io.data_node_config(
              node_id=node_id,
              parent=self.new_handle(node_id),
              variant=self._variant,
              leaf_config=self._leaf_config,
              height=self._height - 1,
          ))
      return node_id

  def _check_for_kid_limits(self):
    '''In case the kids of self are hitting any limits, address them.'''
    if self._height > 0:
      self._check_for_low_capacity()

  def _check_for_consumable_proxy(self, ms):
    TIME_TO_WAIT_BEFORE_CONSUME_PROXY_MS = 4 * 1000

    if self._parent is None:
      if len(self._kids) == 1 and not self._root_consuming_proxy_id and self._height > 1:
        self._time_since_no_consumable_proxy += ms
        if self._time_since_no_consumable_proxy >= TIME_TO_WAIT_BEFORE_CONSUME_PROXY_MS:
          self._consume_proxy()
      else:
        self._time_since_no_consumable_proxy = 0

  def _consume_proxy(self):
    '''Method for a root node to absorb its unique kid.'''
    if self._parent is not None or len(self._kids) != 1:
      raise errors.InternalError("Must have a unique kid and be root to consume a proxy.")

    if self._root_consuming_proxy_id is not None:
      raise errors.InternalError("Root node is already in the process of consuming a separate proxy node.")

    proxy = next(iter(self._kids.values()))
    self._root_consuming_proxy_id = proxy['id']
    self.send(proxy, messages.io.merge_with(self.new_handle(proxy['id'])))

  def _check_for_mergeable_kids(self, ms):
    '''Check whether any two kids should be merged.'''
    TIME_TO_WAIT_BEFORE_KID_MERGE_MS = 2 * 1000

    if self._height > 0:
      best_pair = self._best_mergeable_kids()
      if best_pair is None or self._merging_kid_ids:
        self._time_since_no_mergable_kids_ms = 0
      else:
        self._time_since_no_mergable_kids_ms += ms

        if self._time_since_no_mergable_kids_ms >= TIME_TO_WAIT_BEFORE_KID_MERGE_MS:
          self._merge_kids(*best_pair)

  def _merge_kids(self, left_kid_id, right_kid_id):
    '''
    Merge the kids identified by left_kid_id and right_kid_id

    :param str left_kid_id: The id of one kid to merge.
    :param str right_kid_id: The id of another kid to merge.
    '''
    self._merging_kid_ids.add(left_kid_id)
    self.send(self._kids[left_kid_id],
              messages.io.merge_with(self.transfer_handle(self._kids[right_kid_id], left_kid_id)))

  def _best_mergeable_kids(self):
    '''
    Find the best pair of mergeable kids if they exist.

    :return: None if no 2 kids are mergable.  Otherwise, a pair of the ids of two mergeable kids.
    '''
    # Current algorithm: 2 kids can be merged if each has n_kids less than 1/3 the max
    if len(self._kid_summaries) >= 2:
      MAX_N_KIDS = self.system_config['DATA_NODE_KIDS_LIMIT']
      if MAX_N_KIDS <= 3:
        MERGEABLE_N_KIDS_FIRST = MERGEABLE_N_KIDS_SECOND = 1
      else:
        MERGEABLE_N_KIDS_FIRST = MERGEABLE_N_KIDS_SECOND = MAX_N_KIDS // 3
      n_kids_kid_id_pairs = [(kid_summary['n_kids'], kid_id) for kid_id, kid_summary in self._kid_summaries.items()]
      n_kids_kid_id_pairs.sort()
      (least_n_kids, least_id), (next_least_n_kids, next_least_id) = n_kids_kid_id_pairs[:2]

      if least_n_kids <= MERGEABLE_N_KIDS_FIRST and next_least_n_kids <= MERGEABLE_N_KIDS_SECOND:
        return least_id, next_least_id
    return None

  def _check_for_low_capacity(self):
    '''Check whether the total capacity of this node's kids is too low.'''
    total_kid_capacity = sum(
        self._kid_capacity_limit - kid_summary['size'] for kid_summary in self._kid_summaries.values())

    if total_kid_capacity <= self.system_config['TOTAL_KID_CAPACITY_TRIGGER']:
      if len(self._kids) < self.system_config['DATA_NODE_KIDS_LIMIT']:
        if self._root_proxy_id is None:
          self._spawn_kid()
        else:
          self.logger.warning("Can't spawn children while bumping root node height.")
      else:
        if self._parent is None:
          if self._root_proxy_id is None:
            self._bump_height()
          else:
            # This happens when we've tried to bump the height once already, and the trigger fires again
            # while the newly spawned node bumping the height has not yet confirmed that it is running properly.
            self.logger.warning("Can't bump root node height, as we are waiting for a proxy to spawn.")
        else:
          if not self._warned_low_capacity:
            self._warned_low_capacity = True
            self.logger.warning("nonroot DataNode instance had too little capacity and no room to spawn more kids. "
                                "Capacity is remaining low and is not being increased.")
    else:
      self._warned_low_capacity = False

  def _bump_height(self):
    if self._parent is not None:
      raise errors.InternalError("Only the root node may bump its height.")

    self.logger.info("Root node is starting to bump its height in response to low capacity.")

    self._root_proxy_id = ids.new_id('DataNode_root_proxy')
    self._kids_for_proxy_to_adopt = list(self._kids.values())
    self._height += 1
    self._pending_spawned_kids.add(self._root_proxy_id)
    self._kid_summaries = {}
    self._updated_summary = True
    self._controller.spawn_node(
        messages.io.adopter_node_config(
            adoptees=[self.transfer_handle(kid, self._root_proxy_id) for kid in self._kids_for_proxy_to_adopt],
            data_node_config=messages.io.data_node_config(
                node_id=self._root_proxy_id,
                parent=self.new_handle(self._root_proxy_id),
                variant=self._variant,
                leaf_config=self._leaf_config,
                height=self._height - 1,
            )))

  def _finish_bumping_height(self, proxy):
    self._kid_summaries = {}
    self._updated_summary = True
    self._kids = {proxy['id']: proxy}
    self._graph = NetworkGraph()
    self._graph.add_node(proxy['id'])
    if self._adjacent is not None:
      self.send(
          self._adjacent,
          messages.io.bumped_height(
              proxy=self.transfer_handle(proxy, self._adjacent['id']),
              kid_ids=[kid['id'] for kid in self._kids_for_proxy_to_adopt],
              variant=self._variant))

    self._root_proxy_id = None
    self._kids_for_proxy_to_adopt = None

  def set_initial_kids(self, kids):
    if self._kids:
      raise errors.InternalError("DataNode already has kids")

    self._kids = kids
    for kid_id in kids.keys():
      self._graph.add_node(kid_id)

  def _finish_adding_kid(self, kid):
    kid_id = kid['id']
    self._updated_summary = True
    self._kids[kid_id] = kid
    self._graph.add_node(kid_id)

    if self._exporter is not None:
      self.send(
          self._exporter.receiver,
          messages.migration.update_left_configuration(
              parent_id=self.id,
              new_kids=[{
                  'connection_limit': self.system_config['SUM_NODE_SENDER_LIMIT'],
                  'handle': self.transfer_handle(handle=kid, for_node_id=self._exporter.receiver_id)
              }],
              new_height=self._height))

    if self._importer is not None:
      self.send(
          self._importer.sender,
          messages.migration.update_right_configuration(
              parent_id=self.id,
              new_kids=[self.transfer_handle(kid, self._importer.sender_id)],
              new_height=self._height))

    self._graph.add_node(kid_id)

  def _terminate(self):
    self._stop_checking_limits()
    self._controller.terminate_node(self.id)

  def _maybe_kids_have_left(self):
    if not self._leaving_kids:
      self._terminate()

  def receive(self, message, sender_id):
    if self._routing_kids_listener is not None and self._routing_kids_listener.receive(
        message=message, sender_id=sender_id):
      pass
    elif message['type'] == 'configure_new_flow_right':
      if self._exporter is not None or len(message['right_configurations']) != 1 or self._variant != 'input':
        raise errors.InternalError("A new configure_new_flow_right should only ever arrive at an 'input' DataNode "
                                   "and only when it's waiting to set its exporter,"
                                   " and when the configure_new_flow_right has a single right_configuration.")
      right_config, = message['right_configurations']
      node = right_config['parent_handle']
      self._set_output(node)
      self.send(
          node,
          messages.migration.configure_new_flow_left(
              migration_id=None,
              left_configurations=[
                  messages.migration.left_configuration(
                      height=self._height,
                      is_data=True,
                      node=self.new_handle(node['id']),
                      kids=[{
                          'connection_limit': self.system_config['SUM_NODE_SENDER_LIMIT'],
                          'handle': self.transfer_handle(kid, node['id'])
                      } for kid in self._kids.values()],
                  )
              ]))
    elif message['type'] == 'configure_new_flow_left':
      for left_config in message['left_configurations']:
        node = left_config['node']
        if self._height == -1 and left_config['state']:
          self._leaf.set_state(left_config['state'])
        self._set_input(node)
    elif message['type'] == 'routing_start':
      self._on_routing_start(message=message, sender_id=sender_id)
    elif message['type'] == 'hello_parent':
      if sender_id == self._startup_kid and self._parent is not None:
        self._send_hello_parent()

      if sender_id == self._root_proxy_id:
        self._finish_bumping_height(message['kid'])
      else:
        self._finish_adding_kid(message['kid'])
      self._updated_summary = True
    elif message['type'] == 'goodbye_parent':
      self._updated_summary = True
      if sender_id in self._merging_kid_ids:
        self._merging_kid_ids.remove(sender_id)
      if sender_id in self._kids:
        self._kids.pop(sender_id)
      if sender_id in self._kid_summaries:
        self._kid_summaries.pop(sender_id)

      if self._leaving_kids is not None and sender_id in self._leaving_kids:
        self._leaving_kids.remove(sender_id)
        self._maybe_kids_have_left()

      if sender_id == self._root_consuming_proxy_id:
        self._complete_consuming_proxy()
    elif message['type'] == 'kid_summary':
      if message != self._kid_summaries.get(sender_id, None):
        if sender_id in self._kids:
          self._kid_summaries[sender_id] = message
          self._updated_summary = True
          self._check_for_kid_limits()
    elif message['type'] == 'configure_right_parent':
      pass
    elif message['type'] == 'added_sender':
      node = message['node']
      self.send(
          node,
          messages.migration.configure_new_flow_right(None, [
              messages.migration.right_configuration(
                  n_kids=len(self._kids) if self._height >= 0 else None,
                  parent_handle=self.new_handle(node['id']),
                  height=self._height,
                  is_data=True,
                  availability=self.availability(),
                  connection_limit=self.system_config['SUM_NODE_SENDER_LIMIT'] if self._height >= 0 else 1,
              )
          ]))
      self._added_sender_respond_to = message['respond_to']
    elif message['type'] == 'merge_with':
      if self._parent is None:
        raise errors.InternalError("Root nodes can not merge with other nodes.")
      new_parent = message['node']
      for kid in self._kids.values():
        self.send(kid, messages.migration.adopt(self.transfer_handle(new_parent, kid['id'])))
      self.send(self._parent, messages.io.goodbye_parent())
      self._leaving_kids = set(self._kids.keys())
      self._maybe_kids_have_left()
    elif message['type'] == 'adopt':
      if self._parent is None:
        raise errors.InternalError("Root nodes may not adopt a new parent.")
      self.send(self._parent, messages.io.goodbye_parent())
      self._parent = message['new_parent']
      self._updated_summary = True
      self._sent_hello = False
      self._send_hello_parent()
    else:
      super(DataNode, self).receive(message=message, sender_id=sender_id)

  def _complete_consuming_proxy(self):
    if self._parent is not None:
      raise errors.InternalError("Only root nodes should complete consuming a proxy node.")
    if self._height < 2:
      raise errors.InternalError("A root node should have a height >= 2 when it completes consuming its proxy.")
    self._height -= 1
    self._root_consuming_proxy_id = None

  def _set_input(self, node):
    if self._importer is not None:
      raise errors.InternalError("DataNodes have only a single input node."
                                 "  Can not add a new one once an input already exists")
    if self._variant != 'output':
      raise errors.InternalError("Only output DataNodes can set their input.")

    self._importer = self.linker.new_importer(node)
    if self._added_sender_respond_to:
      self.send(self._added_sender_respond_to, messages.migration.finished_adding_sender(sender_id=node['id']))
      self._added_sender_respond_to = None

  def _set_output(self, node):
    if self._exporter is not None:
      if node['id'] != self._exporter.receiver_id:
        raise errors.InternalError("DataNodes have only a single output node."
                                   "  Can not add a new one once an output already exists")
    else:
      if self._variant != 'input':
        raise errors.InternalError("Only input DataNodes can set their output.")
      self._exporter = self.linker.new_exporter(node)

  @staticmethod
  def from_config(node_config, controller):
    return DataNode(
        node_id=node_config['id'],
        parent=node_config['parent'],
        controller=controller,
        leaf_config=node_config['leaf_config'],
        variant=node_config['variant'],
        height=node_config['height'],
        recorded_user_json=node_config['recorded_user_json'])

  def elapse(self, ms):
    self.linker.elapse(ms)

  def _check_limits(self, ms):
    if self._updated_summary or self._height == 0:
      self._send_kid_summary()
      self._updated_summary = False
    self._check_for_kid_limits()
    self._check_for_mergeable_kids(ms)
    self._check_for_consumable_proxy(ms)

  def _send_kid_summary(self):
    if self._parent is not None and self._height >= 0:
      message = messages.io.kid_summary(
          size=(sum(kid_summary['size']
                    for kid_summary in self._kid_summaries.values()) if self._height > 0 else len(self._kids)),
          n_kids=len(self._kids),
          availability=self.availability())
      if (self._parent['id'], message) != self._last_kid_summary:
        self._last_kid_summary = (self._parent['id'], message)
        self.send(self._parent, message)

  @property
  def _branching_factor(self):
    return self.system_config['DATA_NODE_KIDS_LIMIT']

  @property
  def _kid_capacity_limit(self):
    return self._branching_factor**self._height

  @property
  def _leaf_availability(self):
    return self.system_config['SUM_NODE_SENDER_LIMIT']

  def availability(self):
    if self._height == -1:
      # FIXME(KK): Remove availability based on how many nodes are sending to self.
      return self._leaf_availability
    else:
      from_spawned_kids = sum(kid_summary['availability'] for kid_summary in self._kid_summaries.values())
      from_space_to_spawn_new_kids = self._leaf_availability * self._kid_capacity_limit * (
          self._branching_factor - len(self._kid_summaries))
      return from_spawned_kids + from_space_to_spawn_new_kids

  def _get_capacity(self):
    # find the best kid
    highest_capacity_kid_id, max_kid_capacity, size = None, 0, 0
    for kid_id, kid_summary in self._kid_summaries.items():
      size += kid_summary['size']
      kid_capacity = self._kid_capacity_limit - kid_summary['size']
      if kid_capacity > max_kid_capacity:
        highest_capacity_kid_id, max_kid_capacity = kid_id, kid_capacity

    if highest_capacity_kid_id is None:
      if self._height == 0:
        highest_capacity_kid = None
      else:
        raise errors.NoCapacityError()
    else:
      highest_capacity_kid = self._kids[highest_capacity_kid_id]

    return {
        'height': self._height,
        'size': size,
        'max_size': self._kid_capacity_limit * self._branching_factor,
        'highest_capacity_kid': highest_capacity_kid,
    }

  def stats(self):
    return {
        'height': self._height,
        'n_retransmissions': self.linker.n_retransmissions,
        'n_reorders': self.linker.n_reorders,
        'n_duplicates': self.linker.n_duplicates,
        'sent_messages': self.linker.least_unused_sequence_number,
        'acknowledged_messages': self.linker.least_unacknowledged_sequence_number(),
    }

  def _add_leaf_from_http_get(self, request):
    client_host, client_port = request.client_address
    # Please don't let any unsanitized user provived data into the id
    user_machine_id = ids.new_id('web_client_machine')
    user_name = 'std_web_client_node'
    kid_config = self.create_kid_config(name=user_name, machine_id=user_machine_id)
    return leaf_html.from_kid_config(kid_config)

  def _on_routing_start(self, message, sender_id):
    self._domain_name = message['domain_name']
    if self._height == 0:
      self._http_server_for_adding_leaves = self._controller.new_http_server(
          self._domain_name, lambda request: self._add_leaf_from_http_get(request))
      self.send(self._parent, messages.io.routing_started(server_address=self._http_server_for_adding_leaves.address()))
    else:
      self._routing_kids_listener = RoutingKidsListener(self)
      self._routing_kids_listener.start()

  def routing_kids_finished(self, kid_to_address):
    # Start a load balancer based on kid_to_addresses
    self._load_balancer_frontend = self._controller.new_load_balancer_frontend(
        domain_name=self._domain_name, height=self._height)

    for kid_id, address in kid_to_address.items():
      weight = 1
      self._load_balancer_frontend[address] = weight

    self._load_balancer_frontend.sync()

    if self._parent is not None:
      # Send a routing_started with the address of the load balancer.
      self.send(self._parent, messages.io.routing_started(self._load_balancer_frontend.address()))
    else:
      # Configure DNS based on kid_to_addresses (or the load balancer)
      self._map_all_dns_to(self._load_balancer_frontend.address())

  def _map_all_dns_to(self, server_address):
    self._dns_controller = self._controller.new_dns_controller(self._domain_name)
    self._dns_controller.set_all(server_address['ip'])

  def _route_dns(self, message):
    self._domain_name = message['domain_name']
    self._routing_kids_listener = RoutingKidsListener(self)
    self._routing_kids_listener.start()

  def handle_api_message(self, message):
    if message['type'] == 'create_kid_config':
      return self.create_kid_config(name=message['new_node_name'], machine_id=message['machine_id'])
    elif message['type'] == 'kill_node':
      if self._parent:
        self.send(self._parent, messages.io.goodbye_parent())
      self._terminate()
    elif message['type'] == 'route_dns':
      self._route_dns(message)
    elif message['type'] == 'get_capacity':
      return self._get_capacity()
    elif message['type'] == 'get_kids':
      return self._kids
    elif message['type'] == 'get_senders':
      if self._importer is None:
        return {}
      else:
        return {self._importer.sender_id: self._importer.sender}
    elif message['type'] == 'get_receivers':
      if self._exporter is None:
        return {}
      else:
        return {self._exporter.receiver_id: self._exporter.receiver}
    elif message['type'] == 'get_adjacent_handle':
      return self._adjacent
    elif message['type'] == 'get_output_state':
      if self._height != -1:
        raise errors.InternalError("Can't get output state for a DataNode with height >= 0")
      return self._leaf.state
    else:
      return super(DataNode, self).handle_api_message(message)

  def create_kid_config(self, name, machine_id):
    '''
    Generate a config for a new child leaf node, and mark it as a pending child on this parent node.

    :param str name: The name to use for the new node.

    :param str machine_id: The id of the MachineController which will run the new node.
    :return: A config for the new child node.
    :rtype: :ref:`message`
    '''
    if self._height != 0:
      raise errors.InternalError("Only DataNode instances of height 0 should create kid configs.")

    node_id = ids.new_id('LeafNode_{}'.format(name))
    self.logger.info(
        "Registering a new leaf node config for an internal node. name='{node_name}'",
        extra={
            'data_node_id': self.id,
            'leaf_node_id': node_id,
            'node_name': name
        })
    self._kids[node_id] = None

    return messages.io.data_node_config(
        node_id=node_id,
        parent=self.new_handle(node_id),
        variant=self._variant,
        height=-1,
        leaf_config=self._leaf_config)

  def deliver(self, message, sequence_number, sender_id):
    if self._variant != 'output' or self._height != -1:
      raise errors.InternalError("Only 'output' variant leaf nodes may receive output actions")

    self._leaf.update_current_state(message)

    self.linker.advance_sequence_number()


class RoutingKidsListener(object):
  def __init__(self, node):
    self._node = node
    self._kid_to_address = {node_id: None for node_id in node._kids.keys()}

  def start(self):
    for node_id, node in self._node._kids.items():
      self._node.send(node, messages.io.routing_start(self._node._domain_name))

  def receive(self, message, sender_id):
    if message['type'] == 'routing_started':
      self._kid_to_address[sender_id] = message['server_address']
      if all(val is not None for val in self._kid_to_address.values()):
        self._node.routing_kids_finished(self._kid_to_address)
      return True

    return False
