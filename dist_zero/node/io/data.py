import logging

from dist_zero import settings, messages, errors, recorded, \
    importer, exporter, misc, ids, transaction, message_rate_tracker
from dist_zero.node.node import Node
from dist_zero.node.io import leaf_html
from dist_zero.node.io import leaf

from .monitor import Monitor
from .transactions import remove_leaf

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
    :param object recorded_user_json: None, or configuration for a recorded user.  Only allowed if this is a height 0 Node.
    '''
    self._controller = controller
    self._parent = parent
    self._variant = variant
    self._height = height
    self._leaf_config = leaf_config
    if self._height == 0:
      self._leaf = leaf.Leaf.from_config(leaf_config)
    else:
      self._leaf = None

    self.id = node_id

    # The role to start this node should initialize _kids
    # Giving the interval this node is responsible for.
    # Leaf nodes with an interval_type of 'point' are identified only by their left coordinate,
    # their right interval coordinate will always be None
    self._kids = None

    if self._height == 0:
      self._message_rate_tracker = message_rate_tracker.MessageRateTracker()

    self._added_sender_respond_to = None

    self._updated_summary = True
    '''Set to true when the current summary may have changed.'''
    self._last_kid_summary = None

    self._domain_name = None
    self._routing_kids_listener = None

    self._load_balancer_frontend = None
    '''
    DataNodes with height > 0 will manage a `LoadBalancerFrontend` when
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

    self._monitor = Monitor(self)

    self._stop_checking_limits = self._controller.periodically(CHECK_INTERVAL,
                                                               lambda: self._monitor.check_limits(CHECK_INTERVAL))

  def _receive_input_action(self, message):
    if self._variant != 'input':
      raise errors.InternalError("Only 'input' variant nodes may receive input actions")

    self.logger.warning(
        "Leaf node is not generating an input_action message send since it does not yet have an exporter.")

  def is_data(self):
    return True

  @property
  def current_state(self):
    if self._height != 0:
      raise errors.InternalError("Non-leaf DataNodes do not maintain a current_state.")
    else:
      return self._leaf.state

  @property
  def height(self):
    return self._height

  def checkpoint(self, before=None):
    pass

  @property
  def MERGEABLE_N_KIDS_FIRST(self):
    MAX_N_KIDS = self.system_config['DATA_NODE_KIDS_LIMIT']
    if MAX_N_KIDS <= 3:
      return 1
    else:
      return MAX_N_KIDS // 3

  @property
  def MERGEABLE_N_KIDS_SECOND(self):
    return self.MERGEABLE_N_KIDS_FIRST

  def _best_mergeable_kids(self, do_not_use_ids):
    '''
    Find the best pair of mergeable kids if they exist.

    :return: None if no 2 kids are mergable.  Otherwise, a pair of the ids of two mergeable kids.
    '''
    ordered_kid_ids = list(self._kids)
    best_pair = None
    least_total_kids = None
    for left_id, right_id in zip(ordered_kid_ids, ordered_kid_ids[1:]):
      if left_id not in do_not_use_ids and right_id not in do_not_use_ids:
        if self._kids_are_mergeable(left_id, right_id):
          total_kids = self._kids.summaries[left_id]['n_kids'] + self._kids.summaries[right_id]['n_kids']
          if best_pair is None or total_kids < least_total_kids:
            best_pair = (left_id, right_id)
            least_total_kids = total_kids

    return best_pair

  def _kids_are_mergeable(self, left_id, right_id):
    return left_id in self._kids.summaries and right_id in self._kids.summaries and \
        self._kids.summaries[left_id]['n_kids'] <= self.MERGEABLE_N_KIDS_FIRST and \
        self._kids.summaries[right_id]['n_kids'] <= self.MERGEABLE_N_KIDS_SECOND

  def _terminate(self):
    self._stop_checking_limits()
    self._controller.terminate_node(self.id)

  def receive(self, message, sender_id):
    if self._routing_kids_listener is not None and self._routing_kids_listener.receive(
        message=message, sender_id=sender_id):
      pass
    elif message['type'] == 'routing_start':
      self._on_routing_start(message=message, sender_id=sender_id)
    elif message['type'] == 'goodbye_parent':
      self.start_transaction_eventually(remove_leaf.RemoveLeaf(kid_id=sender_id))
    elif message['type'] == 'kid_summary':
      if sender_id in self._kids:
        if message != self._kids.summaries.get(sender_id, None):
          self._kids.set_summary(sender_id, message)
          if self._monitor.out_of_capacity():
            # These updates should be propogated immediately.
            self._send_kid_summary()
          else:
            self._updated_summary = True
          self._monitor.check_limits(0)
    elif message['type'] == 'configure_right_parent':
      pass
    else:
      super(DataNode, self).receive(message=message, sender_id=sender_id)

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

  def _interval_json(self):
    return self._kids.interval_json()

  def _interval(self):
    return self._kids.interval()

  def _estimated_messages_per_second(self):
    if self._height == 0:
      return self._message_rate_tracker.estimate_rate_hz(self.linker.now_ms)
    else:
      return sum(kid_summary['messages_per_second'] for kid_summary in self._kids.summaries.values())

  def _kid_summary_message(self):
    return messages.io.kid_summary(
        size=(sum(kid_summary['size']
                  for kid_summary in self._kids.summaries.values()) if self._height > 1 else len(self._kids)),
        n_kids=len(self._kids),
        height=self._height,
        messages_per_second=self._estimated_messages_per_second(),
        availability=self.availability())

  def _send_kid_summary(self):
    if self._parent is not None and self._height > 0:
      message = self._kid_summary_message()
      if (self._parent['id'], message) != self._last_kid_summary:
        self._last_kid_summary = (self._parent['id'], message)
        self.send(self._parent, message)

  @property
  def _branching_factor(self):
    return self.system_config['DATA_NODE_KIDS_LIMIT']

  @property
  def _kid_capacity_limit(self):
    return self._branching_factor**(self._height - 1)

  @property
  def _leaf_availability(self):
    return self.system_config['SUM_NODE_SENDER_LIMIT']

  def availability(self):
    if self._height == 0:
      # FIXME(KK): Remove availability based on how many nodes are sending to self.
      return self._leaf_availability
    else:
      from_spawned_kids = sum(kid_summary['availability'] for kid_summary in self._kids.summaries.values())
      from_space_to_spawn_new_kids = self._leaf_availability * self._kid_capacity_limit * (
          self._branching_factor - len(self._kids.summaries))
      return from_spawned_kids + from_space_to_spawn_new_kids

  def _get_capacity(self):
    # find the best kid
    highest_capacity_kid_id, highest_capacity_kid, max_kid_capacity, size = None, None, 0, 0
    if self._height != 1:
      for kid_id, kid_summary in self._kids.summaries.items():
        size += kid_summary['size']
        kid_capacity = self._kid_capacity_limit - kid_summary['size']
        if kid_capacity > max_kid_capacity:
          highest_capacity_kid_id, max_kid_capacity = kid_id, kid_capacity

      if highest_capacity_kid_id is None:
        self.logger.error("No capacity exists to add a kid to this DataNode")
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
    if self._height == 1:
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

  def _get_proxy(self):
    if self._height > 2:
      return self._kids.get_proxy()
    else:
      return None

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
      return {kid_id: self._kids[kid_id] for kid_id in self._kids}
    elif message['type'] == 'get_interval':
      return self._interval_json()
    elif message['type'] == 'get_senders':
      if self._importer is None:
        return {}
      else:
        return {self._importer.sender_id: self._importer.sender}
    elif message['type'] == 'get_receivers':
      return {}
    elif message['type'] == 'get_output_state':
      if self._height != 0:
        raise errors.InternalError("Can't get output state for a DataNode with height > 0")
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
    if self._height != 1:
      raise errors.InternalError("Only DataNode instances of height 1 should create kid configs.")

    node_id = ids.new_id('LeafNode_{}'.format(name))
    self.logger.info(
        "Registering a new leaf node config for an internal node. name='{node_name}'",
        extra={
            'data_node_id': self.id,
            'leaf_node_id': node_id,
            'node_name': name
        })

    parent = self.new_handle(node_id)
    return transaction.add_participant_role_to_node_config(
        node_config=messages.io.data_node_config(
            node_id=node_id, parent=parent, variant=self._variant, height=0, leaf_config=self._leaf_config),
        transaction_id=ids.new_id('AddLeafTransaction'),
        participant_typename='AddLeaf',
        args=dict(parent=parent))

  def deliver(self, message, sequence_number, sender_id):
    if self._variant != 'output' or self._height != 0:
      raise errors.InternalError("Only 'output' variant leaf nodes may receive output actions")

    if self._height == 0:
      self._message_rate_tracker.increment(self.linker.now_ms)

    self._leaf.update_current_state(message)

    self.linker.advance_sequence_number()


class RoutingKidsListener(object):
  def __init__(self, node):
    self._node = node
    self._kid_to_address = {node_id: None for node_id in node._kids}

  def start(self):
    for node_id in self._node._kids:
      self._node.send(self._node._kids[node_id], messages.io.routing_start(self._node._domain_name))

  def receive(self, message, sender_id):
    if message['type'] == 'routing_started':
      self._kid_to_address[sender_id] = message['server_address']
      if all(val is not None for val in self._kid_to_address.values()):
        self._node.routing_kids_finished(self._kid_to_address)
      return True

    return False
