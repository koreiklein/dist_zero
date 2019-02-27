from dist_zero import errors, transaction
from .transactions import split_kid, merge_kids, consume_proxy, spawn_kid, bump_height


class Monitor(object):
  def __init__(self, node: 'DataNode'):
    self._node = node

    self._time_since_no_consumable_proxy = 0

    # To limit excessive warnings regarding being at low capacity.
    self._warned_low_capacity = False

    # Maps pairs of mergeable nodes to the time since they became mergeable
    self._mergeable_pair_to_time_since_mergeable = {}
    self._mergeable_node_ids = set() # All the ids used as keys in self._mergeable_pair_to_time_since_mergeable

  def check_limits(self, ms):
    self._will_check_limits = True
    self._node.start_transaction_eventually(CheckLimitsTransaction(self, ms))

  def _check_limits_inside_transaction(self, ms):
    if self._node._updated_summary or self._node._height == 1:
      self._node._send_kid_summary()
      self._node._updated_summary = False
    self._check_for_low_capacity()
    self._check_for_mergeable_kids(ms)
    self._check_for_consumable_proxy(ms)
    self._will_check_limits = False

  def out_of_capacity(self):
    total_kid_capacity = sum(
        self._node._kid_capacity_limit - kid_summary['size'] for kid_summary in self._node._kids.summaries.values())

    if total_kid_capacity <= self._node.system_config['TOTAL_KID_CAPACITY_TRIGGER']:
      return True

    return False

  def _check_for_low_capacity(self):
    '''Check whether the total capacity of this node's kids is too low.'''
    if self._node._height <= 1:
      return # Nodes of height <= 1 never address low capacity themselves

    if set(self._node._kids.summaries.keys()) < set(self._node._kids):
      return # Wait till we have summaries for all our kids

    if self.out_of_capacity():
      if len(self._node._kids) < self._node.system_config['DATA_NODE_KIDS_LIMIT']:
        self._spawn_kid()
      else:
        if self._node._parent is None:
          self._node.start_transaction_eventually(bump_height.BumpHeight())
        else:
          self._node._send_kid_summary()
          if not self._warned_low_capacity:
            self._warned_low_capacity = True
            self._node.logger.warning(
                "nonroot DataNode instance had too little capacity and no room to spawn more kids. "
                "Capacity is remaining low and is not being increased.")
    else:
      self._warned_low_capacity = False

  def _spawn_kid(self):
    best_kid_id = None
    fitness = 0
    for kid_id, summary in self._node._kids.summaries.items():
      kid_fitness = summary['n_kids'] # The more kids, the fitter for splitting.
      if kid_fitness >= fitness:
        fitness = kid_fitness
        best_kid_id = kid_id

    if best_kid_id is None:
      raise errors.InternalError("Monitor should not attempt to spawn kids when no suitable kids exist.")
    if fitness <= 1:
      # _spawn_kid should only be called when we are low on capacaity.  That should imply that there are many
      # kids which themselves have more than one kid.
      raise errors.InternalError("Monitor should not attempt to spawn kids when no kid has more than one kid.")

    self._node.start_transaction_eventually(split_kid.SplitKid(kid_id=best_kid_id))

  def _check_for_consumable_proxy(self, ms):
    TIME_TO_WAIT_BEFORE_CONSUME_PROXY_MS = 4 * 1000

    if self._node._parent is None:
      if self._node._get_proxy():
        self._time_since_no_consumable_proxy += ms
        if self._time_since_no_consumable_proxy >= TIME_TO_WAIT_BEFORE_CONSUME_PROXY_MS:
          self._node.start_transaction_eventually(consume_proxy.ConsumeProxy())
      else:
        self._time_since_no_consumable_proxy = 0

  @property
  def is_watching(self):
    return bool(self._mergeable_pair_to_time_since_mergeable) or self._time_since_no_consumable_proxy > 0

  def _watch_pair_for_merge(self, pair):
    self._mergeable_pair_to_time_since_mergeable[pair] = 0
    self._mergeable_node_ids.add(pair[0])
    self._mergeable_node_ids.add(pair[1])

  def _unwatch_pair_for_merge(self, pair):
    self._mergeable_pair_to_time_since_mergeable.pop(pair)
    self._mergeable_node_ids.remove(pair[0])
    self._mergeable_node_ids.remove(pair[1])

  def _check_for_mergeable_kids(self, ms):
    '''Check whether any two kids should be merged.'''
    TIME_TO_WAIT_BEFORE_KID_MERGE_MS = 2 * 1000

    if self._node._height > 1:
      for pair in list(self._mergeable_pair_to_time_since_mergeable.keys()):
        # Add some time to how long it has waited
        self._mergeable_pair_to_time_since_mergeable[pair] += ms

        if not self._node._kids_are_mergeable(*pair):
          self._unwatch_pair_for_merge(pair) # No longer mergeable.  Forget about them
        elif self._mergeable_pair_to_time_since_mergeable[pair] >= TIME_TO_WAIT_BEFORE_KID_MERGE_MS:
          # Mergeable!  Request a transaction to attempt to merge them eventually, it should succeed if they're still
          # mergeable when the transaction starts.
          self._unwatch_pair_for_merge(pair)
          self._node.start_transaction_eventually(merge_kids.MergeKids(*pair))

      best_pair = self._node._best_mergeable_kids(self._mergeable_node_ids)
      while best_pair is not None:
        self._watch_pair_for_merge(best_pair)
        best_pair = self._node._best_mergeable_kids(self._mergeable_node_ids)


class CheckLimitsTransaction(transaction.OriginatorRole):
  def __init__(self, monitor, ms):
    self._monitor = monitor
    self._ms = ms

  @property
  def log_starts_and_stops(self):
    return False

  async def run(self, controller: 'TransactionRoleController'):
    controller.logger.debug("Running CheckLimitsTransaction")
    self._monitor._check_limits_inside_transaction(self._ms)
