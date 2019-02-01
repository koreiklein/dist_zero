from dist_zero import errors
from .transactions import merge_kids, consume_proxy, spawn_kid, bump_height


class Monitor(object):
  def __init__(self, node: 'DataNode'):
    self._node = node

    self._time_since_no_mergable_kids_ms = 0
    self._time_since_no_consumable_proxy = 0

    # To limit excessive warnings regarding being at low capacity.
    self._warned_low_capacity = False

  def _check_limits(self, ms):
    if self._node._updated_summary or self._node._height == 1:
      self._node._send_kid_summary()
      self._node._updated_summary = False
    self._check_for_kid_limits()
    self._check_for_mergeable_kids(ms)
    self._check_for_consumable_proxy(ms)

  def _check_for_low_capacity(self):
    '''Check whether the total capacity of this node's kids is too low.'''
    if set(self._node._kid_summaries.keys()) < set(self._node._kids.keys()):
      return # Wait till we have summaries for all our kids

    total_kid_capacity = sum(
        self._node._kid_capacity_limit - kid_summary['size'] for kid_summary in self._node._kid_summaries.values())

    if total_kid_capacity <= self._node.system_config['TOTAL_KID_CAPACITY_TRIGGER']:
      if len(self._node._kids) < self._node.system_config['DATA_NODE_KIDS_LIMIT']:
        self._spawn_kid()
      else:
        if self._node._parent is None:
          self._node.start_transaction_eventually(bump_height.BumpHeight())
        else:
          if not self._warned_low_capacity:
            self._warned_low_capacity = True
            self._node.logger.warning(
                "nonroot DataNode instance had too little capacity and no room to spawn more kids. "
                "Capacity is remaining low and is not being increased.")
    else:
      self._warned_low_capacity = False

  def _spawn_kid(self):
    self._node.start_transaction_eventually(spawn_kid.SpawnKid())

  def _check_for_kid_limits(self):
    '''In case the kids of self are hitting any limits, address them.'''
    if self._node._height > 1:
      self._check_for_low_capacity()

  def _check_for_consumable_proxy(self, ms):
    TIME_TO_WAIT_BEFORE_CONSUME_PROXY_MS = 4 * 1000

    if self._node._parent is None:
      if self._node._get_proxy():
        self._time_since_no_consumable_proxy += ms
        if self._time_since_no_consumable_proxy >= TIME_TO_WAIT_BEFORE_CONSUME_PROXY_MS:
          self._node.start_transaction_eventually(consume_proxy.ConsumeProxy())
      else:
        self._time_since_no_consumable_proxy = 0

  def _check_for_mergeable_kids(self, ms):
    '''Check whether any two kids should be merged.'''
    TIME_TO_WAIT_BEFORE_KID_MERGE_MS = 2 * 1000

    if self._node._height > 1:
      best_pair = self._node._best_mergeable_kids()
      if best_pair is None:
        self._time_since_no_mergable_kids_ms = 0
      else:
        self._time_since_no_mergable_kids_ms += ms

        if self._time_since_no_mergable_kids_ms >= TIME_TO_WAIT_BEFORE_KID_MERGE_MS:
          self._time_since_no_mergable_kids_ms = 0
          self._node.start_transaction_eventually(merge_kids.MergeKids(*best_pair))
