# Messages


def increment(amount):
  return { 'type': 'increment', 'amount': amount }

# Nodes

class NodeManager(object):
  '''The class that Nodes dispatch all their requests to send messages or schedule things.'''
  def send(receiver, msg):
    raise RuntimeError("Not Yet Implemented")


class TestNodeManager(NodeManager):
  def __init__(self, test_manager):
    self.test_manager = test_manager


class TestManager(object):
  '''For creating NodeManager instances to use in a local test'''
  def new_node_manager(self):
    return TestNodeManager(test_manager=self)


class NodeInternal(object):
  def receive(self, msg):
    raise RuntimeError("Abstract Superclass")

  def elapse(self, ms):
    raise RuntimeError("Abstract Superclass")


class Monitor(object):
  def send(self, receiver, msg):
    raise RuntimeError("Abstract Superclass")

  def receive(self, msg):
    raise RuntimeError("Abstract Superclass")

class RateMonitorHelper(object):
  NEW_BUCKET_INTERVAL = 1000 # Number of seconds covered by each finished bucket
  LIMIT_PER_INTERVAL = 50 # Once this many counts arrive in a single bucket, it's time to split

  def __init__(self, migration, manager):
    '''
    A manager that watches to see how quickly something is happening, and if
    it is receiving them too fast, it initialtes migration.
    '''
    self._manager = manager

    # A list of dicts {'time': start time in ms, 'messages': number of messages in that bucket}
    self._buckets = []
    self._migration = migration

    self._new_bucket()

  def _new_bucket(self):
    self._buckets.append({ 'time': 0, 'count': 0})

  def send(self, receiver, msg):
    pass

  def increment(self):
    self._buckets[-1]['count'] += 1

  def elapse(self, ms):
    bucket = self._buckets[-1]
    bucket['time'] += ms
    if bucket['time'] > RateMonitorHelper.NEW_BUCKET_INTERVAL:
      self._new_bucket()

    self._check_for_limit()

  def _check_for_limit(self):
    for bucket in self._buckets:
      count_per_bucket_interval = bucket['count']
      if count_per_bucket_interval > RateMonitorHelper.LIMIT_PER_INTERVAL:
        self._manager.migrate(self._migration)

class ReceiveRateMonitor(Monitor):
  def __init__(self, migration, manager):
    self._helper = RateMonitorHelper(migration=migration, manager=manager)

  def send(self, receiver, msg):
    pass

  def receive(self, msg):
    self._helper.increment()

  def elapse(self, ms):
    self._helper.elapse(ms)

class SendRateMonitor(Monitor):
  def __init__(self, migration, manager):
    self._helper = RateMonitorHelper(migration=migration, manager=manager)

  def send(self, receiver, msg):
    self._helper.increment()

  def receive(self, msg):
    pass

  def elapse(self, ms):
    self._helper.elapse(ms)


class SumNode(NodeInternal):
  SEND_INTERVAL_MS = 30 # Number of ms between sends to receivers. 

  def __init__(self, senders, receivers, manager, monitors):
    self._senders = senders
    self._receivers = receivers
    self._manager = manager
    self._monitors = monitors

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

  def receive(self, msg):
    if msg['type'] == 'increment':
      for monitor in self._monitors:
        monitor.receive(msg)
      self._unsent_total += msg['amount']
    elif msg['type'] == 'add_sender':
      self._senders.append(msg['sender'])
    elif msg['type'] == 'add_receiver':
      self._senders.append(msg['receiver'])
    else:
      raise RuntimeError("Unrecognized message {}".format(msg))

  def elapse(self, ms):
    for monitor in self._monitors:
      monitor.elapse(ms)
    self._unsent_time_ms += ms
    if self._unsent_total > 0 and self._unsent_time_ms > SumNode.SEND_INTERVAL:
      self._send_to_all()

  def _send_to_all(self):
    for receiver in self._receivers:
      msg = increment(self._unsent_total)
      self._manager.send(receiver, msg)
      for monitor in self._monitors:
        monitor.send(receiver, msg)
    self._unsent_time_ms = 0
    self._sent_total += self._unsent_total
    self._unsent_total = 0

def run():
  manager = TestManager()
  users_root_node = UsersNode(kids=[])

  sum_node_manager = manager.new_node_manager()
  split_sum_migration = SplitSumMigration()
  sum_root_node = SumNode(
      senders=[users_root_node],
      receivers=[users_root_node],
      manager=sum_node_manager,
      monitors=[
        SendRateMonitor(migration=split_sum_migration, manager=sum_node_manager),
        ReceiveRateMonitor(migration=split_sum_migration, manager=sum_node_manager),
        ],
      )

