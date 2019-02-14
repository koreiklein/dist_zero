import blist


class LinkGraphManager(object):
  '''
  Manager class for creating and maintaining the subgraph of link nodes associated with a single parent `LinkNode`
  '''

  def __init__(self, source_object_intervals, target_object_intervals, constraints):
    self._constraints = constraints

    self._source_object_to_block = {
        value: SourceBlock(value=value, start=start, width=width)
        for value, (start, width) in source_object_intervals
    }
    self._target_object_to_block = {
        value: TargetBlock(value=value, start=start, width=width)
        for value, (start, width) in target_object_intervals
    }

    self._source_blocks = set(self._source_object_to_block.values())
    self._target_blocks = set(self._target_object_to_block.values())

    self._queue = blist.blist()

    center = InternalBlock(
        x_start=MinusInf,
        x_stop=Inf,
        y_start=MinusInf,
        y_stop=Inf,
        # FIXME(KK): Double check whether it's proper to use infinities or the extreme provided srcs and tgts
        #x_start=self._source_object_to_block[source_object_intervals[0][0]],
        #x_stop=self._source_object_to_block[source_object_intervals[-1][0]],
        #y_start=self._target_object_to_block[target_object_intervals[0][0]],
        #y_stop=self._target_object_to_block[target_object_intervals[-1][0]],
    )

    for src in self._source_blocks:
      _connect(src, center)
    for tgt in self._target_blocks:
      _connect(center, tgt)

    self._queue.append(center)
    self._flush_queue() # Generates all the other blocks

  def layers(self):
    '''
    Return a list of sets of blocks.  Each block between the source and target blocks will occur
    exactly once in the result.
    The `LinkGraphManager` class does not have a formal semantics for what it means to be in a layer,
    so the allocation of blocks between layers should follow some heuristic.
    '''
    # Current algorithm: Each block is given the layer corresponding to the minimum path length between
    # it and any source node.
    result = [set(self._source_blocks)]
    seen = set()
    while True:
      next_layer = set()
      for x in result[-1]:
        if x not in seen:
          seen.add(x)
          if not x.is_target:
            next_layer.update(x.above)
      if next_layer:
        result.append(next_layer)
      else:
        return result

  def x_min(self):
    return min(block.start for block in self._source_blocks)

  def x_max(self):
    return min(block.stop for block in self._source_blocks)

  def y_min(self):
    return min(block.start for block in self._target_blocks)

  def y_max(self):
    return min(block.stop for block in self._target_blocks)

  def _flush_queue(self):
    while self._queue:
      self._check_block_for_constraints(self._queue.pop(0))

  def _overloaded(self, block):
    # FIXME(KK): Write up notes about how we calculate load on a block and implement them
    return len(block.above) > self._constraints.max_above or \
        len(block.below) > self._constraints.max_below or \
        len(block.above) + len(block.below) > self._constraints.max_connections

  def _try_split_x_or_y(self, block):
    if len(block.above) > self._constraints.max_above or (len(block.above) > len(block.below)):
      return self._try_split_y(block) or self._try_split_x(block)
    else:
      return self._try_split_x(block) or self._try_split_y(block)

  def _check_block_for_constraints(self, block):
    if self._overloaded(block):
      if not self._try_split_x_or_y(block):
        new = self._split_z(block)
        self._queue.append(block)
        self._queue.append(new)

  def _try_split_x(self, block):
    if any(len(x.below) >= self._constraints.max_below for x in block.above if x.is_target):
      return False # Can't split block on x if an above target block has already maxed out its connections
    elif len(block.below) <= 1:
      return False # Not enough space to split
    else:
      new = self._split_x(block)
      self._queue.append(block)
      self._queue.append(new)

      return True

  def _try_split_y(self, block):
    if any(len(x.above) >= self._constraints.max_above for x in block.below if x.is_source):
      return False # Can't split block on y if a below source block has already maxed out its connections
    elif len(block.above) <= 1:
      return False # Not enough space to split
    else:
      new = self._split_y(block)
      self._queue.append(block)
      self._queue.append(new)

      return True

  def _x_split_index(self, block):
    '''Index into block.below at which to split the block in two in the x dimension'''
    # FIXME(KK): Use mass here to pick a better index
    return len(block.below) // 2

  def _y_split_index(self, block):
    '''Index into block.above at which to split the block in two in the y dimension'''
    # FIXME(KK): Use mass here to pick a better index
    return len(block.above) // 2

  def _split_x(self, block):
    index = self._x_split_index(block)
    new = InternalBlock(
        x_start=block.below[index].x_start, x_stop=block.x_stop, y_start=block.y_start, y_stop=block.y_stop)
    block.x_stop = block.below[index - 1].x_stop

    for x in block.below[index:]:
      _disconnect(x, block)
      _connect(x, new)
    for x in block.above:
      _connect(new, x)

    return new

  def _split_y(self, block):
    index = self._y_split_index(block)
    new = InternalBlock(
        x_start=block.x_start, x_stop=block.x_stop, y_start=block.above[index].y_start, y_stop=block.y_stop)
    block.y_stop = block.above[index - 1].y_stop

    for x in block.above[index:]:
      _disconnect(block, x)
      _connect(new, x)
    for x in block.below:
      _connect(x, new)

    return new

  def _split_z(self, block):
    new = InternalBlock(x_start=block.x_start, x_stop=block.x_stop, y_start=block.y_start, y_stop=block.y_stop)
    for x in block.above[:]:
      _disconnect(block, x)
      _connect(new, x)
    _connect(block, new)
    return new


class Constraints(object):
  '''Constraints on various properties of the graph.'''

  def __init__(self, max_above, max_below, max_connections=None):
    self.max_above = max_above
    self.max_below = max_below
    self.max_connections = max_connections if max_connections is not None else self.max_above + self.max_below


_by_x = lambda block: block.x_start
_by_y = lambda block: block.y_start


def _connect(left, right):
  left.above.add(right)
  right.below.add(left)


def _disconnect(left, right):
  left.above.remove(right)
  right.below.remove(left)


class Block(object):
  '''One of the nodes in the graph managed by `LinkGraphManager`'''

  def __init__(self):
    self.below = blist.sortedlist([], key=_by_x)
    self.above = blist.sortedlist([], key=_by_y)

  @property
  def is_source(self):
    return False

  @property
  def is_target(self):
    return False


class InternalBlock(Block):
  '''Blocks created and maintained by this manager.'''

  def __init__(self, x_start, x_stop, y_start, y_stop):
    self.x_start = x_start
    self.x_stop = x_stop
    self.y_start = y_start
    self.y_stop = y_stop

    super(InternalBlock, self).__init__()


class SourceOrTargetBlock(Block):
  '''Superclass for source or target blocks.'''

  def __init__(self, value, start, width):
    self.value = value
    self.start = start
    self.width = width
    super(SourceOrTargetBlock, self).__init__()

  @property
  def stop(self):
    return self.start + self.width

  def __le__(self, other):
    if other.__class__ == self.__class__:
      return self.start <= other.start
    else:
      return other >= self

  def __lt__(self, other):
    if other.__class__ == self.__class__:
      return self.start < other.start
    else:
      return other > self

  def __ge__(self, other):
    if other.__class__ == self.__class__:
      return self.start >= other.start
    else:
      return other <= self

  def __gt__(self, other):
    if other.__class__ == self.__class__:
      return self.start > other.start
    else:
      return other < self


class SourceBlock(SourceOrTargetBlock):
  '''Source block'''

  def __init__(self, *args, **kwargs):
    self.x_start = self
    self.x_stop = self
    self.y_start = MinusInf
    self.y_stop = Inf
    super(SourceBlock, self).__init__(*args, **kwargs)

  @property
  def is_source(self):
    return True


class TargetBlock(SourceOrTargetBlock):
  '''Target block'''

  def __init__(self, *args, **kwargs):
    self.x_start = MinusInf
    self.x_stop = Inf
    self.y_start = self
    self.y_stop = self
    super(TargetBlock, self).__init__(*args, **kwargs)

  @property
  def is_target(self):
    return True


class _Inf(object):
  def __le__(self, other):
    return other == Inf

  def __ge__(self, other):
    return True

  def __lt__(self, other):
    return False

  def __gt__(self, other):
    return other != Inf


class _MinusInf(object):
  def __le__(self, other):
    return True

  def __ge__(self, other):
    return other == MinusInf

  def __lt__(self, other):
    return other != MinusInf

  def __gt__(self, other):
    return False


Inf = _Inf()
'''Inifinity.  A special object greater than everything else.'''
MinusInf = _MinusInf()
'''Minus Inifinity.  A special object less than everything else.'''
