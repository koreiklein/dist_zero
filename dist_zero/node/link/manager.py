import blist

from dist_zero import intervals


class LinkGraphManager(object):
  '''
  Manager class for creating and maintaining the subgraph of link nodes associated with a single parent `LinkNode`
  '''

  def __init__(self, source_object_intervals, target_object_intervals, constraints):
    '''
    Initialize a manager to connect source objects to target objects.
    Upon initialization, the `LinkGraphManager` will calculate a potentially very large number of
    `InternalBlock` instances to insert between the sources and targets.

    :param list[tuple[object,tuple[float,float]]] source_object_intervals: List of triplets (source, interval)
      where source is any python object to identify that source, and interval is a pair
      (start, width) giving the start point and width of the interval that source should manage.
    :param list[tuple[object,tuple[float,float]]] target_object_intervals: List of triplets for targets.
    :param constraints: Constraints on what kind of graphs are allowed.
    :type constraints: `Constraints`
    '''
    self._constraints = constraints

    self._source_object_to_block = {
        value: SourceBlock(value=value, start=start, width=width)
        for value, (start, width) in source_object_intervals
    }
    self._target_object_to_block = {
        value: TargetBlock(value=value, start=start, width=width)
        for value, (start, width) in target_object_intervals
    }

    self._queue = blist.blist()
    self._source_block_to_updaters = {src: {} for src in self._source_object_to_block.values()}
    self._target_block_to_updaters = {tgt: {} for tgt in self._target_object_to_block.values()}

    center = self._new_block(
        x_start=intervals.Min,
        x_stop=intervals.Max,
        y_start=intervals.Min,
        y_stop=intervals.Max,
    )

    for src in self._source_block_to_updaters:
      _connect(src, center)
    for tgt in self._target_block_to_updaters:
      _connect(center, tgt)

    self._queue.append(center)
    self._flush_queue() # Generates all the other blocks

  def source_block(self, value):
    return self._source_object_to_block[value]

  def target_block(self, value):
    return self._target_object_to_block[value]

  def _remove_block(self, block):
    '''
    Remove a block from those above and below it; remove its updaters, and mark it as removed.
    Also, put affected blocks into the self._queue.
    This method does not do any of the cleanup associated with removing source or target blocks.
    '''
    for x in block.above[:]:
      self._queue.append(x)
      _disconnect(block, x)
    for x in block.below[:]:
      self._queue.append(x)
      _disconnect(x, block)
    block.is_removed = True

    empty = {}
    self._source_block_to_updaters.get(block.x_start, empty).pop(block, None)
    self._source_block_to_updaters.get(block.x_stop, empty).pop(block, None)
    self._target_block_to_updaters.get(block.y_start, empty).pop(block, None)
    self._target_block_to_updaters.get(block.y_stop, empty).pop(block, None)

  @staticmethod
  def block_rectangle(block, limits=(intervals.Min, intervals.Max, intervals.Min, intervals.Max)):
    x_min, x_max, y_min, y_max = limits
    if block.is_source:
      return (block.start, block.stop), (y_min, y_max)
    elif block.is_target:
      return (x_min, x_max), (block.start, block.stop)
    else:
      return (block.x_start.start, block.x_stop.start), (block.y_start.start, block.y_stop.start)

  def split_src(self, source_value, new_source_value, new_width):
    '''
    Split the source block identified by ``source_value`` in two, allocating the rightmost ``new_width`` to
    the new block.

    :param object source_value: The source
    :param object new_source_value: The python object to use as the new source
    :param float new_width: The amount of space on the right of ``source_value`` to allocate to ``new_source_value``
    '''
    source = self._source_object_to_block[source_value]
    new_source = self._source_object_to_block[new_source_value] = SourceBlock(
        value=new_source_value, start=source.stop - new_width, width=new_width)
    self._source_block_to_updaters[new_source] = {}

    for x in source.above:
      _connect(new_source, x)
      self._queue.append(x)

    self._flush_queue()

    source.width -= new_width

  def split_tgt(self, target_value, new_target_value, new_width):
    '''
    Split the target block identified by ``target_value`` in two, allocating the rightmost ``new_width`` to
    the new block.

    :param object target_value: The target
    :param object new_target_value: The python object to use as the new target
    :param float new_width: The amount of space on the right of ``target_value`` to allocate to ``new_target_value``
    '''
    target = self._target_object_to_block[target_value]
    new_target = self._target_object_to_block[new_target_value] = TargetBlock(
        value=new_target_value, start=target.stop - new_width, width=new_width)
    self._target_block_to_updaters[new_target] = {}

    for x in target.below:
      _connect(x, new_target)
      self._queue.append(x)

    self._flush_queue()

    target.width -= new_width

  def merge_src(self, left, right):
    '''
    Merge the ``left`` source into the ``right`` source. 
    This operation removes the ``left`` target and grows the size of the ``right`` target.

    :param object left: A source
    :param object right: The source immediately after ``left``
    '''
    left = self._source_object_to_block.pop(left)
    right = self._source_object_to_block[right]
    self._remove_block(left)
    self._queue.extend(right.above)
    self._flush_queue()

    # Do these updates at the end to keep the sortedlist objects from getting into a bad state
    # during the removal of blocks
    right.start -= left.width
    right.width += left.width
    for updater in self._source_block_to_updaters.pop(left).values():
      updater(right)

  def merge_tgt(self, left, right):
    '''
    Merge the ``left`` target into the ``right`` target.
    This operation removes the ``left`` target and grows the size of the ``right`` target.

    :param object left: A target
    :param object right: The target immediately after ``left``
    '''
    left = self._target_object_to_block.pop(left)
    right = self._target_object_to_block[right]
    self._remove_block(left)
    self._queue.extend(right.below)
    self._flush_queue()

    # Do these updates at the end to keep the sortedlist objects from getting into a bad state
    # during the removal of blocks
    right.start -= left.width
    right.width += left.width
    for updater in self._target_block_to_updaters.pop(left).values():
      updater(right)

  def internal_blocks(self):
    '''
    :return: the set of blocks internal to this `LinkGraphManager`
      (i.e. blocks that are neither source not target blocks).
    :rtype: set[`InternalBlock`]
    '''
    result = set()
    queue = list(self._source_object_to_block.values())
    while queue:
      block = queue.pop()
      if not block.is_target and block not in result:
        result.add(block)
        queue.extend(block.above)

    return result

  def layers(self):
    '''
    Return a list of sets of blocks.  Each block between the source and target blocks will occur
    exactly once in the result.
    The `LinkGraphManager` class does not have a formal semantics for what it means to be in a layer,
    so the allocation of blocks between layers should follow some heuristic.

    If you're looking to iterate over the blocks computed by `LinkGraphManager`, `internal_blocks` is a better method.
    '''
    # Current algorithm: Each block is given the layer corresponding to the minimum path length between
    # it and any source node.
    result = [set(self._source_block_to_updaters)]
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
    return min(block.start for block in self._source_block_to_updaters)

  def x_max(self):
    return min(block.stop for block in self._source_block_to_updaters)

  def y_min(self):
    return min(block.start for block in self._target_block_to_updaters)

  def y_max(self):
    return min(block.stop for block in self._target_block_to_updaters)

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
    if not block.is_removed and not block.is_target and not block.is_source:
      if not block.below or not block.above:
        # This block has an area of 0 and should be removed
        self._remove_block(block)
      elif self._overloaded(block):
        if not self._try_split_x_or_y(block):
          self._split_z(block)

  def _try_split_x(self, block):
    if any(len(x.below) >= self._constraints.max_below for x in block.above if x.is_target):
      return False # Can't split block on x if an above target block has already maxed out its connections
    elif len(block.below) <= 1:
      return False # Not enough space to split
    else:
      self._split_x(block)

      return True

  def _try_split_y(self, block):
    if any(len(x.above) >= self._constraints.max_above for x in block.below if x.is_source):
      return False # Can't split block on y if a below source block has already maxed out its connections
    elif len(block.above) <= 1:
      return False # Not enough space to split
    else:
      self._split_y(block)

      return True

  def _x_split_index(self, block):
    '''Index into block.below at which to split the block in two in the x dimension'''
    # FIXME(KK): Use mass here to pick a better index
    return len(block.below) // 2

  def _y_split_index(self, block):
    '''Index into block.above at which to split the block in two in the y dimension'''
    # FIXME(KK): Use mass here to pick a better index
    return len(block.above) // 2

  def _new_block(self, x_start, x_stop, y_start, y_stop):
    result = InternalBlock()

    def _set_x_start(value):
      if not result.is_removed:
        result.x_start = value
        if value != intervals.Min:
          self._source_block_to_updaters[value][result] = _set_x_start

    def _set_x_stop(value):
      if not result.is_removed:
        result.x_stop = value
        if value != intervals.Max:
          self._source_block_to_updaters[value][result] = _set_x_stop

    def _set_y_start(value):
      if not result.is_removed:
        result.y_start = value
        if value != intervals.Min:
          self._target_block_to_updaters[value][result] = _set_y_start

    def _set_y_stop(value):
      if not result.is_removed:
        result.y_stop = value
        if value != intervals.Max:
          self._target_block_to_updaters[value][result] = _set_y_stop

    _set_x_start(x_start)
    _set_x_stop(x_stop)
    _set_y_start(y_start)
    _set_y_stop(y_stop)

    return result

  def _split_x(self, block):
    index = self._x_split_index(block)
    new = self._new_block(
        x_start=block.below[index].x_start, x_stop=block.x_stop, y_start=block.y_start, y_stop=block.y_stop)
    block.x_stop = block.below[index].x_start

    for x in block.below[index:]:
      _disconnect(x, block)
      _connect(x, new)
    for x in block.above:
      _connect(new, x)
      self._queue.append(x)

    self._queue.append(block)
    self._queue.append(new)

  def _split_y(self, block):
    index = self._y_split_index(block)
    new = self._new_block(
        x_start=block.x_start, x_stop=block.x_stop, y_start=block.above[index].y_start, y_stop=block.y_stop)
    block.y_stop = block.above[index].y_start

    for x in block.above[index:]:
      _disconnect(block, x)
      _connect(new, x)
    for x in block.below:
      _connect(x, new)
      self._queue.append(x)

    self._queue.append(block)
    self._queue.append(new)

  def _split_z(self, block):
    new = self._new_block(x_start=block.x_start, x_stop=block.x_stop, y_start=block.y_start, y_stop=block.y_stop)
    for x in block.above[:]:
      _disconnect(block, x)
      _connect(new, x)
    _connect(block, new)
    self._queue.append(block)
    self._queue.append(new)


class Constraints(object):
  '''Constraints on various properties of the graph.'''

  def __init__(self, max_above, max_below, max_connections=None):
    self.max_above = max_above
    '''Maxmimum number of `Blocks <Block>` allowed to sit above any block.'''
    self.max_below = max_below
    '''Maxmimum number of `Blocks <Block>` allowed to sit below any block.'''
    self.max_connections = max_connections if max_connections is not None else self.max_above + self.max_below
    '''Maxmimum number of `Blocks <Block>` allowed to sit adjacent (above or below) any block.'''


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
    self.is_removed = False

  def __repr__(self):
    return f"{self.__class__.__name__}(x_start={self.x_start.start}, x_stop={self.x_stop.start}, y_start={self.y_start.start}, y_stop={self.y_stop.start})"

  @property
  def is_source(self):
    return False

  @property
  def is_target(self):
    return False


class InternalBlock(Block):
  '''
  Blocks created and maintained by this manager.
  All blocks that do not correspond to a source or a target will be `InternalBlock` instances.
  '''
  pass


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
    self.y_start = intervals.Min
    self.y_stop = intervals.Max
    super(SourceBlock, self).__init__(*args, **kwargs)

  def __repr__(self):
    return f"{self.__class__.__name__}(x_start={self.start}, x_stop={self.stop}, y_start={self.y_start.start}, y_stop={self.y_stop.start})"

  @property
  def is_source(self):
    return True


class TargetBlock(SourceOrTargetBlock):
  '''Target block'''

  def __init__(self, *args, **kwargs):
    self.x_start = intervals.Min
    self.x_stop = intervals.Max
    self.y_start = self
    self.y_stop = self
    super(TargetBlock, self).__init__(*args, **kwargs)

  def __repr__(self):
    return f"{self.__class__.__name__}(x_start={self.start}, x_stop={self.stop}, y_start={self.start}, y_stop={self.stop})"

  @property
  def is_target(self):
    return True
