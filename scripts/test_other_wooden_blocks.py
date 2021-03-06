import blist

from collections import defaultdict

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as colors

PALLETTE = list(colors.TABLEAU_COLORS.values())

epsilon = 0.0001


class SourceOrTarget(object):
  def __init__(self, value, start, width):
    self.value = value
    self.start = start
    self.width = width

  def __le__(self, other):
    return self.start <= other.start

  def __lt__(self, other):
    return self.start < other.start

  def __ge__(self, other):
    return self.start >= other.start

  def __gt__(self, other):
    return self.start > other.start


class Source(SourceOrTarget):
  pass


class Target(SourceOrTarget):
  pass


class Coords(object):
  def __init__(self, src_start, src_width, tgt_start, tgt_width):
    self.src_start = src_start
    self.src_width = src_width
    self.tgt_start = tgt_start
    self.tgt_width = tgt_width

  def aspect(self):
    return self.src_width / self.tgt_width

  @property
  def src_stop(self):
    return self.src_start + self.src_width

  @property
  def tgt_stop(self):
    return self.tgt_start + self.tgt_width

  def plot(self, color):
    x = [self.src_start, self.src_stop, self.src_stop, self.src_start]
    y = [self.tgt_start, self.tgt_start, self.tgt_stop, self.tgt_stop]
    plt.plot(x, y, color, alpha=0.8)
    plt.fill(x, y, color, alpha=0.4)

  def copy(self):
    return Coords(
        src_start=self.src_start, src_width=self.src_width, tgt_start=self.tgt_start, tgt_width=self.tgt_width)

  def __repr__(self):
    return f"src:({self.src_start},{self.src_width}), tgt:[{self.tgt_start},{self.tgt_width})"


class Constraints(object):
  def __init__(self, max_above, max_below, max_mass, max_connections=None):
    self.max_above = max_above
    self.max_below = max_below
    self.max_mass = max_mass
    self.max_connections = max_connections if max_connections is not None else self.max_above + self.max_below


class LinkGraphManager(object):
  def __init__(self, sources, targets, source_width, target_width, source_interval, target_interval, constraints):
    self._n_new_blocks = 0
    self._sources = set(sources)
    self._targets = set(targets)

    self._constraints = constraints

    self._above = {} # Map each non target block to a sortedlist of blocks above it, sorted by target interval
    self._below = {} # Map each non source block to a sortedlist of blocks below it, sorted by source interval

    src_min, src_total_width = source_interval
    tgt_min, tgt_total_width = target_interval

    self._src_min = src_min
    self._src_total_width = src_total_width
    self._tgt_min = tgt_min
    self._tgt_total_width = tgt_total_width

    self._coords = {}
    self._queue = blist.blist()

    for src in self._sources:
      start, width = source_width[src]
      self._coords[src] = Coords(src_start=start, src_width=width, tgt_start=tgt_min, tgt_width=tgt_total_width)

    for tgt in self._targets:
      start, width = target_width[tgt]
      self._coords[tgt] = Coords(src_start=src_min, src_width=src_total_width, tgt_start=start, tgt_width=width)

  def layers(self):
    cur_layer = set(self._sources)
    remaining_targets = set(self._targets)
    result = [cur_layer]
    processed = set()
    while remaining_targets:
      next_layer = set()
      for x in cur_layer:
        if x in processed:
          continue
        else:
          processed.add(x)
        if x in self._targets:
          if x in remaining_targets:
            remaining_targets.remove(x)
        else:
          for y in self._above[x]:
            next_layer.add(y)
            if y in remaining_targets:
              remaining_targets.remove(y)
      cur_layer = next_layer
      result.append(cur_layer)

    return result

  def path(self, source, target):
    coords = self._coords[target]
    current = source
    while True:
      yield current
      if current in self._targets:
        if current == target:
          return
        else:
          raise RuntimeError("Did not find a path to target. Reached end at the wrong place.")
      else:
        options = self._above[current]
        i = options.bisect_left(target)
        if i < len(options) and abs(self._coords[options[i]].tgt_start - self._coords[target].tgt_start) < epsilon:
          current = options[i]
        else:
          if i == 0:
            raise RuntimeError("Did not find a path to target.  Next step was to the left of the available next steps.")
          else:
            current = options[i - 1]

  def fill_in(self):
    center = self._new_block()

    self._coords[center] = Coords(
        src_start=self._src_min,
        src_width=self._src_total_width,
        tgt_start=self._tgt_min,
        tgt_width=self._tgt_total_width)

    self._below[center] = blist.sortedlist(self._sources, key=self._by_source)
    self._above[center] = blist.sortedlist(self._targets, key=self._by_target)

    for src in self._sources:
      self._above[src] = blist.sortedlist([center], key=self._by_target)

    for tgt in self._targets:
      self._below[tgt] = blist.sortedlist([center], key=self._by_source)

    self._queue.append(center)

    self._flush_queue()

  def _flush_queue(self):
    while self._queue:
      self._check_block_for_constraints(self._queue.pop(0))

  def _check_block_for_constraints(self, block):
    nabove, nbelow = len(self._above[block]), len(self._below[block])
    if nabove > self._constraints.max_above and self._try_split_y(block):
      return
    elif nbelow > self._constraints.max_below and self._try_split_x(block):
      return
    elif self._mass(
        self._coords[block]) > self._constraints.max_mass or nabove + nbelow > self._constraints.max_connections:
      if (self._try_split_x(block) or self._try_split_y(block)) if nbelow >= nabove else \
          (self._try_split_y(block) or self._try_split_x(block)):
        return

      # Neither the x nor the y split worked (certainly both were tried)
      if nabove <= 1 and nbelow <= 1:
        raise RuntimeError(
            "Impossible! The messages from a unique sender to a unique receiver are already too high in mass.")
      else:
        new = self._split_z(block)
        self._queue.append(block)
        self._queue.append(new)

  def _validate(self):
    for x, vals in self._above.items():
      for y in vals:
        if self._coords[x].aspect() > self._coords[y].aspect():
          import ipdb
          ipdb.set_trace()

  def _try_split_x(self, block):
    if any(len(self._below[x]) >= self._constraints.max_below for x in self._above[block] if x in self._targets):
      return False # Can't split block if an above target node has already maxed out its connections
    elif len(self._below[block]) <= 1:
      return False # Not enough space to split
    else:
      new = self._split_x(block)
      self._queue.append(block)
      self._queue.append(new)

      return True

  def _try_split_y(self, block):
    if any(len(self._above[x]) >= self._constraints.max_above for x in self._below[block] if x in self._sources):
      return False # Can't split block if a below source node has already maxed out its connections
    elif len(self._above[block]) <= 1:
      return False # Not enough space to split
    else:
      new = self._split_y(block)
      self._queue.append(block)
      self._queue.append(new)

      return True

  def merge_src(self, left_source, right_source):
    '''Remove the right_source and grow the left_source to include its range'''
    new_width = self._coords[right_source].src_width

    self._grow_x_width(left_source, new_width)
    self._shrink_x_width(right_source, new_width)

    self._remove_block(right_source)

    self._sources.remove(right_source)

  def merge_tgt(self, left_target, right_target):
    '''Remove the right_target and grow the left_target to include its range'''
    new_width = self._coords[right_target].tgt_width

    self._grow_y_width(left_target, new_width)
    self._shrink_y_width(right_target, new_width)

    self._remove_block(right_target)
    self._targets.remove(right_target)

  def _grow_x_width(self, block, width):
    '''Add extra width to the right of the rectangle for this block'''
    for parent in self._above[block]:
      if self._below[parent][-1] == block:
        self._grow_x_width(parent, width)

    self._coords[block].src_width += width

  def _grow_y_width(self, block, width):
    '''Add extra width to the top of the rectangle for this block'''
    for child in self._below[block]:
      if self._above[child][-1] == block:
        self._grow_y_width(child, width)

    self._coords[block].tgt_width += width

  def _shrink_x_width(self, block, width):
    '''Remove width from the left of the rectangle for this block'''
    new_width = self._coords[block].src_width - width

    for parent in self._above[block]:
      if self._below[parent][0] == block:
        self._shrink_x_width(parent, width)

    self._coords[block].src_start += width
    self._coords[block].src_width = new_width

  def _shrink_y_width(self, block, width):
    '''Remove width from the bottom of the rectangle for this block'''
    new_width = self._coords[block].tgt_width - width

    for child in self._below[block]:
      if self._above[child][0] == block:
        self._shrink_y_width(child, width)

    self._coords[block].tgt_start += width
    self._coords[block].tgt_width = new_width

  def _remove_block(self, block):
    if block not in self._targets:
      for parent in self._above.pop(block):
        if block not in self._below[parent]:
          import ipdb
          ipdb.set_trace()
        self._below[parent].remove(block)
    if block not in self._sources:
      for child in self._below.pop(block):
        if block not in self._above[child]:
          import ipdb
          ipdb.set_trace()
        self._above[child].remove(block)

    self._coords.pop(block)

  def split_src(self, source, new_source, new_width):
    self._sources.add(new_source)

    coords = self._coords[source]

    self._coords[new_source] = Coords(
        src_start=coords.src_stop - new_width,
        src_width=new_width,
        tgt_start=coords.tgt_start,
        tgt_width=coords.tgt_width,
    )
    coords.src_width -= new_width
    above = self._above[source]
    self._above[new_source] = above[:]
    for x in above:
      self._below[x].add(new)
      if x not in self._targets:
        self._queue.append(x)

    self._flush_queue()

  def split_tgt(self, target, new_target, new_width):
    self._targets.add(new_target)

    coords = self._coords[target]

    self._coords[new_target] = Coords(
        tgt_start=coords.tgt_stop - new_width,
        tgt_width=new_width,
        src_start=coords.src_start,
        src_width=coords.src_width,
    )
    coords.tgt_width -= new_width
    below = self._below[target]
    self._below[new_target] = below[:]
    for x in below:
      self._above[x].add(new)
      if x not in self._sources:
        self._queue.append(x)

    self._flush_queue()

  def _split_x(self, block):
    below = self._below[block]
    above = self._above[block]
    block_coords = self._coords[block]
    below_coords = [self._coords[x] for x in below]
    i = self._split_interval_source(below_coords)
    half_src_width = sum(below_coords[j].src_width for j in range(i))

    new = self._new_block()
    self._coords[new] = Coords(
        src_start=below_coords[i].src_start,
        src_width=block_coords.src_width - half_src_width,
        tgt_start=block_coords.tgt_start,
        tgt_width=block_coords.tgt_width,
    )
    block_coords.src_width = half_src_width
    self._below[new] = below[i:]
    self._below[block] = below[:i]
    self._above[new] = above[:] # Copy the above blist

    # Update the belows of the aboves and the aboves of the belows
    for x in above:
      self._below[x].add(new)
      if x not in self._targets:
        self._queue.append(x)
    for x in below[i:]:
      above_x = self._above[x]
      above_x.remove(block)
      above_x.add(new)

    return new

  def _split_y(self, block):
    below = self._below[block]
    above = self._above[block]
    block_coords = self._coords[block]
    above_coords = [self._coords[x] for x in above]
    i = self._split_interval_target(above_coords)
    half_tgt_width = sum(above_coords[j].tgt_width for j in range(i))

    new = self._new_block()
    self._coords[new] = Coords(
        src_start=block_coords.src_start,
        src_width=block_coords.src_width,
        tgt_start=above_coords[i].tgt_start,
        tgt_width=block_coords.tgt_width - half_tgt_width,
    )
    block_coords.tgt_width = half_tgt_width
    self._above[new] = above[i:]
    self._above[block] = above[:i]
    self._below[new] = below[:] # Copy the below blist

    # Update the belows of the aboves and the aboves of the belows
    for x in below:
      self._above[x].add(new)
      if x not in self._sources:
        self._queue.append(x)
    for x in above[i:]:
      below_x = self._below[x]
      below_x.remove(block)
      below_x.add(new)

    return new

  def _split_z(self, block):
    new = self._new_block()
    self._coords[new] = self._coords[block].copy()

    self._above[new] = self._above[block]
    self._above[block] = blist.sortedlist([new], key=self._by_target)
    self._below[new] = blist.sortedlist([block], key=self._by_source)

    for x in self._above[new]:
      below_x = self._below[x]
      below_x.remove(block)
      below_x.add(new)

    return new

  def _mass(self, coords):
    # FIXME(KK): Use histogram information to do this
    return 0.001

  def _split_interval_source(self, coords):
    # FIXME(KK): Use histogram information to do this
    return len(coords) // 2

  def _split_interval_target(self, coords):
    # FIXME(KK): Use histogram information to do this
    return len(coords) // 2

  def _new_block(self):
    self._n_new_blocks += 1
    return self._n_new_blocks

  def _by_target(self, block):
    return self._coords[block].tgt_start

  def _by_source(self, block):
    return self._coords[block].src_start


# DEMO/TESTING CODE


def test_demo_solver():
  class Endpoint(object):
    pass

  def new_ends(n):
    width = 1.0 / n
    for i in range(n):
      result = Endpoint()
      result.start = i * width
      result.width = width
      yield result

  sources = list(new_ends(50))
  targets = list(new_ends(60))
  manager = LinkGraphManager(
      sources=sources,
      targets=targets,
      source_width={x: (x.start, x.width)
                    for x in sources},
      target_width={x: (x.start, x.width)
                    for x in targets},
      source_interval=(0.0, 1.0),
      target_interval=(0.0, 1.0),
      constraints=Constraints(
          max_above=7,
          max_below=7,
          max_mass=100,
      ))
  manager.fill_in()
  for to_merge in [4, 10, 11, 2, 28, 2, 2]:
    manager.merge_src(sources[to_merge], sources.pop(to_merge + 1))
  for to_merge in [4, 10, 11, 2, 28, 2, 2, 5, 5, 5]:
    manager.merge_tgt(targets[to_merge], targets.pop(to_merge + 1))
  n_blocks = len(manager._coords)
  print('running validation checks')
  manager._validate()
  print('getting layers')

  layers = manager.layers()
  print(f"Total blocks = {n_blocks}.\nn_layers = {len(layers)}")
  for layer in reversed(layers):
    plt.figure(figsize=(10, 8))
    for i, block in enumerate(layer):
      manager._coords[block].plot(PALLETTE[i % len(PALLETTE)])
  plt.show()
  #import ipdb; ipdb.set_trace()
  #plt.show()


def run():
  test_demo_solver()


if __name__ == '__main__':
  run()
