'''
Prototype code for solving the wooden blocks problem.

This script is meant to explore the kind of solution DistZero might use
for determining the network to use when connecting source and target neighbors's kids
of a LinkNode when all these kids are associated with a half-open-rectangle in the (source_keys X target_keys) keyspace.


Statement of wooden blocks problem:


You are given two constants, max_above and max_below.
There are two square sushi mats made of thin bamboo rectangles.
One mat is on top of the other. The lines on the mats are arranged perpendicular to each other.
The task is to lift one mat up, and to fill the space between them with wooden blocks so that
  - Each block is a rectangular prism, oriented according to the same x-y-z axes as the mats
  - All the space between them is filled with wood.
  - The top surface of each wooden block is touching no more than max_above other blocks.
  - The bottom surface of each wooden block is touching no more than max_below other blocks.

This problem is relevant to connecting the left neighbors' kids of a `LinkNode` with its right neighbors' kids.
The x dimension in the wooden blocks problem is analogous to the keyspace of the source dataset.
The y dimension in the wooden blocks problem is analogous to the keyspace of the target dataset.
The z dimension in the wooden blocks problem is analogous to the layer index from source to target.
The bottom and top bamboo mats represent the source and target neighbors' kids repectively.
The z-width of a rectangle is meaningless.
The touching of the top face of a block with the bottom face of another block is analogous to a connection
between two LinkNodes.
max_above and max_below correspond to maximum out degree and in degree in the connection graph respectively.

The below prototype solution is meant to inspire a full solution to be added to DistZero and used by LinkNodes.

'''
import bisect

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as colors

PALLETTE = list(colors.TABLEAU_COLORS.values())

epsilon = 0.003


class Rectangle(object):
  '''A half-open 2-dimensional rectangle in the x-y plane.'''

  def __init__(self, x, y, w, h):
    self.x = x
    self.y = y
    self.w = w
    self.h = h

  def __repr__(self):
    return f"(x={self.x}, y={self.y}, w={self.w}, h={self.h})"

  def plot(self, color):
    x = [self.x, self.right, self.right, self.x]
    y = [self.y, self.y, self.top, self.top]
    plt.plot(x, y, color, alpha=0.8)
    plt.fill(x, y, color, alpha=0.4)

  @property
  def right(self):
    return self.x + self.w

  @property
  def top(self):
    return self.y + self.h


class Column(object):
  '''One column of rectangles in a `Layer`'''

  def __init__(self, x, w, yh_list=None):
    self.x = x
    self.w = w

    self.yh_list = [] if yh_list is None else yh_list
    self.finished_updating_yh_list()

  @property
  def h(self):
    y, h = self.yh_list[-1]
    return y + h

  def finished_updating_yh_list(self):
    self.ys = [y for y, h in self.yh_list]
    self.tops = [y + h for y, h in self.yh_list]

  @property
  def right(self):
    return self.x + self.w


class Layer(object):
  '''Represents a single layer of boxes in a solution to a boxes problem.'''

  def __init__(self, columns=None):
    self.columns = [] if columns is None else columns
    self.left_endpoints = [column.x for column in self.columns]
    self.right_endpoints = [column.right for column in self.columns]

    self._h = None

  def _calculate_height(self):
    height = self.columns[0].h
    for column in self.columns[1:]:
      if abs(column.h - height) >= epsilon:
        raise RuntimeError("Layer should not be made of columns of distinct heights.")

    return height

  @property
  def h(self):
    if self._h is None:
      self._h = self._calculate_height()

    return self._h

  def fits_beside(self, next_layer, max_touching):
    return all(max_touching >= next_layer.count_overlapping(rectangle) for rectangle in self.rectangles())

  def fits_below(self, next_layer, max_above, max_below):
    return self.fits_beside(next_layer, max_above) and next_layer.fits_beside(self, max_below)

  def get_overlapping(self, rectangle):
    i = bisect.bisect_right(self.left_endpoints, rectangle.x) - 1
    j = bisect.bisect_left(self.right_endpoints, rectangle.right) + 1
    total = 0
    for column in self.columns[i:j]:
      k = bisect.bisect_right(column.ys, rectangle.y) - 1
      l = bisect.bisect_left(column.tops, rectangle.top) + 1
      for y, h in column.yh_list[k:l]:
        yield Rectangle(x=column.x, y=y, w=column.w, h=h)

  def count_overlapping(self, rectangle):
    i = bisect.bisect_left(self.left_endpoints, rectangle.x)
    j = bisect.bisect_left(self.right_endpoints, rectangle.right) + 1
    total = 0
    for column in self.columns[i:j]:
      k = bisect.bisect_left(column.ys, rectangle.y)
      l = bisect.bisect_left(column.tops, rectangle.top) + 1
      total += l - k
    return total

  def rectangles(self):
    for column in self.columns:
      for (y, h) in column.yh_list:
        yield Rectangle(x=column.x, w=column.w, y=y, h=h)

  def plot(self):
    plt.figure(figsize=(10, 8))
    for i, rectangle in enumerate(self.rectangles()):
      rectangle.plot(PALLETTE[i % len(PALLETTE)])


class RectangleSolver(object):
  def __init__(self, srcs, tgts, max_below, max_above):
    self.max_below = max_below
    self.max_above = max_above

    self.src_layer = srcs
    self.tgt_layer = tgts

    self.cur_layer = self.src_layer
    self.layers = [self.src_layer]

    self.h = self.src_layer.h
    if abs(self.h - self.tgt_layer.h) >= epsilon:
      raise RuntimeError("Bad inputs to RectangleSolver: Src and Tgt layers"
                         f" have different heights: {self.h} != {self.tgt_layer.h}")

  def solve(self):
    while not self._tgt_layer_fits():
      next_layer = self._create_new_layer()
      assert self.cur_layer.fits_below(next_layer, max_above=self.max_above, max_below=self.max_below)
      self.cur_layer = next_layer
      self.layers.append(self.cur_layer)
    self.layers.append(self.tgt_layer)

    return self.layers

  def _tgt_layer_fits(self):
    return self.cur_layer.fits_below(self.tgt_layer, max_above=self.max_above, max_below=self.max_below)

  def _group_columns(self, i):
    '''Pick and return j to make mergeable columns self.cur_layer.columns[i:j]'''
    return min(i + (self.max_below - 1), len(self.cur_layer.columns)) # TODO(KK): Consider a better algorithm

  def _next_rectangle_height(self, column, cur_y):
    # Calculate cur_h
    cur_y_rectangle = Rectangle(x=column.x, y=cur_y, w=column.w, h=epsilon)
    return min((rectangle.h / (self.max_above - 1) for rectangle in self.cur_layer.get_overlapping(cur_y_rectangle)),
               default=min(rectangle.h for rectangle in self.tgt_layer.get_overlapping(cur_y_rectangle)))

  def _create_new_layer(self):
    '''Nondestructively create and return a new layer that would fit between self.cur_layer and self.tgt_layer'''
    columns = []
    ijs = [] # columns[k] will be the merge of self.cur_layer.columns[i:j] where (i, j) = ijs[k]
    i = 0
    while i < len(self.cur_layer.columns):
      j = self._group_columns(i)
      columns.append(
          Column(x=self.cur_layer.columns[i].x, w=self.cur_layer.columns[j - 1].right - self.cur_layer.columns[i].x))
      ijs.append((i, j))
      i = j

    for column, (i, j) in zip(columns, ijs):
      # append to yh_list in column

      # Rules / Priorities:
      #  - Each yh added to column should be no smaller than any of the overlapping yhs in self.tgt_layer
      #  - No yh in in self.cur_layer.column[i:j] can overlap more than self.max_above
      #  - Each yh added to column should otherwise be as small as possible
      cur_y = 0
      while cur_y + epsilon < self.h:
        cur_h = self._next_rectangle_height(column=column, cur_y=cur_y)
        column.yh_list.append((cur_y, cur_h))
        cur_y += cur_h

      column.finished_updating_yh_list()

    return Layer(columns=columns)


# =================================================
# =============== Testing/demo code ===============
# =================================================


def partition(n):
  result = 0.0
  inc = 1.0 / n
  while result < 1.0:
    yield result
    result += inc


def initial(nsrcs, ntgts, max_above=10, max_below=10):
  return RectangleSolver(
      max_above=max_above,
      max_below=max_below,
      srcs=Layer(columns=[Column(x=x, w=1.0 / nsrcs, yh_list=[(0.0, 1.0)]) for x in partition(nsrcs)]),
      tgts=Layer(columns=[Column(x=0.0, w=1.0, yh_list=[(y, 1.0 / ntgts) for y in partition(ntgts)])]))


def demo_solver():
  solver = initial(nsrcs=128, ntgts=100, max_above=4, max_below=4)
  layers = solver.solve()
  for layer in reversed(layers):
    layer.plot()
  plt.show()


def test_rectangles():
  i = 0
  for xstart in range(-5, 3):
    for ystart in range(-5, 3):
      x = [xstart, xstart + 1, xstart + 1, xstart]
      y = [ystart, ystart, ystart + 1, ystart + 1]

      plt.plot(x, y, PALLETTE[i % len(PALLETTE)], alpha=0.8)
      plt.fill(x, y, PALLETTE[i % len(PALLETTE)], alpha=0.5)
      i += 1

  plt.ylim((-5, 5))
  plt.xlim((-5, 5))
  plt.show()


def run():
  demo_solver()
  #print(list(partition(10)))
  #test_rectangles()


if __name__ == '__main__':
  run()
