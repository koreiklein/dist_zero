'''
Run this module as the main python script to display demos using matplotlib.
'''
import pytest

from dist_zero.node.link.manager import LinkGraphManager, Constraints


def _partition(n):
  width = 1.0 / n
  return [(i, (i * width, width)) for i in range(n)]


@pytest.fixture
def manager_A():
  return LinkGraphManager(
      source_object_intervals=_partition(120),
      target_object_intervals=_partition(110),
      constraints=Constraints(max_above=10, max_below=10))


@pytest.fixture
def manager_B():
  return LinkGraphManager(
      source_object_intervals=_partition(32),
      target_object_intervals=_partition(1),
      constraints=Constraints(max_above=2, max_below=2))


@pytest.fixture
def manager_C():
  return LinkGraphManager(
      source_object_intervals=_partition(1),
      target_object_intervals=_partition(1),
      constraints=Constraints(max_above=10, max_below=10))


def _merge_some_srcs_in_A(manager):
  n_removed = 0
  for start, stop in [(3, 20), (26, 40), (80, 93)]:
    for i in range(stop - 1, start, -1):
      n_removed += 1
      manager.merge_src(i, stop)
  return n_removed


def _merge_some_tgts_in_A(manager):
  n_removed = 0
  for start, stop in [(6, 22), (33, 58), (100, 109)]:
    for i in range(stop - 1, start, -1):
      n_removed += 1
      manager.merge_tgt(i, stop)
  return n_removed


def test_initialize_link_manager(manager_A):
  assert len(manager_A.layers()) == 4


def test_merge_srcs_and_tgts(manager_A):
  n_srcs_removed = _merge_some_srcs_in_A(manager_A)
  n_tgts_removed = _merge_some_tgts_in_A(manager_A)


def test_empty_link_manager():
  manager = LinkGraphManager(
      source_object_intervals=[], target_object_intervals=[], constraints=Constraints(max_above=10, max_below=10))


def _split_C_src(manager, n_parts):
  for i, (start, width) in _partition(n_parts)[1:]:
    manager.split_src(i - 1, i, (n_parts - i) * width)


def _split_C_tgt(manager, n_parts):
  for i, (start, width) in _partition(n_parts)[1:]:
    manager.split_tgt(i - 1, i, (n_parts - i) * width)


def test_grow_link_manager_with_splits(manager_C):
  _split_C_src(manager_C, n_parts=120)
  _split_C_tgt(manager_C, n_parts=110)


def demo_link_manager_A():
  from . import link_manager_plots
  manager = manager_A()
  _merge_some_srcs_in_A(manager)
  _merge_some_tgts_in_A(manager)
  link_manager_plots.plot_manager_layers(manager)


def demo_link_manager_B():
  from . import link_manager_plots
  manager = manager_B()
  manager.merge_src(9, 10)
  manager.merge_src(8, 10)
  manager.merge_src(7, 10)
  manager.merge_src(6, 10)
  link_manager_plots.plot_manager_layers(manager)


def demo_link_manager_C():
  from . import link_manager_plots
  manager = manager_C()
  _split_C_tgt(manager, n_parts=120)
  _split_C_src(manager, n_parts=110)
  link_manager_plots.plot_manager_layers(manager)


if __name__ == '__main__':
  demo_link_manager_C()
