'''
Run this module as the main python script to display demos using matplotlib.
'''
from dist_zero.node.link.manager import LinkGraphManager, Constraints


def _partition(n):
  width = 1.0 / n
  return [(i, (i * width, width)) for i in range(n)]


def manager_A():
  return LinkGraphManager(
      source_object_intervals=_partition(120),
      target_object_intervals=_partition(110),
      constraints=Constraints(max_above=10, max_below=10))


def test_initialize_link_manager():
  manager = manager_A()
  assert len(manager.layers()) == 4


def test_empty_link_manager():
  manager = LinkGraphManager(
      source_object_intervals=[], target_object_intervals=[], constraints=Constraints(max_above=10, max_below=10))


def demo_link_manager():
  from . import link_manager_plots
  manager = manager_A()
  link_manager_plots.plot_manager_layers(manager)


if __name__ == '__main__':
  demo_link_manager()
