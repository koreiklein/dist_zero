from dist_zero import intervals
from dist_zero.node.link.manager import LinkGraphManager

import matplotlib.pyplot as plt
import matplotlib.colors as colors

PALLETTE = list(colors.TABLEAU_COLORS.values())


def plot_block(block, color, limits):
  a, b = LinkGraphManager.block_rectangle(block, limits=limits)
  x_start, x_stop = a
  y_start, y_stop = b

  x = [x_start, x_stop, x_stop, x_start]
  y = [y_start, y_start, y_stop, y_stop]

  plt.plot(x, y, color, alpha=0.8)
  plt.fill(x, y, color, alpha=0.4)


def plot_layer(layer, limits):
  for i, block in enumerate(layer):
    plot_block(block, PALLETTE[i % len(PALLETTE)], limits)


def plot_manager_layers(manager):
  limits = manager.x_min(), manager.x_max(), manager.y_min(), manager.y_max()
  layers = manager.layers()
  for layer in reversed(layers):
    plt.figure(figsize=(10, 8))
    plot_layer(layer, limits)
  plt.show()
