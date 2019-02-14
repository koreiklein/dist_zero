from dist_zero.node.link.manager import Inf, MinusInf

import matplotlib.pyplot as plt
import matplotlib.colors as colors

PALLETTE = list(colors.TABLEAU_COLORS.values())


def plot_block(block, color, limits):
  x_min, x_max, y_min, y_max = limits
  if block.is_source:
    x_start = block.start
    x_stop = block.stop
    y_start = y_min
    y_stop = y_max
  elif block.is_target:
    x_start = x_min
    x_stop = x_max
    y_start = block.start
    y_stop = block.stop
  else:
    x_start = block.x_start.start if MinusInf != block.x_start else x_min
    y_start = block.y_start.start if MinusInf != block.y_start else y_min
    x_stop = block.x_stop.start if Inf != block.x_stop else x_max
    y_stop = block.y_stop.start if Inf != block.y_stop else y_max

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
