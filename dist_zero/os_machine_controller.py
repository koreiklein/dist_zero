import logging
import time

from . import machine

logger = logging.getLogger(__file__)

class OsMachineController(machine.MachineController):
  pass

if __name__ == '__main__':
  while True:
    time.sleep(1)
    print("Hello docker world")
