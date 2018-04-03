import logging
import uuid
import sys
import time

from dist_zero import machine, messages

logger = logging.getLogger(__file__)

class OsMachineController(machine.MachineController):
  def __init__(self, id):
    self.id = id

  def handle(self):
    return messages.os_machine_controller_handle(self.id)

  def runloop(self):
    while True:
      print("OsMachineController run loop iteration")
      time.sleep(1)
 

if __name__ == '__main__':
  machine_controller = OsMachineController(sys.argv[1])
  machine_controller.runloop()

