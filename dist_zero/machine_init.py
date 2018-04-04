import sys

from .os_machine_controller import OsMachineController

if __name__ == '__main__':
  machine_id = sys.argv[1]
  machine_name = sys.argv[2]
  machine_controller = OsMachineController(machine_id, machine_name)
  machine_controller.configure_logging()
  machine_controller.runloop()

