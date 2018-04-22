import sys

from .os_machine_controller import OsMachineController

if __name__ == '__main__':
  machine_id, machine_name, mode, system_id = sys.argv[1:]
  machine_controller = OsMachineController(id=machine_id, name=machine_name, mode=mode, system_id=system_id)
  machine_controller.configure_logging()
  machine_controller.runloop()
