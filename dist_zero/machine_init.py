import sys

from .os_machine_controller import OsMachineController


def run_new_machine_controller_from_args():
  '''
  Read arguments from `sys.argv` and enter the runloop of a new `MachineController` instance for the current host.
  '''
  machine_id, machine_name, mode, system_id = sys.argv[1:]
  machine_controller = OsMachineController(id=machine_id, name=machine_name, mode=mode, system_id=system_id)
  machine_controller.configure_logging()
  machine_controller.runloop()


if __name__ == '__main__':
  run_new_machine_controller_from_args()
