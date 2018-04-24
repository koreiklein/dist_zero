import sys

from dist_zero.machine_runner import MachineRunner


def run_new_machine_runner_from_args():
  '''
  Read arguments from `sys.argv` and enter the runloop of a new `MachineController` instance for the current host.
  '''
  machine_id, machine_name, mode, system_id = sys.argv[1:]
  machine_runner = MachineRunner(machine_id=machine_id, machine_name=machine_name, mode=mode, system_id=system_id)
  machine_runner.configure_logging()
  machine_runner.runloop()


if __name__ == '__main__':
  run_new_machine_runner_from_args()
