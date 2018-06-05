import sys
import traceback
import logging

from dist_zero.machine_runner import MachineRunner

logger = logging.getLogger(__name__)


def run_new_machine_runner_from_args():
  '''
  Read arguments from `sys.argv` and enter the runloop of a new `MachineController` instance for the current host.
  '''
  machine_id, machine_name, mode, system_id = sys.argv[1:]
  machine_runner = MachineRunner(machine_id=machine_id, machine_name=machine_name, mode=mode, system_id=system_id)
  machine_runner.configure_logging()
  machine_runner.runloop()


if __name__ == '__main__':
  try:
    run_new_machine_runner_from_args()
  except Exception as exn:
    e_type, e_value, e_tb = sys.exc_info()
    exn_lines = traceback.format_exception(e_type, e_value, e_tb)
    logger.error(
        "Error starting a machine_runner: {e_lines}", extra={
            'e_type': str(e_type),
            'e_lines': ''.join(exn_lines)
        })
    sys.exit(-1)
