import json
import logging
import sys
import traceback

from dist_zero.machine_runner import MachineRunner

logger = logging.getLogger(__name__)


def run_new_machine_runner_from_args():
  '''
  Read arguments from `sys.argv` and enter the runloop of a new `MachineController` instance for the current host.
  '''
  config_filename = sys.argv[1]
  with open(config_filename, 'r') as f:
    machine_runner = MachineRunner(machine_config=json.load(f))
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
