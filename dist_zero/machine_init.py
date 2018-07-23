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
  run_new_machine_runner_from_args()
