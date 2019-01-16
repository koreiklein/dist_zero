'''
DistZero startup script for running a single `MachineController` in the current process.
This script is generally invoked with ``python -m dist_zero.machine_init``
'''
import json
import logging
import sys
import traceback

from dist_zero.machine_runner import MachineRunner

logger = logging.getLogger(__name__)


def run_new_machine_runner_from_args():
  '''
  Root runction to start run a single `MachineController` in the current process.
  It will make a single call to `MachineRunner.runloop` using arguments from `sys.argv`
  '''
  config_filename = sys.argv[1]
  with open(config_filename, 'r') as f:
    machine_runner = MachineRunner(machine_config=json.load(f))
  machine_runner.configure_logging()
  machine_runner.runloop()


if __name__ == '__main__':
  run_new_machine_runner_from_args()
