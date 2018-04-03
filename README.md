# DistZero

## Getting started

  cp .env.template .env
  nosetests

## Modes

DistZero has three modes of operation.  In each mode, all nodes behave the same way,
but they are instantiated differently and given passed different instances of `MachineController`.

#### "simulated" Mode
In this mode, nodes are run in the main python process with `SimulatedMachineController`s

#### "virtual" Mode
In this mode, nodes are run in virtual machines or containers on the same host as the main python process.
The nodes are passed `OsMachineController`s.  Each `OsMachineController` is under the illusion that it controls
its own OS, but is in fact running in a VM or container.

#### "cloud" Mode
In this mode, nodes are passed `OsMachineController`s just as in virtual mode.  Each `OsMachineController` controls
its own machine in the cloud.


## Testing
Tests are given nose labels based on the mode that they test.  Naturally, "virtual" and "cloud" tests tend to
be more expensive to run than "simulated" tests.

To run tests specific to a single mode, you can filter by nose atributes:

  nosetests -a mode=simulated
  nosetests -a mode=virtual
  nosetests -a mode=cloud

Virtual mode tests will spin up docker containers on the host running the tests,
and cloud mode tests will spin up instances in the cloud.  Tests should generally be written to clean up any
resources they use during testing, but you should still be aware that it's possible for tests to leak containers or
cloud instances if something goes wrong with the cleanup.

## Environment Configuration
The environment is defined entirely in `dist_zero/settings.py` and nowhere else.
Other modules shoud not read system environment variables.

In a production environment, environment variables should be set in the OS.

In development, the environment is configured in a .env file.
To allow developers to tamper with their .env during development, the .env file
is not checked into source control.  Instead, a .env.template file is checked in with
reasonable default environment variable values.
