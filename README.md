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
The nodes are passed `MachineController`s.  Each `MachineController` is under the illusion that it controls
its own OS, but is in fact running in a VM or container.

#### "cloud" Mode
In this mode, nodes are passed `MachineController`s just as in virtual mode.  Each `MachineController` controls
its own machine in the cloud.

## Logging

In virtual and simulated mode, logs are written to the .tmp subdirectory.
You can find logs for the spawners that coordinate spinning up the system and running tests.
Also, under .tmp there are directories created for each container, each with their logs.
You can print them all out for viewing with `./scripts/tail_container_logs`.

For clound/production logging, all DistZero MachineController instances will also log to a logstash endpoint
configured via the environment variables `LOGSTASH_HOST` and `LOGSTASH_PORT`.

### Log analysis tools

For convenience, the ./elk/ subdirectory contains
a complete [elk stack](https://www.elastic.co/elk-stack) for log analysis during development,
and docker-compose.yml defines how to run it with docker-compose.
You can build the stack with `docker-compose build` and run it with `docker-compospe up -d`.

To send simulated logs there during development, set `LOGSTASH_HOST=localhost` and `LOGSTASH_PORT=5533` (or whatever
port you're using to run the logstash instance).

To send logs there from virtualized machines running on the same host, you'll need the hostname of the host from
containers.  When using docker-for-mac, this is `host.docker.internal`
[see the notes](https://docs.docker.com/docker-for-mac/release-notes/#docker-community-edition-17060-ce-mac18-2017-06-28-stable)

The elk stack contains a [LogTrail](https://github.com/sivasamyk/logtrail) configuration for the kibana logtrail plugin
for browsing logs across all the machines involved.
To use it, visit the kibana container at [http://localhost:5601](http://localhost:5601) and navigate to the logtrail
plugin.

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
