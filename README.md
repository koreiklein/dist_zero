# DistZero

[![Build Status](https://travis-ci.org/koreiklein/dist_zero.svg?branch=dev)](https://travis-ci.org/koreiklein/dist_zero)

## Overview

DistZero is an experimental distributed computing framework for managing serverless datastructures.

DistZero is still under development.

## Modes

DistZero has three modes of operation. Cloud mode is currently the only 'production' mode,
but there are other modes that approximate production so as to make it easy to run tests and demos.

In each mode, DistZero will manage

- a set of *machines* representing physical computers.
- a set of *nodes* representing units of work to be performed by the machines.

#### simulated Mode
In this mode, the machines are simulated by the main python process.

#### virtual Mode
In this mode, each machine is virtualized in its own docker container on the same host as the main python process and
runs in that container under the illusion that it controls the whole OS.

#### cloud Mode
In this mode, each machine is provisioned in the cloud.  It may be either physical or virtual.

## Installation / Setup

DistZero requires a few steps to get it up and running.

- Create a virtual python environment and install python packages (see the section below on Python Packages)
- Set up env variables (see the section below on environment setup)
- Install the [capnproto](https://capnproto.org/) compiler.
  - Instructions can be found [here](https://capnproto.org/install.html).
- Install the c plugin for the capnproto compiler
  - On a Mac
    - Install the build tools
      - brew install libtool
      - brew install automake
      - If stuck, you may want to check out the discussions at
        - [stackoverflow](https://stackoverflow.com/questions/9575989/install-autoreconf-on-osx-lion).
        - [the github issue](https://github.com/maxmind/libmaxminddb/issues/9).
    - Install the [capnpc c plugin](https://github.com/opensourcerouting/c-capnproto)

## Python Packages

Python packages are managed by [pipenv](https://docs.pipenv.org/).

Ranges of acceptable root packages are given in Pipfile, and locked specific versions are in Pipfile.lock

You can install a virtual environment with `pipenv install --python` from the repository root directory,
and add packages to that virtual environment with `pipenv install --dev`.

Python commands that use the environment should be run inside the virtual environment.  You can either run
`pipenv shell` to get a shell in that environment, or prefix each command with `pipenv run`


## Environment Variables

System environment variables are made accessible to python code by `dist_zero/settings.py` and
nowhere else.  Other python modules should not read system environment variables.

Environment variables for running DistZero code are configured in a .env file.
To allow developers to tamper with their .env during development, the .env file
is not checked into source control.  Instead, a .env.template file is checked in with
reasonable default environment variable values.

Before running any production code, you'll need to somehow create a suitable .env file.
During development, you can run `cp .env.template .env` and then modify the variables in .env to your liking.

Note that pipenv also reads and sets environment variables from the .env file when setting up a virtual environment.

## Logging

In virtual and simulated mode, logs are written to the .tmp subdirectory in a JSON format and a human readable format.
In there, you can find the logs from the main python process.

In virtual mode, DistZero will create directories in .tmp to hold the logs from each container.
You can tail them with `./scripts/tail_container_logs`.

For production logging, all DistZero machines should also log to a logstash endpoint
configured via the environment variables `LOGSTASH_HOST` and `LOGSTASH_PORT`.

### Log analysis tools

For convenience, the ./elk/ subdirectory contains
a complete [elk stack](https://www.elastic.co/elk-stack) for log analysis during development,
and docker-compose.yml defines how to run it with docker-compose.
You can build the stack with `docker-compose build` and run it with `docker-compose up -d`.

To send logs there during development *in simulated mode*, set `LOGSTASH_HOST=localhost` and `LOGSTASH_PORT=5533`.

To send logs there while running *in virtual mode*, you'll need a hostname that the containers can use to refer
to the host.  When using docker-for-mac, this is `host.docker.internal`
[see the notes](https://docs.docker.com/docker-for-mac/release-notes/#docker-community-edition-17060-ce-mac18-2017-06-28-stable)

The elk stack contains a [LogTrail](https://github.com/sivasamyk/logtrail) configuration for browsing logs across all
the machines involved.

To use it, visit the kibana container at [http://localhost:5601](http://localhost:5601) and navigate to the logtrail
plugin.

## Documentation

The [online documentation](https://koreiklein.github.io/dist_zero/) is built using
[sphinx](http://www.sphinx-doc.org/en/master/) as part of the CI process.  You can build the docs yourself
locally with `pipenv run make html`.

## Testing

Tests are written with the [pytest](https://docs.pytest.org/en/latest/) testing framework and kept in `./test/`.

Tests are given pytest marks  based on the mode that they test.  "virtual" and "cloud" tests tend to
be more expensive to run than "simulated" tests.

To run tests specific to a single mode, you can filter with pytest:

```bash
pipenv run pytest -k simulated
pipenv run pytest -k virtual
pipenv run pytest -k cloud
```

Virtual mode tests will spin up docker containers on the host running the tests,
and cloud mode tests will spin up instances in the cloud.  The tests should generally be written to clean up any
resources they use during testing, but you should still be aware that it's possible for tests to leak containers or
cloud instances if something goes wrong with the cleanup.

## Development

All development work on DistZero is managed in [pivotal tracker](https://www.pivotaltracker.com/n/projects/2160764).

## Continuous Integration

Builds are done on [travis](https://travis-ci.org/koreiklein/dist_zero).  To keep the CI tests reasonably lightweight,
the .travis-ci.yml runs the tests in simulated mode only.

