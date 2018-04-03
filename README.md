# DistZero

## Getting started

  cp .env.template .env
  nosetests


## Environment Configuration
The environment is defined entirely in `dist_zero/settings.py` and nowhere else.
Other modules shoud not read system environment variables.

In a production environment, environment variables should be set in the OS.

In development, the environment is configured in a .env file.
To allow developers to tamper with their .env during development, the .env file
is not checked into source control.  Instead, a .env.template file is checked in with
reasonable default environment variable values.
