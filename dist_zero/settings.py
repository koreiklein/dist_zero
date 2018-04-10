''' 
Environment configuration.
'''

import os

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DIST_ZERO_ENV = os.environ['DIST_ZERO_ENV']

ALWAYS_REBUILD_DOCKER_IMAGES = os.environ['ALWAYS_REBUILD_DOCKER_IMAGES'].lower() == 'true'

# URL for the docker server
DOCKER_BASE_URL = os.environ['DOCKER_BASE_URL']

# Transport settings
MACHINE_CONTROLLER_DEFAULT_UDP_PORT = 9876

MACHINE_CONTROLLER_DEFAULT_TCP_PORT = 9877

MSG_BUFSIZE = 2048

TESTING = DIST_ZERO_ENV == "test"
