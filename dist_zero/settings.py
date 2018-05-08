''' 
Environment configuration.

All environment variables used to configure DistZero should be accessed by only this module
and stored in python variables for use by other modules.
'''

import os

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DIST_ZERO_ENV = os.environ['DIST_ZERO_ENV']

ALWAYS_REBUILD_DOCKER_IMAGES = os.environ.get('ALWAYS_REBUILD_DOCKER_IMAGES', '').lower() == 'true'

# URL for the docker server
DOCKER_BASE_URL = os.environ.get('DOCKER_BASE_URL', '')

# Transport settings
MACHINE_CONTROLLER_DEFAULT_UDP_PORT = 9876
MACHINE_CONTROLLER_DEFAULT_TCP_PORT = 9877

MSG_BUFSIZE = 2048

# Aws Credentials (for spawning nodes in the cloud)
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY_ID = os.environ.get('AWS_SECRET_ACCESS_KEY_ID', '')

DEFAULT_AWS_REGION = os.environ.get('DEFAULT_AWS_REGION', '')
AWS_BASE_AMI = os.environ.get('AWS_BASE_AMI', '')
DEFAULT_AWS_SECURITY_GROUP = os.environ.get('DEFAULT_AWS_SECURITY_GROUP', '')
DEFAULT_AWS_INSTANCE_TYPE = 't2.micro'

# Logging settings
MIN_LOG_LEVEL = int(os.environ['MIN_LOG_LEVEL'])

LOGSTASH_HOST = os.environ['LOGSTASH_HOST']
LOGSTASH_PORT = int(os.environ['LOGSTASH_PORT'])

LOGZ_IO_TOKEN = os.environ.get('LOGZ_IO_TOKEN', '')

TESTING = DIST_ZERO_ENV == "test"

# List of environment variables to be copied to spawned cloud machine instances.
CLOUD_ENV_VARS = [
    'DIST_ZERO_ENV',
    'LOGSTASH_HOST',
    'LOGSTASH_PORT',
    'LOGZ_IO_TOKEN',
    'MIN_LOG_LEVEL',
]
