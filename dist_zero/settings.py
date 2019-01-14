''' 
Environment configuration.

All environment variables used to configure DistZero should be accessed by only the dist_zero.settings
module.  It will parse and process them into variables that other modules can rely on for configuration.

Generally, environment variables will live in :file:`.env`.
'''

import asyncio
import os

import uvloop

from dotenv import load_dotenv, find_dotenv

USE_UV_LOOP = os.environ.get('USE_UV_LOOP', '')
use_uv_loop = USE_UV_LOOP.strip().lower() == 'true'
if use_uv_loop:
  asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

ENCRYPT_ALL_MESSAGES = os.environ.get('ENCRYPT_ALL_MESSAGES', 'true')
encrypt_all_messages = ENCRYPT_ALL_MESSAGES.strip().lower() == 'true'

load_dotenv(find_dotenv())

DIST_ZERO_ENV = os.environ['DIST_ZERO_ENV']

IS_TESTING_ENV = DIST_ZERO_ENV == 'test'

ALWAYS_REBUILD_DOCKER_IMAGES = os.environ.get('ALWAYS_REBUILD_DOCKER_IMAGES', '').strip().lower() == 'true'

# URL for the docker server
DOCKER_BASE_URL = os.environ.get('DOCKER_BASE_URL', '')

# Transport settings
MACHINE_CONTROLLER_DEFAULT_UDP_PORT = 9876
MACHINE_CONTROLLER_DEFAULT_TCP_PORT = 9877

MACHINE_CONTROLLER_ROUTING_PORT_RANGE = (10000, 20000)

MSG_BUFSIZE = 2048

HAPROXY_STATS_USERNAME = os.environ.get('HAPROXY_STATS_USERNAME')
HAPROXY_STATS_PASSWORD = os.environ.get('HAPROXY_STATS_PASSWORD')

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

C_DEBUG = os.environ.get('C_DEBUG', 'false')
c_debug = C_DEBUG.lower() == 'true'

CAPNP_DIR = os.environ['CAPNP_DIR']

TESTING = DIST_ZERO_ENV == "test"

# List of environment variables to be copied to spawned cloud machine instances.
# Settings variables that are hardcoded into the settings file will be present in the settings file no the remote
# host and do not need to be copied.
# Settings variables that are determined from the os environment will need to be copied if they are
# to be present on the remote host.
CLOUD_ENV_VARS = [
    'DIST_ZERO_ENV',

    # Logging
    'MIN_LOG_LEVEL',
    'LOGSTASH_HOST',
    'LOGSTASH_PORT',
    'LOGZ_IO_TOKEN',

    # For configuration Haproxies
    'HAPROXY_STATS_USERNAME',
    'HAPROXY_STATS_PASSWORD',

    # AWS Variables
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY_ID',
    'DEFAULT_AWS_REGION',
    'ENCRYPT_ALL_MESSAGES',
    'USE_UV_LOOP',
]
