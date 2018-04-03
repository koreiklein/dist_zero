'''
Environment configuration.
'''

import os

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DIST_ZERO_ENV=os.environ['DIST_ZERO_ENV']

# URL for the docker server
DOCKER_BASE_URL=os.environ['DOCKER_BASE_URL']


TESTING = DIST_ZERO_ENV == "test"
