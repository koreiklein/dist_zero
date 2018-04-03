'''
Environment configuration.
'''

import os

DIST_ZERO_ENV=os.environ.get('DIST_ZERO_ENV')


TESTING = DIST_ZERO_ENV == "test"
