import os
import sys

IS_TRAVIS = 'TRAVIS_BUILD_NUMBER' in os.environ
IS_PYTEST = hasattr(sys, '_called_from_test')

TEST = IS_PYTEST or IS_TRAVIS

USER_AGENT = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0"
MONGO_HOST = 'localhost'
MONGO_PORT = 27017

if TEST:
    MONGO_DATABASE = 'metrik'
else:
    MONGO_DATABASE = 'metrik-test'
