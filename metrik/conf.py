import os
import sys
from six.moves.configparser import RawConfigParser


def get_config_locations():
    return ['/etc/metrik', os.path.expanduser('~/.metrik')]


def get_default_conf():
    cur_file_dir = os.path.dirname(os.path.realpath(__file__))
    return open(cur_file_dir + '/default.conf', 'r')


# Normally it's terrible practice to put static calls into the signature,
# but this is safe (for now) since the get_config_locations() won't change
# during a run. I.e. it starts up and that's the only time it's ever needed.
def get_config(extra_locations=get_config_locations(), is_test=False):
    config = RawConfigParser()

    config.readfp(get_default_conf())

    conf_files = config.read(extra_locations)
    for conf_file in conf_files:
        config.readfp(open(conf_file, 'r'))

    # Not a huge fan of when developers think they're smarter than the
    # end-users, but I'm calling it a special case for testing
    is_travis = 'TRAVIS_BUILD_NUMBER' in os.environ
    is_pytest = hasattr(sys, '_called_from_test')
    if is_pytest or is_travis or is_test:
        config.set('metrik', 'mongo_database', 'metrik-test')

    return config
