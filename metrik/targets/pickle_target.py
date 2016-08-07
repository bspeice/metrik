from luigi.target import Target
from pickle import dump, dumps, load, loads
from os.path import exists, join
from os import unlink
from tempfile import tempdir
import base64


class PickleTarget(Target):
    def __init__(self, name):
        self.name = name

    def full_path(self):
        return join(tempdir, self.name)

    def exists(self):
        return exists(self.full_path())

    def write(self, object):
        with open(self.full_path(), 'w+b') as handle:
            dump(object, handle)

    def read(self):
        return load(open(self.full_path(), 'rb'))
