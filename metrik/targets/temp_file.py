from os.path import exists, join
from tempfile import tempdir

from luigi.target import Target


class TempFileTarget(Target):
    def __init__(self, name):
        self.name = name

    def full_path(self):
        return join(tempdir, self.name)

    def exists(self):
        return exists(self.full_path())