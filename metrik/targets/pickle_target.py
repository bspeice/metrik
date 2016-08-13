from pickle import dump, load

from metrik.targets.temp_file import TempFileTarget


class PickleTarget(TempFileTarget):
    def write(self, obj):
        with open(self.full_path(), 'w+b') as handle:
            dump(obj, handle)

    def read(self):
        return load(open(self.full_path(), 'rb'))
