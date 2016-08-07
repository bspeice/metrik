from unittest import TestCase
import luigi
from metrik.targets.pickle_target import PickleTarget


class FibTask(luigi.Task):
    s = luigi.IntParameter()

    def requires(self):
        if self.s >= 2:
            return [FibTask(self.s - 1), FibTask(self.s - 2)]
        else:
            return []

    def output(self):
        return PickleTarget(self.task_id)

    def run(self):
        if self.s <= 1:
            val = self.s
        else:
            count = 0
            for input in self.input():
                count += input.read()
            val = count

        self.output().write(val)


class TestPickleTarget(TestCase):
    def test_fibonacci(self):
        f = FibTask(6)
        luigi.build([f], local_scheduler=True)

        ret = f.output().read()
        assert ret == 8

        f = FibTask(100)
        luigi.build([f], local_scheduler=True)
        assert f.output().read() == 354224848179261915075
