from unittest import TestCase

from metrik.targets.noop import NoOpTarget


class NoOpTest(TestCase):
    def test_sanity(self):
        t = NoOpTarget()
        assert t.exists()
