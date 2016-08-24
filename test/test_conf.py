from unittest import TestCase

from metrik.conf import get_config


class ConfigurationTest(TestCase):

    def test_config_returns_values(self):
        config = get_config([])
        assert config is not None