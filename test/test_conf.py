from unittest import TestCase

from metrik.conf import get_config


class ConfigurationTest(TestCase):

    def test_config_returns_values(self):
        config = get_config([])
        assert config is not None

    def test_config_manual_test_instruction(self):
        config = get_config()
        self.assertEqual(config.get('metrik', 'mongo_database'), 'metrik')

        config = get_config(is_test=False)
        self.assertEqual(config.get('metrik', 'mongo_database'), 'metrik')

        config = get_config(is_test=True)
        self.assertEqual(config.get('metrik', 'mongo_database'), 'metrik-test')