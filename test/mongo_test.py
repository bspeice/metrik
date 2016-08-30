from unittest import TestCase
from pymongo import MongoClient

from metrik.conf import get_config
from metrik.targets.mongo import MongoTarget


class MongoTest(TestCase):
    def setUp(self):
        config = get_config(is_test=True)
        self.client = MongoClient(
            host=config.get('metrik', 'mongo_host'),
            port=config.getint('metrik', 'mongo_port'))
        self.db = self.client[config.get('metrik', 'mongo_database')]

    def tearDown(self):
        super(MongoTest, self).tearDown()
        config = get_config(is_test=True)
        self.client.drop_database(config.get('metrik', 'mongo_database'))


class MongoTestTest(MongoTest):
    # Test that the database is dropped correctly after each test
    def test_round1(self):
        t = MongoTarget('consistent_collection', 'consistent_id')
        assert not t.exists()
        t.persist({'a': 'b'})

    def test_round2(self):
        t = MongoTarget('consistent_collection', 'consistent_id')
        assert not t.exists()
        t.persist({'a': 'b'})