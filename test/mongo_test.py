from unittest import TestCase
from pymongo import MongoClient

from metrik.conf import MONGO_DATABASE, MONGO_PORT, MONGO_HOST
from metrik.targets.mongo import MongoTarget


class MongoTest(TestCase):
    def tearDown(self):
        super(MongoTest, self).tearDown()
        client = MongoClient(MONGO_HOST, MONGO_PORT)
        client.drop_database(MONGO_DATABASE)


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