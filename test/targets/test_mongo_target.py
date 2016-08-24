from pymongo import MongoClient
from random import randint

from metrik.targets.mongo import MongoTarget
from metrik.conf import get_config
from test.mongo_test import MongoTest


class MongoTargetTest(MongoTest):

    def test_exists(self):
        collection = 'test_collection'
        id = 1234
        t = MongoTarget(collection, id)
        assert not t.exists()

        t.persist({'a': 'b'})
        assert t.exists()

        db_collection = self.db[collection]
        db_collection.remove(id)
        assert not t.exists()

    def test_persist_retrieve(self):
        collection = 'test_collection'
        id = 1234
        t = MongoTarget(collection, id)

        d = {str(k): randint(0, 9999) for k in range(20)}
        t.persist(d)

        u = MongoTarget(collection, id)
        assert u.retrieve() == d