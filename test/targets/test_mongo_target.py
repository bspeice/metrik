from pymongo import MongoClient
from random import randint
from datetime import datetime, timedelta

from metrik.targets.mongo import MongoTarget
from metrik.conf import get_config
from test.mongo_test import MongoTest
from metrik import __version__ as version

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

    def test_scaling(self):
        # There was an issue in the past where creating too many targets
        # blew things up because it would allocate threads on construction.
        # Make sure we can trivially scale correctly
        targets = [MongoTarget('test_collection', i) for i in range(10000)]

        self.assertEqual(len(targets), 10000)

    def test_metrik_version_saved(self):
        t = MongoTarget('test_collection', 1234)
        t.persist({})
        r = t.retrieve()
        assert r['_metrik_version'] == version

    def test_created_at_timestamp(self):
        present = datetime.now()
        t = MongoTarget('test_collection', 1234)
        t.persist({})
        r = t.retrieve()
        self.assertGreaterEqual(present, r['_created_at'])

        one_second_past = present - timedelta(seconds=1)
        t = MongoTarget('test_collection', 1235)
        t.persist({}, present=one_second_past)
        r = t.retrieve()
        self.assertEqual(one_second_past, r['_created_at'])