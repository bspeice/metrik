from random import randint

from metrik.merge import merge, open_connection
from metrik.conf import get_config
from test.mongo_test import MongoTest


class MergeTest(MongoTest):
    db2_name = 'metrik_test_2'
    collection_name = 'merge_test'

    def setUp(self):
        super(MergeTest, self).setUp()
        self.client2 = self.client
        self.db2 = self.client2[self.db2_name]

    def tearDown(self):
        super(MergeTest, self).tearDown()
        self.client2.drop_database(self.db2_name)

    def test_left_right_merge(self):
        item_string = str(randint(-9999999, 9999999))
        item = {'string': item_string}
        item_id = self.db[self.collection_name].save(item)

        merge(self.client, self.client2,
              self.db.name, self.db2.name)

        assert len(list(self.db[self.collection_name].find())) == 0
        assert len(list(self.db2[self.collection_name].find())) == 1

        item_retrieved = self.db2[self.collection_name].find_one({'_id': item_id})
        assert item_retrieved is not None
        assert item_retrieved['string'] == item_string

    def test_merge_is_nondestructive(self):
        item1_id = self.db2[self.collection_name].save({})
        item2_id = self.db[self.collection_name].save({})

        assert len(list(self.db[self.collection_name].find())) == 1
        assert len(list(self.db2[self.collection_name].find())) == 1

        merge(self.client, self.client,
              self.db.name, self.db2.name)
        assert len(list(self.db[self.collection_name].find())) == 0
        assert len(list(self.db2[self.collection_name].find())) == 2