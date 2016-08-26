from luigi import Target
from pymongo import MongoClient
from datetime import datetime
from contextlib import contextmanager

from metrik.conf import get_config


class MongoTarget(Target):

    @contextmanager
    def get_db(self):
        config = get_config()
        client = MongoClient(
            host=config.get('metrik', 'mongo_host'),
            port=config.getint('metrik', 'mongo_port'))

        yield client[config.get('metrik', 'mongo_database')]

        client.close()

    def __init__(self, collection, id):
        self.collection = collection
        self.id = id

    def exists(self):
        with self.get_db() as db:
            return db[self.collection].find_one({
                '_id': self.id
            }) is not None

    def persist(self, dict_object):
        id_dict = dict_object
        id_dict['_id'] = self.id

        with self.get_db() as db:
            return db[self.collection].insert_one(id_dict).inserted_id

    def retrieve(self):
        with self.get_db() as db:
            return db[self.collection].find_one({'_id': self.id})
