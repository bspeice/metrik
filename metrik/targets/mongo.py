from luigi import Target
from pymongo import MongoClient
from datetime import datetime
from contextlib import contextmanager

from metrik.conf import get_config
from metrik import __version__ as version


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

    def persist(self, dict_object, present=None):
        id_dict = dict_object
        id_dict['_id'] = self.id
        id_dict['_metrik_version'] = version

        # Because MongoDB isn't microsecond-accurate, we need to set the
        # microseconds to 0 to ensure consistency
        present_deref = present if present is not None else datetime.now()
        present_deref.replace(microsecond=0)
        id_dict['_created_at'] = present_deref

        with self.get_db() as db:
            return db[self.collection].insert_one(id_dict).inserted_id

    def retrieve(self):
        with self.get_db() as db:
            return db[self.collection].find_one({'_id': self.id})
