from luigi import Target
from pymongo import MongoClient
from metrik.conf import get_config
from datetime import datetime


class MongoTarget(Target):

    def __init__(self, collection, id):
        config = get_config()
        self.connection = MongoClient(
            host=config.get('metrik', 'mongo_host'),
            port=config.getint('metrik', 'mongo_port'))[
            config.get('metrik', 'mongo_database')
        ]
        self.collection = self.connection[collection]
        self.id = id

    def exists(self):
        return self.collection.find_one({
            '_id': self.id
        }) is not None

    def persist(self, dict_object):
        id_dict = dict_object
        id_dict['_id'] = self.id
        return self.collection.insert_one(id_dict).inserted_id

    def retrieve(self):
        return self.collection.find_one({'_id': self.id})
