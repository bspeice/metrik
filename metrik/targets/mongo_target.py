from luigi import Target
from pymongo import MongoClient
from metrik.conf import MONGO_HOST, MONGO_PORT, MONGO_DATABASE


class MongoTarget(Target):

    def __init__(self, collection, id):
        self.connection = MongoClient(MONGO_HOST, MONGO_PORT)[MONGO_DATABASE]
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
