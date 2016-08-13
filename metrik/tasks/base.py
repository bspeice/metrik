from __future__ import print_function
from luigi import Task

from metrik.targets.mongo_target import MongoTarget


class MongoCreateTask(Task):
    def __init__(self, *args, **kwargs):
        super(MongoCreateTask, self).__init__(*args, **kwargs)
        self.mongo_id = hash(str(self.to_str_params()))

    def get_collection_name(self):
        raise NotImplementedError('Please set the collection name.')

    def output(self):
        return MongoTarget(self.get_collection_name(), self.mongo_id)

    def run(self):
        p_names = self.get_params()
        p_values = self.get_param_values(p_names, [], self.param_kwargs)
        print(p_values)
        data_dict = self.retrieve_data(**dict(p_values))
        data_dict['_id'] = self.mongo_id
        self.output().persist(data_dict)

    @staticmethod
    def retrieve_data(self, *args, **kwargs):
        raise NotImplementedError('Get me some data!')

