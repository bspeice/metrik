from __future__ import print_function

import logging
import datetime
from time import sleep

from luigi import Task
from luigi.parameter import DateMinuteParameter, BoolParameter
from pymongo import MongoClient

from metrik.targets.mongo import MongoTarget
from metrik.targets.noop import NoOpTarget
from metrik.conf import MONGO_HOST, MONGO_PORT, MONGO_DATABASE


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


# noinspection PyAbstractClass
class MongoNoBackCreateTask(MongoCreateTask):
    # Have one parameter to make sure that the MongoTarget created by `super`
    # doesn't blow up.
    current_datetime = DateMinuteParameter()
    live = BoolParameter()

    def __init__(self, *args, **kwargs):
        super(MongoNoBackCreateTask, self).__init__(*args, **kwargs)
        child_name = type(self).__name__
        if not self.live:
            logging.warning('Trying to create {} without running'
                            ' live, errors potentially to ensue.'.format(child_name))

    def output(self):
        if self.live:
            return super(MongoNoBackCreateTask, self).output()
        else:
            # TODO: return target closest to self.current_datetime
            return NoOpTarget()

    def run(self):
        # It only makes sense to run these tasks live: they can only retrieve
        # data in the moment, and can not go back to back-fill data. This is
        # very unfortunate, but there is plenty of valuable to be had that we
        # wish to persist for the future.
        if self.live:
            return super(MongoNoBackCreateTask, self).run()


class MongoRateLimit(object):
    rate_limit_collection = 'rate_limit'

    def __init__(self, service, limit, interval, max_tries=5, backoff=.5):
        """

        :param present:
        :type present: datetime.datetime
        :param service:
        :param limit:
        :param interval:
        :type interval: datetime.timedelta
        :param max_tries:
        :param backoff:
        """
        self.service = service
        self.limit = limit
        self.interval = interval
        self.max_tries = max_tries
        self.backoff = backoff
        self.db = MongoClient(host=MONGO_HOST, port=MONGO_PORT)[MONGO_DATABASE]

    def get_present(self):
        return datetime.datetime.now()

    def query_locks(self, present):
        return self.db[self.rate_limit_collection].find(
            {'_created_at': {'$gt': present - self.interval},
             'service': self.service}).count()

    def save_lock(self, present):
        self.db[self.rate_limit_collection].save({
            '_created_at': present, 'service': self.service
        })

    def sleep_until(self, present):
        future_time = present + self.interval * self.backoff
        return (future_time - present).total_seconds()

    def acquire_lock(self):
        num_tries = 0
        while num_tries < self.max_tries:
            num_tries += 1
            num_locks = self.query_locks(self.get_present())
            if num_locks < self.limit:
                self.save_lock(self.get_present())
                return True
            elif num_tries < self.max_tries:
                sleep_amount = self.sleep_until(self.get_present())
                sleep(sleep_amount)

        return False


