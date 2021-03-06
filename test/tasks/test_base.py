from unittest import TestCase
from datetime import datetime, timedelta

from metrik.tasks.base import MongoNoBackCreateTask, MongoRateLimit
from test.mongo_test import MongoTest


class BaseTaskTest(TestCase):
    def test_mongo_no_back_live_false(self):
        # Test that default for `live` parameter is False
        task = MongoNoBackCreateTask(current_datetime=datetime.now())
        assert not task.live


class RateLimitTest(MongoTest):
    def test_save_creates_record(self):
        service = 'testing_ratelimit'
        interval = timedelta(seconds=1)
        assert self.db[MongoRateLimit.rate_limit_collection].count() == 0

        present = datetime.now()
        onesec_back = present - timedelta(seconds=1)
        ratelimit = MongoRateLimit()
        assert ratelimit.query_locks(onesec_back, interval, service) == 0

        ratelimit.save_lock(present, service)
        assert self.db[MongoRateLimit.rate_limit_collection].count() == 1
        assert ratelimit.query_locks(onesec_back, interval, service) == 1

    def test_save_creates_correct_service(self):
        service_1 = 'testing_ratelimit_1'
        service_2 = 'testing_ratelimit_2'
        interval = timedelta(seconds=1)

        ratelimit = MongoRateLimit()

        present = datetime.now()
        assert self.db[MongoRateLimit.rate_limit_collection].count() == 0
        assert ratelimit.query_locks(present, interval, service_1) == 0
        assert ratelimit.query_locks(present, interval, service_2) == 0

        ratelimit.save_lock(present, service_1)
        assert self.db[MongoRateLimit.rate_limit_collection].count() == 1
        assert ratelimit.query_locks(present, interval, service_1) == 1
        assert ratelimit.query_locks(present, interval, service_2) == 0

    def test_acquire_lock_fails(self):
        service = 'testing_ratelimit'
        limit = 1
        interval = timedelta(seconds=1)
        max_tries = 1
        backoff = 10

        # The first scenario is as follows:
        # We try to acquire a lock with 1 try, backoff is 10.
        # We are checking for locks up to 1 second ago, and there
        # is a lock in the database from a half-second ago. Thus,
        # we should fail immediately since we did not acquire the
        # lock and are only allowed one try.
        # Ultimately, we are testing that the 'fail immediately'
        # switch gets triggered correctly
        ratelimit = MongoRateLimit()

        start = datetime.now()
        ratelimit.save_lock(start, service)
        did_acquire = ratelimit.acquire_lock(service, limit, interval,
                                             max_tries, backoff)
        end = datetime.now()
        assert not did_acquire
        assert (end - start).total_seconds() < 1

    def test_acquire_lock_succeeds(self):
        service = 'testing_ratelimit'
        limit = 1
        interval = timedelta(seconds=1)
        max_tries = 2
        backoff = 1

        # The first scenario is as follows:
        # We try to acquire a lock with two tries, backoff is 1.
        # We put a single lock in initially (a half second in the past),
        # thus when we try to acquire on the first try, we should fail.
        # However, the backoff should kick in, and we acquire successfully
        # on the second try
        ratelimit = MongoRateLimit()

        start = datetime.now()
        ratelimit.save_lock(start - timedelta(seconds=.5), service)
        did_acquire = ratelimit.acquire_lock(service, limit, interval,
                                             max_tries, backoff)
        end = datetime.now()
        # Check that we acquired the lock
        assert did_acquire
        # Check that we only used one backoff period
        total_seconds = (end - start).total_seconds()
        assert 1 < total_seconds < 2

    def test_acquire_lock_succeeds_float(self):
        # And do the whole thing all over again, but with a floating backoff
        service = 'testing_ratelimit'
        limit = 1
        interval = timedelta(seconds=1)
        max_tries = 2
        backoff = 1.01

        # The first scenario is as follows:
        # We try to acquire a lock with two tries, backoff is 1.
        # We put a single lock in initially (a half second in the past),
        # thus when we try to acquire on the first try, we should fail.
        # However, the backoff should kick in, and we acquire successfully
        # on the second try
        ratelimit = MongoRateLimit()

        start = datetime.now()
        ratelimit.save_lock(start - timedelta(seconds=.5), service)
        did_acquire = ratelimit.acquire_lock(service, limit, interval,
                                             max_tries, backoff)
        end = datetime.now()
        # Check that we acquired the lock
        assert did_acquire
        # Check that we used at least one backoff period
        total_seconds = (end - start).total_seconds()
        assert 1 < total_seconds

    def test_sleep_for_gives_correct_time(self):
        ratelimit = MongoRateLimit()

        assert ratelimit.sleep_for(timedelta(seconds=1), 1) == 1
        assert ratelimit.sleep_for(timedelta(seconds=1), 2) == 2
        assert ratelimit.sleep_for(timedelta(seconds=1), 1.1) == 1.1
