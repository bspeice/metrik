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
        assert self.db[MongoRateLimit.rate_limit_collection].count() == 0

        present = datetime.now()
        onesec_back = present - timedelta(seconds=1)
        ratelimit = MongoRateLimit(
            service, 1, timedelta(seconds=1)
        )
        assert ratelimit.query_locks(onesec_back) == 0

        ratelimit.save_lock(present)
        assert self.db[MongoRateLimit.rate_limit_collection].count() == 1
        assert ratelimit.query_locks(onesec_back) == 1

    def test_save_creates_correct_service(self):
        service_1 = 'testing_ratelimit_1'
        service_2 = 'testing_ratelimit_2'

        ratelimit1 = MongoRateLimit(
            service_1, 1, timedelta(seconds=1)
        )
        ratelimit2 = MongoRateLimit(
            service_2, 1, timedelta(seconds=1)
        )

        present = datetime.now()
        assert self.db[MongoRateLimit.rate_limit_collection].count() == 0
        assert ratelimit1.query_locks(present) == 0
        assert ratelimit2.query_locks(present) == 0

        ratelimit1.save_lock(present)
        assert self.db[MongoRateLimit.rate_limit_collection].count() == 1
        assert ratelimit1.query_locks(present) == 1
        assert ratelimit2.query_locks(present) == 0

    def test_acquire_lock_fails(self):
        service = 'testing_ratelimit'

        # The first scenario is as follows:
        # We try to acquire a lock with 1 try, backoff is 10.
        # We are checking for locks up to 1 second ago, and there
        # is a lock in the database from a half-second ago. Thus,
        # we should fail immediately since we did not acquire the
        # lock and are only allowed one try.
        # Ultimately, we are testing that the 'fail immediately'
        # switch gets triggered correctly
        ratelimit = MongoRateLimit(
            service, 1, timedelta(seconds=1), max_tries=1, backoff=10
        )

        start = datetime.now()
        ratelimit.save_lock(start)
        did_acquire = ratelimit.acquire_lock()
        end = datetime.now()
        assert not did_acquire
        assert (end - start).total_seconds() < 1

    def test_acquire_lock_succeeds(self):
        service = 'testing_ratelimit'

        # The first scenario is as follows:
        # We try to acquire a lock with two tries, backoff is 1.
        # We put a single lock in initially (a half second in the past),
        # thus when we try to acquire on the first try, we should fail.
        # However, the backoff should kick in, and we acquire successfully
        # on the second try
        ratelimit = MongoRateLimit(
            service, 1, timedelta(seconds=1), max_tries=2, backoff=1
        )

        start = datetime.now()
        ratelimit.save_lock(start - timedelta(seconds=.5))
        did_acquire = ratelimit.acquire_lock()
        end = datetime.now()
        # Check that we acquired the lock
        assert did_acquire
        # Check that we only used one backoff period
        assert (end - start).total_seconds() < 2