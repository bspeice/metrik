from unittest import TestCase
from datetime import datetime

from metrik.tasks.base import MongoNoBackCreateTask


class BaseTaskTest(TestCase):

    def test_mongo_no_back_live_false(self):
        task = MongoNoBackCreateTask(current_datetime=datetime.now())
        assert not task.live