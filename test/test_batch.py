from unittest import TestCase
from datetime import datetime

from metrik.batch import flows, build_cron_file


class BatchTest(TestCase):
    def test_flows_return_schedule(self):
        present = datetime.now()
        live = False
        for flow_name, flow_class in flows.items():
            assert flow_class(present=present,
                              live=live).get_schedule() is not None
