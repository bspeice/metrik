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

    def test_cron_string(self):
        cron_string = build_cron_file()
        assert len(cron_string.split('\n')) == len(flows) + 1
        assert cron_string[-1] == '\n'
