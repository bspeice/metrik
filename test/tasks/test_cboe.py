from unittest import TestCase

from metrik.tasks.cboe import CboeOptionableList


class CboeTest(TestCase):

    def test_optionable_list(self):
        companies = CboeOptionableList.retrieve_data()['companies']
        assert len(companies) > 2000


    def test_optionable_list_format(self):
        companies = CboeOptionableList.retrieve_data()['companies']
        symbols = [c['Stock Symbol'] for c in companies]

        assert 'AAPL' in symbols