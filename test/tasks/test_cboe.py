from unittest import TestCase

from metrik.tasks.cboe import CboeOptionableList


class CboeTest(TestCase):

    def test_optionable_list(self):
        companies = CboeOptionableList.retrieve_data()['companies']
        assert len(companies) > 2000
        # TODO: Get lists of companies from ETF holdings and verify that they
        # can be found here as well - this should be a superset