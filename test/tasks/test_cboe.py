from unittest import TestCase

from metrik.tasks.cboe import CboeOptionableList


class CboeTest(TestCase):

    def test_optionable_list(self):
        companies = CboeOptionableList.retrieve_data()['companies']
        assert len(companies) > 2000