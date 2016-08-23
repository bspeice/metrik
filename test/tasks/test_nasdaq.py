from unittest import TestCase

from metrik.tasks.nasdaq import NasdaqCompanyList, NasdaqETFList


class NasdaqTest(TestCase):

    def test_company_list(self):
        companies = NasdaqCompanyList.retrieve_data()['companies']
        assert len(companies) > 4000
        # TODO: Get lists of companies from ETF holdings and verify that they
        # can be found here as well - this should be a superset


    def test_etf_list(self):
        etfs = NasdaqETFList.retrieve_data()['etfs']
        assert len(etfs) > 1500