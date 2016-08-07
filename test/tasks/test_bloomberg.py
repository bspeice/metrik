from unittest import TestCase

from metrik.conf import USER_AGENT
from metrik.tasks.bloomberg import BloombergEquityInfo


class BloombergTest(TestCase):
    def test_correct_info_apple(self):
        sector, industry, sub_industry = \
            BloombergEquityInfo.retrieve_info("AAPL:US", USER_AGENT)

        assert sector == 'Technology'
        assert industry == 'Hardware'
        assert sub_industry == 'Communications Equipment'

    def test_correct_info_kcg(self):
        sector, industry, sub_industry = \
            BloombergEquityInfo.retrieve_info("KCG:US", USER_AGENT)

        assert sector == 'Financials'
        assert industry == 'Institutional Financial Svcs'
        assert sub_industry == 'Institutional Brokerage'