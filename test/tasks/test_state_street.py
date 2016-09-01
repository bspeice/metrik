# coding=utf-8
from unittest import TestCase
from datetime import datetime

from metrik.tasks.state_street import StateStreetHoldings


class StateStreetHoldingTest(TestCase):
    def test_spy_holdings(self):
        holdings_dict = StateStreetHoldings.retrieve_data(
            'SPY', datetime.now(), True
        )

        self.assertEqual(holdings_dict['Ticker Symbol'], 'SPY')
        self.assertEqual(holdings_dict['Fund Name'], u'SPDR® S&P 500® ETF')
        self.assertGreaterEqual(len(holdings_dict['holdings']), 500)
        # Long live AAPL
        self.assertTrue(holdings_dict['holdings'][0]['Identifier'] == u'AAPL')

    def test_sdy_holdings(self):
        holdings_dict = StateStreetHoldings.retrieve_data(
            'SDY', datetime.now(), True
        )

        self.assertEqual(holdings_dict['Ticker Symbol'], 'SDY')
        self.assertEqual(holdings_dict['Fund Name'], u'SPDR® S&P® Dividend ETF')
        self.assertTrue(holdings_dict['holdings'][0]['Identifier'] == 'HCP')

    def test_spyd_holdings(self):
        holdings_dict = StateStreetHoldings.retrieve_data(
            'SPYD', datetime.now(), True
        )

        self.assertEqual(holdings_dict['Ticker Symbol'], 'SPYD')
        self.assertEqual(holdings_dict['Fund Name'], u'SPDR® S&P® 500 High Dividend ETF')

    def test_r3k_holdings(self):
        holdings_dict = StateStreetHoldings.retrieve_data(
            'THRK', datetime.now(), True
        )

        self.assertEqual(holdings_dict['Ticker Symbol'], 'THRK')
        self.assertEqual(holdings_dict['Fund Name'], u'SPDR Russell 3000® ETF')
        # Interesting story: the fund is not required to actually invest in all
        # 3000 Russell equities, but just seeks to track the index in general.
        # That's why the test is against 2000, not 3000.
        # This also means that we can't check lists of say iShares against this
        # because they're not guaranteed to be consistent.
        self.assertGreaterEqual(len(holdings_dict['holdings']), 2000)
