from unittest import TestCase
from datetime import datetime
from pandas_datareader.data import get_data_yahoo
from numpy.testing import assert_allclose
import pytest
from six.moves import map
from pytz import utc

from metrik.tasks.tradeking import Tradeking1mTimesales, TradekingOptionsQuotes, TradekingApi
from metrik.trading_days import TradingDay
from metrik.targets.mongo import MongoTarget
from test.mongo_test import MongoTest


@pytest.mark.parametrize('ticker', [
    'AAPL', 'GOOG', 'SPY', 'REGN', 'SWHC', 'BAC', 'NVCR', 'ARGT'
])
def test_returns_verifiable(ticker):
    # Test that the quotes line up with data off of Yahoo
    now = datetime.now()
    prior_day = now - TradingDay(1)

    quotes = Tradeking1mTimesales.retrieve_data(prior_day, ticker)

    yahoo_ohlc = list(map(tuple, get_data_yahoo(ticker, prior_day, prior_day)[
        ['Open', 'High', 'Low', 'Close']
    ].values))[0]

    open = high = close = 0
    low = 999999
    for index, quote in enumerate(quotes['quotes']):
        if index == 0:
            open = quote['opn']
        if index == len(quotes['quotes']) - 1:
            close = quote['last']
        high = max(high, quote['hi'])
        low = min(low, quote['lo'])

    tradeking_ohlc = (open, high, low, close)

    assert_allclose(tradeking_ohlc, yahoo_ohlc, rtol=1e-3)

@pytest.mark.parametrize('ticker', [
    'AAPL', 'GOOG', 'SPY', 'REGN', 'SWHC', 'BAC', 'NVCR', 'ARGT'
])
def test_chain_returns(ticker):
    api = TradekingApi()
    chain = TradekingOptionsQuotes.retrieve_chain_syms(api, ticker)

    assert len(chain) > 20

class TradekingTest(MongoTest):

    def test_record_is_saveable(self):
        # Had an issue previously where the `date` type would cause saving
        # to fail. Make sure that doesn't continue
        now = datetime.now()
        prior_day = now - TradingDay(1)
        quotes = Tradeking1mTimesales.retrieve_data(prior_day, 'AAPL')
        t = MongoTarget('tradeking', hash('just_testing'))
        t.persist(quotes)
        quotes_retrieved = t.retrieve()
        for quote in quotes_retrieved['quotes']:
            quote['datetime'] = utc.localize(quote['datetime'])
            quote['timestamp'] = utc.localize(quote['timestamp'])

        assert quotes == quotes_retrieved
