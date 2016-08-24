from unittest import TestCase
from datetime import datetime
from pandas_datareader.data import get_data_yahoo
from numpy.testing import assert_allclose
import pytest

from metrik.tasks.tradeking import Tradeking1mTimesales
from metrik.trading_days import TradingDay


@pytest.mark.parametrize('ticker', [
    'AAPL', 'GOOG', 'SPY', 'REGN', 'SWHC', 'BAC', 'NVCR'
])
def test_returns_verifiable(ticker):
    # Test that the quotes line up with data off of Yahoo
    now = datetime.now()
    prior_day = now - TradingDay(1)

    quotes = Tradeking1mTimesales.retrieve_data(prior_day, ticker)

    yahoo_ohlc = map(tuple, get_data_yahoo(ticker, prior_day, prior_day)[
        ['Open', 'High', 'Low', 'Close']
    ].values)[0]

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
