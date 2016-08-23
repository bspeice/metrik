from unittest import TestCase
from datetime import datetime

from metrik.trading_days import TradingDay, is_trading_day


class TradingDayTest(TestCase):
    def test_skip_july4(self):
        start = datetime(2016, 7, 1)  # Friday
        end = start + TradingDay(1)
        assert end == datetime(2016, 7, 5)

    def test_skip_july4_backwards(self):
        end = datetime(2016, 7, 5)
        start = end - TradingDay(1)
        assert start == datetime(2016, 7, 1)

    def test_not_bday(self):
        for year in range(2000, 2016):
            date = datetime(year, 7, 4)
            assert not is_trading_day(date)

    def test_is_bday(self):
        assert is_trading_day(datetime(2016, 8, 23))
