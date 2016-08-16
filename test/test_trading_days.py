from unittest import TestCase
from datetime import datetime

from metrik.trading_days import TradingDay


class TradingDayTest(TestCase):

    def test_skip_july4(self):
        start = datetime(2016, 7, 1)  # Friday
        end = start + TradingDay(1)
        assert end == datetime(2016, 7, 5)

    def test_skip_july4_backwards(self):
        end = datetime(2016, 7, 5)
        start = end - TradingDay(1)
        assert start == datetime(2016, 7, 1)