from unittest import TestCase
from datetime import datetime
import pytz

from metrik.tasks.ice import LiborRateTask
from metrik.conf import USER_AGENT


# noinspection PyUnresolvedReferences
class TestICE(TestCase):

    def test_correct_libor_Aug8_2016(self):
        # Validate with:
        # https://www.theice.com/marketdata/reports/icebenchmarkadmin/ICELiborHistoricalRates.shtml?excelExport=&criteria.reportDate=8%2F8%2F16&criteria.currencyCode=USD
        aug8_libor = LiborRateTask.retrieve_data(datetime(2016, 8, 8), 'USD')

        assert aug8_libor.overnight == .4189
        assert aug8_libor.one_week == .4431
        assert aug8_libor.one_month == .5119
        assert aug8_libor.two_month == .6268
        assert aug8_libor.three_month == .8065
        assert aug8_libor.six_month == 1.1852
        assert aug8_libor.one_year == 1.5081

        london_tz = pytz.timezone('Europe/London')
        actual = london_tz.localize(datetime(2016, 8, 8, 11, 45, 6))
        assert aug8_libor.publication == actual


    def test_correct_libor_Aug9_2010(self):
        # Validate with:
        # https://www.theice.com/marketdata/reports/icebenchmarkadmin/ICELiborHistoricalRates.shtml?excelExport=&criteria.reportDate=8%2F9%2F10&criteria.currencyCode=USD
        aug9_libor = LiborRateTask.retrieve_data(datetime(2010, 8, 9), 'USD')

        assert aug9_libor.overnight == .23656
        assert aug9_libor.one_week == .27725
        assert aug9_libor.one_month == .29
        assert aug9_libor.two_month == .3375
        assert aug9_libor.three_month == .40438
        assert aug9_libor.six_month == .6275
        assert aug9_libor.one_year == .995


        london_tz = pytz.timezone('Europe/London')
        actual = london_tz.localize(datetime(2010, 8, 9, 15, 49, 12))
        assert aug9_libor.publication == actual

    def test_correct_date_reasoning(self):
        # Make sure I document how to handle datetime issues in the future
        london_tz = pytz.timezone('Europe/London')
        ny_tz = pytz.timezone('America/New_York')

        # DON'T YOU DARE SET TZINFO, SHENANIGANS HAPPEN
        assert (datetime(2016, 8, 8, 15, 0, 0, tzinfo=london_tz) <
                datetime(2016, 8, 8, 15, 0, 0, tzinfo=ny_tz))

        assert (datetime(2016, 8, 8, 15, 0, 0, tzinfo=london_tz) >
                datetime(2016, 8, 8, 10, 0, 0, tzinfo=ny_tz))

        # ALWAYS USE timezone.localize()
        assert (london_tz.localize(datetime(2016, 8, 8, 15)) ==
                ny_tz.localize(datetime(2016, 8, 8, 10)))