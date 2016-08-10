from unittest import TestCase
from datetime import datetime
import pytz

from metrik.tasks.ice import USDLibor


# noinspection PyUnresolvedReferences
class TestICE(TestCase):

    def test_correct_libor_Aug8_2016(self):
        # Validate with:
        # https://www.theice.com/marketdata/reports/icebenchmarkadmin/ICELiborHistoricalRates.shtml?excelExport=&criteria.reportDate=8%2F8%2F16&criteria.currencyCode=USD
        aug8_libor = USDLibor.retrieve_data(datetime(2016, 8, 8))

        assert (aug8_libor[aug8_libor['Tenor'] == 'Overnight']['USD ICE LIBOR'] == .4189).all()
        assert (aug8_libor[aug8_libor['Tenor'] == '1 Week']['USD ICE LIBOR'] == .4431).all()
        assert (aug8_libor[aug8_libor['Tenor'] == '1 Month']['USD ICE LIBOR'] == .5119).all()
        assert (aug8_libor[aug8_libor['Tenor'] == '2 Month']['USD ICE LIBOR'] == .6268).all()
        assert (aug8_libor[aug8_libor['Tenor'] == '3 Month']['USD ICE LIBOR'] == .8065).all()
        assert (aug8_libor[aug8_libor['Tenor'] == '6 Month']['USD ICE LIBOR'] == 1.1852).all()
        assert (aug8_libor[aug8_libor['Tenor'] == '1 Year']['USD ICE LIBOR'] == 1.5081).all()

        london_tz = pytz.timezone('Europe/London')
        actual = london_tz.localize(datetime(2016, 8, 8, 11, 45, 6))
        assert (aug8_libor['Publication Time'] == actual).all()

    def test_correct_libor_Aug9_2010(self):
        # Validate with:
        # https://www.theice.com/marketdata/reports/icebenchmarkadmin/ICELiborHistoricalRates.shtml?excelExport=&criteria.reportDate=8%2F9%2F10&criteria.currencyCode=USD
        aug9_libor = USDLibor.retrieve_data(datetime(2010, 8, 9))

        assert (aug9_libor[aug9_libor['Tenor'] == 'Overnight']['USD ICE LIBOR'] == .23656).all()
        assert (aug9_libor[aug9_libor['Tenor'] == '1 Week']['USD ICE LIBOR'] == .27725).all()
        assert (aug9_libor[aug9_libor['Tenor'] == '1 Month']['USD ICE LIBOR'] == .29).all()
        assert (aug9_libor[aug9_libor['Tenor'] == '2 Month']['USD ICE LIBOR'] == .3375).all()
        assert (aug9_libor[aug9_libor['Tenor'] == '3 Month']['USD ICE LIBOR'] == .40438).all()
        assert (aug9_libor[aug9_libor['Tenor'] == '6 Month']['USD ICE LIBOR'] == .6275).all()
        assert (aug9_libor[aug9_libor['Tenor'] == '1 Year']['USD ICE LIBOR'] == .995).all()

        london_tz = pytz.timezone('Europe/London')
        actual = london_tz.localize(datetime(2010, 8, 9, 15, 49, 12))
        assert (aug9_libor['Publication Time'] == actual).all()

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