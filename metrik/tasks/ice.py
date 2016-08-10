from luigi.task import Task
# noinspection PyUnresolvedReferences
from six.moves.urllib.parse import quote_plus
import pandas as pd
import pytz
from dateutil.parser import parse


class USDLibor(Task):

    @staticmethod
    def retrieve_data(date):
        url = ('https://www.theice.com/marketdata/reports/icebenchmarkadmin/'
               'ICELiborHistoricalRates.shtml?excelExport='
               '&criteria.reportDate={}&criteria.currencyCode=USD').format(
            quote_plus(date.strftime('%m/%d/%y'))
        )

        def parse_london(dt_str):
            # Pandas does its best to try and help us out by modifying the
            # actual csv content to try and add timezone and date information.
            # Which is not in any sense what we want.
            # So we have convoluted steps to go and fix that.
            london_tz = pytz.timezone('Europe/London')
            # Note that parse() implicitly adds timezone information because
            # of how pandas gave us the value
            dt = parse(dt_str).replace(year=date.year,
                                       month=date.month,
                                       day=date.day)
            return dt.astimezone(london_tz)

        # Skip 1 row at top for header (header=0),
        # and read 7 total rows. For whatever reason,
        # pandas totally ignores both skipfooter and skip_footer.
        # WTF pandas.
        df = pd.read_csv(
            url, names=['Tenor', 'Publication Time', 'USD ICE LIBOR'],
            header=0, parse_dates=['Publication Time'],
            nrows=7, date_parser=parse_london,
        )

        return df
