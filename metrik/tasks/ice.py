from luigi.task import Task
# noinspection PyUnresolvedReferences
from six.moves.urllib.parse import quote_plus
import pandas as pd
import pytz
from dateutil.parser import parse
import logging


class USDLibor(Task):

    @staticmethod
    def retrieve_data(date):
        url = ('https://www.theice.com/marketdata/reports/icebenchmarkadmin/'
               'ICELiborHistoricalRates.shtml?excelExport='
               '&criteria.reportDate={}&criteria.currencyCode=USD').format(
            quote_plus(date.strftime('%m/%d/%y'))
        )

        def parse_london(dt_str):
            # I'm getting inconsistent behavior in how Pandas parses the CSV
            # file for dates and times. On Travis, it doesn't look like the
            # content is being modified. On my computer, Pandas is spitting
            # back a localized time. So, after parsing, if we have a timezone-
            # enabled datetime, switch to Europe/London, and if not, add the
            # Europe/London info to it
            london_tz = pytz.timezone('Europe/London')
            # Note that parse() implicitly adds timezone information because
            # of how pandas gave us the value
            dt = parse(dt_str).replace(year=date.year,
                                       month=date.month,
                                       day=date.day)
            try:
                return dt.astimezone(london_tz)
            except ValueError:
                return london_tz.localize(dt)

        # Skip 1 row at top for header (header=0),
        # and read 7 total rows. For whatever reason,
        # pandas totally ignores both skipfooter and skip_footer.
        # WTF pandas.
        df = pd.read_csv(
            url, names=['Tenor', 'Publication Time', 'USD ICE LIBOR'],
            header=0, parse_dates=['Publication Time'],
            nrows=7, date_parser=parse_london,
        )
        logging.info('Publication time for USD ICE on {}: {}'.format(
            date.strftime('%m/%d/%Y'), df['Publication Time'].unique()
        ))

        return df
