from luigi.task import Task
from luigi.parameter import DateParameter, Parameter
# noinspection PyUnresolvedReferences
from six.moves.urllib.parse import quote_plus
import pytz
from collections import namedtuple
import requests
import datetime
import csv
from io import StringIO
from dateutil.parser import parse

from metrik.targets.mongo_target import MongoTarget


LiborRate = namedtuple('LiborRate', [
    'publication', 'overnight', 'one_week', 'one_month', 'two_month',
    'three_month', 'six_month', 'one_year', 'currency'
])


class LiborRateTask(Task):

    date = DateParameter()
    currency = Parameter()

    def output(self):
        return MongoTarget('libor', hash(self.task_id))

    def run(self):
        libor_record = self.retrieve_data(self.date, self.currency)
        self.output().persist(libor_record._asdict())

    @staticmethod
    def retrieve_data(date, currency):
        url = ('https://www.theice.com/marketdata/reports/icebenchmarkadmin/'
               'ICELiborHistoricalRates.shtml?excelExport='
               '&criteria.reportDate={}&criteria.currencyCode={}').format(
            quote_plus(date.strftime('%m/%d/%y')),
            currency
        )

        fields = ['tenor', 'publication', 'usd_ice_libor']
        text = requests.get(url).text
        f = StringIO(text)
        next(f)  # Skip the header
        record = {'currency': currency}
        for row in csv.DictReader(f, fieldnames=fields):
            mapping = {
                'Overnight': 'overnight',
                '1 Week': 'one_week',
                '1 Month': 'one_month',
                '2 Month': 'two_month',
                '3 Month': 'three_month',
                '6 Month': 'six_month',
                '1 Year': 'one_year'
            }
            if row['usd_ice_libor']:
                record[mapping[row['tenor']]] = float(row['usd_ice_libor'])
            if row['publication']:
                # Weird things happen with the publication field. For whatever reason,
                # the *time* is correct, but very often the date gets screwed up.
                # When I download the CSV with Firefox I only see the times - when I
                # download with `requests`, I see both date (often incorrect) and time.
                dt = parse(row['publication'])
                dt = dt.replace(year=date.year, month=date.month, day=date.day)
                record['publication'] = dt

        return LiborRate(**record)
