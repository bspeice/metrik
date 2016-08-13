from __future__ import print_function
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
from metrik.tasks.base import MongoCreateTask

LiborRate = namedtuple('LiborRate', [
    'publication', 'overnight', 'one_week', 'one_month', 'two_month',
    'three_month', 'six_month', 'one_year', 'currency'
])


class LiborRateTask(MongoCreateTask):

    date = DateParameter()
    currency = Parameter()

    def get_collection_name(self):
        return 'libor'

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

        # TODO: Messing with globals() is probably a terrible idea, is there
        # a better way to write the below code?
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
                globals()[mapping[row['tenor']]] = float(row['usd_ice_libor'])

            if row['publication']:
                # Weird things happen with the publication field. For whatever reason,
                # the *time* is correct, but very often the date gets screwed up.
                # When I download the CSV with Firefox I only see the times - when I
                # download with `requests`, I see both date (often incorrect) and time.
                dt = parse(row['publication'])
                dt = dt.replace(year=date.year, month=date.month, day=date.day)
                globals()['publication'] = dt

        # Because of the shenanigans I did earlier with locals(), ignore
        # unresolved references. Probably a better way to do this.
        # noinspection PyUnresolvedReferences
        return {
            'currency': currency,
            'publication': publication,
            'overnight': overnight,
            'one_week': one_week,
            'one_month': one_month,
            'two_month': two_month,
            'three_month': three_month,
            'six_month': six_month,
            'one_year': one_year
        }
