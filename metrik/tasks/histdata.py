from zipfile import ZipFile
from io import BytesIO
import requests
import pandas as pd
from datetime import datetime

from metrik.tasks.base import MongoCreateTask
from metrik.utils import masked_get
from metrik.conf import USER_AGENT


class HistDataWrapper(MongoCreateTask):
    @staticmethod
    def fetch_zip(currency, year, month, side):
        """

        :param currency: Currency pair specified as EURUSD
        :type currency: str
        :param year: Year for retrieval as an integer
        :param month: Month for retrieval as an integer
        :param side: BID or ASK for which side of the trade you want
        :return: zip file content with CSV of tick data
        """
        # HistData has some interesting tricks they go through to make it
        # hard to bulk download data. Specifically: you post to one page with
        # your `Referer` as a different page to get the data, plus some extra
        # POST body things in order to get the data to come.
        request_url = 'http://www.histdata.com/get.php'
        month_leading = '{:02d}'.format(month)

        # No idea what this parameter does, because I'm not convinced it's a nonce:
        if side == 'BID':
            tk = '6b95868ebd96e92f3f28196354e1901f'
        else:
            tk = 'fa921f9b65d175d05c5976b5f78e5333'

        zip_fname = 'HISTDATA_COM_NT_{pair}_T_{side}_{year}{month_leading}.zip'.format(
            year=year,
            month_leading=month_leading,
            side=side,
            pair=currency
        )

        zip_url = 'http://www.histdata.com/' \
                  'download-free-forex-historical-data/?/ninjatrader/' \
                  'tick-{side}-quotes/{pair}/{year}/{month_nonleading}/' \
                  '{zip_fname}' \
            .format(
                year=year,
                side=side.lower(),
                month_nonleading=str(month),
                pair=currency.lower(),
                zip_fname=zip_fname
            )

        request_content = requests.post(request_url, headers={
            'Host': 'www.histdata.com',
            'Referer': zip_url,
            'User-Agent': USER_AGENT,
            'Cookie': '__cfduid=d42d8f540c786afd0568ab63eb20c50eb1471099760; complianceCookie=off',
        }, data={
            'tk': tk,
            'date': year,
            'datemonth': '{year}{month_leading}'.format(
                year=year, month_leading=month_leading),
            'platform': 'NT',
            'timeframe': 'T_{}'.format(side),
            'fxpair': currency
        }).content

        return BytesIO(request_content)

    @staticmethod
    def fetch_csv(zip_content, currency, year, month, side):
        csv_fname = 'DAT_NT_{pair}_T_{side}_{year}{month_leading}.csv'.format(
            pair=currency,
            side=side,
            year=year,
            month_leading='{:02d}'.format(month)
        )
        z = ZipFile(zip_content)
        bytes = z.read(csv_fname)
        return BytesIO(bytes)

    @staticmethod
    def parse_csv(csv_content):
        def parse_timestamp(string):
            return datetime.strptime(string, '%Y%m%d %H%M%S')

        table = pd.read_csv(
            filepath_or_buffer=csv_content,
            names=['Timestamp', 'Price', 'Unknown'],
            parse_dates=['Timestamp'],
            date_parser=parse_timestamp,
            sep=';'
        ).drop('Unknown', axis=1)

        return table.to_dict()