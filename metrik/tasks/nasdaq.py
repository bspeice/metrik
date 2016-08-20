import requests
import pandas as pd
from six import StringIO

from metrik.tasks.base import MongoNoBackCreateTask


class NasdaqCompanyList(MongoNoBackCreateTask):
    def get_collection_name(self):
        return 'nasdaq_company_list'

    @staticmethod
    def retrieve_data(*args, **kwargs):
        # Explicitly use requests to make mocking easy
        csv_bytes = requests.get('http://www.nasdaq.com/screening/'
                                 'companies-by-region.aspx?&render=download') \
            .content
        csv_filelike = StringIO(csv_bytes)
        company_csv = pd.read_csv(csv_filelike)[
            ['Symbol', 'Name', 'LastSale', 'MarketCap', 'Country', 'IPOyear',
             'Sector', 'Industry']
        ]
        return {'companies': company_csv.to_dict(orient='records')}


class NasdaqETFList(MongoNoBackCreateTask):
    def get_collection_name(self):
        return 'nasdaq_etf_list'

    @staticmethod
    def retrieve_data(*args, **kwargs):
        csv_bytes = requests.get('http://www.nasdaq.com/investing/etfs/'
                                 'etf-finder-results.aspx?download=Yes') \
            .content
        csv_filelike = StringIO(csv_bytes)
        etf_csv = pd.read_csv(csv_filelike)[['Symbol', 'Name', 'LastSale']]
        return {'etfs': etf_csv.to_dict(orient='records')}
