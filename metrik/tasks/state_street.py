import requests
from luigi.parameter import Parameter
import pandas as pd

from metrik.tasks.base import MongoNoBackCreateTask


class StateStreetHoldings(MongoNoBackCreateTask):
    ticker = Parameter()  # type: str

    @staticmethod
    def retrieve_data(ticker, current_datetime, live):
        # TODO: Actually make this static
        base_url = 'https://www.spdrs.com/site-content/xls/{fund}_All_Holdings.xls'
        fund_url = base_url.format(fund=ticker)

        excel_content = pd.read_excel(fund_url, header=None)

        # The actual stuff we care about is arranged in tabular format, thus
        # we actually want to get the rows where the far-right column is
        # not null.
        final_column_index = len(excel_content.columns) - 1
        # And build a series of True/False for "We do want this row" and
        # "we do not want this row" respectively
        do_retain = excel_content[[final_column_index]].isnull() == False
        retain_index = do_retain[do_retain[final_column_index] == True].index

        # Actual content is in rows 2 onwards
        holding_df = excel_content.ix[retain_index[1:]]
        # Headers are in row 1
        holding_df.columns = excel_content.ix[retain_index[0]]

        # And also get the metadata that are in the rows prior to content
        metadata = excel_content.ix[0:retain_index[0]-1].dropna(axis=1)
        metadata_dict = {row[0].strip(':'): row[1]
                         for i, row in metadata.iterrows()}

        return dict(
            holdings=holding_df.to_dict(orient='record'),
            **metadata_dict
        )

    def get_collection_name(self):
        return 'state_street_holdings'