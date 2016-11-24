import requests
import pandas as pd
from six import StringIO

from metrik.tasks.base import MongoNoBackCreateTask


class CboeOptionableList(MongoNoBackCreateTask):
    def get_collection_name(self):
        return 'cboe_optionable_list'

    @staticmethod
    def retrieve_data(*args, **kwargs):
        # Explicitly use requests to make mocking easy
        csv_bytes = requests.get('http://www.cboe.com/publish/scheduledtask/'
                                 'mktdata/cboesymboldir2.csv').content
        csv_str = csv_bytes.decode('ascii')

        # Because some of the fields include extra commas, we need to
        # pre-process them out
        old_sep = ','
        new_sep = '|'
        csv_rows = csv_str.split('\r\n')
        csv_header = csv_rows[1]
        num_cols = len(csv_header.split(old_sep))
        csv_content = [r.replace(old_sep, new_sep, num_cols - 1)
                       for r in csv_rows[1:]]
        content_str = '\n'.join(csv_content)
        csv_filelike = StringIO(content_str)

        company_csv = pd.read_csv(csv_filelike, sep=new_sep)
        return {'companies': company_csv.to_dict(orient='records')}
