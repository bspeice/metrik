from luigi import Task, Parameter
from pyquery import PyQuery as pq
import logging
# noinspection PyUnresolvedReferences
from six.moves.urllib.parse import quote_plus
# noinspection PyUnresolvedReferences
from six.moves.html_parser import HTMLParser
from six import PY2


class BloombergEquityInfo(Task):
    bbg_code = Parameter()
    user_agent = Parameter()

    @staticmethod
    def retrieve_info(bbg_code, user_agent):
        class EquityInfoParser(HTMLParser):
            def __init__(self, keys):
                if PY2:
                    HTMLParser.__init__(self)
                else:
                    super(EquityInfoParser, self).__init__()
                self.keys = keys
                self.records = {k: None for k in keys}
                self.do_record = {k: False for k in keys}

            def handle_data(self, data):
                stripped = data.strip()
                # Ignore blank lines
                if not stripped:
                    return
                for k, v in self.do_record.items():
                    if v:
                        self.records[k] = stripped
                        self.do_record[k] = False

                if stripped in self.keys:
                    self.do_record[stripped] = True

            def get_records(self):
                return self.records

        url = 'http://www.bloomberg.com/quote/{}'.format(
            quote_plus(bbg_code))
        logging.info('Visiting "{}" with agent "{}'.format(url, user_agent))
        html = pq(url, {'User-Agent': user_agent}).html()

        keys = ['Sector', 'Industry', 'Sub-Industry']
        eq_info = EquityInfoParser(keys)
        eq_info.feed(html)
        records = eq_info.get_records()

        return [records[k] for k in keys]


class BloombergFXPrice(Task):
    bbg_code = Parameter()
    user_agent = Parameter()

    @staticmethod
    def retrieve_price(bbg_code, user_agent):
        url = 'http://www.bloomberg.com/quote/{}'.format(
            quote_plus(bbg_code)
        )
        logging.info('Visiting "{}" with agent "{}"'.format(url, user_agent))
        html = pq(url, {'User-Agent': user_agent})

        price = float(html('.price').text())
        logging.info('Found FX price {}: {}'.format(bbg_code, price))
        return price
