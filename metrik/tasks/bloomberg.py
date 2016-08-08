from luigi import Task, Parameter
from pyquery import PyQuery as pq
from urllib.parse import quote_plus
import logging


class BloombergEquityInfo(Task):
    bbg_code = Parameter()
    user_agent = Parameter()

    @staticmethod
    def retrieve_info(bbg_code, user_agent):
        url = 'http://www.bloomberg.com/quote/{}'.format(
            quote_plus(bbg_code))
        logging.info('Visiting "{}" with agent "{}'.format(url, user_agent))
        html = pq(url, {'User-Agent': user_agent})

        sector, industry, sub_industry = (
            html("div.cell:nth-child(13) > div:nth-child(2)").text(),
            html("div.cell:nth-child(14) > div:nth-child(2)").text(),
            html("div.cell:nth-child(15) > div:nth-child(2)").text()
        )

        return sector, industry, sub_industry
