from luigi import Task, Parameter
from pyquery import PyQuery as pq


class BloombergEquityInfo(Task):
    bbg_code = Parameter()
    user_agent = Parameter()

    @staticmethod
    def retrieve_info(bbg_code, user_agent):
        html = pq('http://www.bloomberg.com/quote/{}'.format(bbg_code),
                  {'User-Agent': user_agent})

        sector, industry, sub_industry = (
            html("div.cell:nth-child(13) > div:nth-child(2)").text(),
            html("div.cell:nth-child(14) > div:nth-child(2)").text(),
            html("div.cell:nth-child(15) > div:nth-child(2)").text()
        )

        return sector, industry, sub_industry
