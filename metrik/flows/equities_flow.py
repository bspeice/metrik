from metrik.flows.base import Flow, MarketClose
from metrik.tasks.nasdaq import NasdaqETFList, NasdaqCompanyList
from metrik.tasks.tradeking import Tradeking1mTimesales


class EquitiesFlow(Flow):
    def __init__(self, *args, **kwargs):
        super(EquitiesFlow, self).__init__(*args, **kwargs)

    @staticmethod
    def get_schedule():
        return MarketClose()

    def _run(self):
        # When we yield dependencies in `run` instead of `_requires`,
        # they get executed dynamically and we can use their results inline
        etfs = NasdaqETFList(current_datetime=self.present, live=self.live)
        companies = NasdaqCompanyList(current_datetime=self.present,
                                      live=self.live)

        yield etfs
        yield companies

        tradeking_etfs = [Tradeking1mTimesales(
            present=self.present.date(),
            symbol=e['Symbol']
        ) for e in etfs.output().retrieve()['etfs']]
        yield tradeking_etfs

        tradeking_companies = [Tradeking1mTimesales(
            present=self.present.date(),
            symbol=c['Symbol']
        ) for c in companies.output().retrieve()['companies']]
        yield tradeking_companies
