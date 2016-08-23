from metrik.flows.base import Flow, MarketClose
from metrik.tasks.nasdaq import NasdaqETFList, NasdaqCompanyList


class EquitiesFlow(Flow):
    @staticmethod
    def get_schedule():
        MarketClose()

    def _requires(self):
        return [NasdaqETFList(current_datetime=self.present, live=self.live),
                NasdaqCompanyList(current_datetime=self.present,
                                  live=self.live)]