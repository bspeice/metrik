from metrik.flows.base import Flow, MarketClose
from metrik.conf import get_config
from metrik.tasks.cboe import CboeOptionableList
from metrik.tasks.tradeking import TradekingOptionsQuotes

class EquitiesFlow(Flow):
    def __init__(self, *args, **kwargs):
        super(EquitiesFlow, self).__init__(*args, **kwargs)

    @staticmethod
    def get_schedule():
        return MarketClose()

    def _run(self):
        optionable = CboeOptionableList(current_datetime=self.present,
                                        live=self.live)
        yield optionable

        options_quotes = [TradekingOptionsQuotes(symbol=s,
                                                 current_datetime=self.present,
                                                 live=self.live)
                          for s in optionable.output().retrieve()['companies']]
        yield options_quotes