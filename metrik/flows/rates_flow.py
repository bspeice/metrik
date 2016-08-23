from metrik.flows.base import Flow, WeekdayMidnight
from metrik.tasks.ice import LiborRateTask


class LiborFlow(Flow):

    @staticmethod
    def get_schedule():
        return WeekdayMidnight()

    def _requires(self):
        currencies = ['USD']
        return [LiborRateTask(self.present, currency)
                for currency in currencies]
