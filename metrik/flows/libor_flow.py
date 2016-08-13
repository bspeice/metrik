from luigi import WrapperTask, DateParameter, LocalTarget

from metrik.tasks.ice import LiborRateTask
from metrik.targets.temp_file import TempFileTarget


class LiborFlow(WrapperTask):
    date = DateParameter()

    def requires(self):
        currencies = ['USD']
        return [LiborRateTask(self.date, currency)
                for currency in currencies]
