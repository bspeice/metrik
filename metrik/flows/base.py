from luigi.task import WrapperTask
from luigi.parameter import DateMinuteParameter
import pandas as pd

from metrik.trading_days import is_trading_day


class Flow(WrapperTask):
    present = DateMinuteParameter()

    def __init__(self, force=False, *args, **kwargs):
        super(Flow, self).__init__(*args, **kwargs)
        self.force = force

    @staticmethod
    def get_schedule():
        raise NotImplementedError('Your flow should know when it should be run.')

    def _requires(self):
        raise NotImplementedError('I need to know what tasks should be run!')

    def requires(self):
        if self.force or self.get_schedule().check_trigger(self.present):
            return self._requires()


class Schedule(object):
    def get_cron_string(self):
        raise NotImplementedError()

    def check_trigger(self, present):
        return True


class DailyMidnight(Schedule):
    def get_cron_string(self):
        return '0 0 * * *'


class WeekdayMidnight(Schedule):
    def get_cron_string(self):
        return '0 0 * * 1-5'


class MarketClose(Schedule):
    def get_cron_string(self):
        return '5 16 * * 1-5'

    def check_trigger(self, present):
        return is_trading_day(present)
