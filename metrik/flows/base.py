from luigi.task import Task
from luigi.parameter import DateMinuteParameter, BoolParameter
import pandas as pd

from metrik.trading_days import is_trading_day


class Flow(Task):
    present = DateMinuteParameter()
    live = BoolParameter()

    def __init__(self, force=False, *args, **kwargs):
        super(Flow, self).__init__(*args, **kwargs)
        self.force = force
        self.did_run = False

    def complete(self):
        return self.did_run

    @staticmethod
    def get_schedule():
        raise NotImplementedError('Your flow should know when it should be run.')

    def _run(self):
        raise NotImplementedError('I need to know what tasks should be run!')

    def run(self):
        if self.force or self.get_schedule().check_trigger(self.present):
            for yielded in self._run():
                yield yielded

        self.did_run = True


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
