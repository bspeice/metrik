from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, \
    nearest_workday, USMartinLutherKingJr, USPresidentsDay, GoodFriday,\
    USMemorialDay, USLaborDay, USThanksgivingDay
from pandas.tseries.offsets import CustomBusinessDay


class USTradingCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]

def TradingDay(n):
    return CustomBusinessDay(n, calendar=USTradingCalendar())