from calendar import monthrange
from datetime import datetime, date, timedelta
from typing import Union, Tuple, Optional
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay

from algo_trade.data_handler.calendar.constants import TODAY, TIME_ZONE, \
    TIME_CUTOFF, DATE_FMT, \
    MARKET_START_TIME, MARKET_CLOSE_TIME
from algo_trade.utils.logger.log_configurator import LogConfig
from algo_trade.utils.constants import LOG_LEVEL
from algo_trade.utils.meta import AsyncLoggingMeta

from algo_trade.data_handler.source.nse.data_cache.cache_manager import \
    nsedata_cache

LogConfig.setup_logging("", LOG_LEVEL)
logger = LogConfig.get_logger(__name__)


class MarketHolidays(metaclass=AsyncLoggingMeta):
    """
    Class that provides operation to fetch the available Market Holidays
     from NSE records and accordingly provide various tools to as per needs.
    """

    def __init__(self):
        self.today = TODAY
        self.data = nsedata_cache()

    def __call__(self):
        return self.get_market_holidays_list()

    def request_leaves_data(self):
        return self.data.market_holidays

    def get_market_holidays_with_description(self,
                                             index_col_name: str = 'Sr_no',
                                             date_col: str = 'tradingDate') -> pd.DataFrame:
        """
        Generate Market Holidays with Description.
        Intention of this method is to populate/update database table records on a yearly basis.

        :param index_col_name: (default) 'Sr_no'
        :param date_col: (default) 'tradingDay'

        :returns: DataFrame which can be wrapped around DB Table operations method.

        """

        leave_dates = self.request_leaves_data()
        leaves = DataFrame(leave_dates)
        leaves[date_col] = to_datetime(leaves[date_col])
        leaves = leaves.rename(
            columns={"tradingDate": "Date", "weekDay": "Day",
                     "description": "Holiday"})
        leaves = leaves.drop(columns=[index_col_name])

        return leaves

    def get_market_holidays_list(self) -> list:
        """
        Generate Market Holidays by making a get request to NSE's Holidays url.

        :returns: List.
        """

        leave_dates = self.request_leaves_data()
        leave_dates = [i.strftime(DATE_FMT) for i in
                       leave_dates.trade_day.to_list()]

        return leave_dates


MarketHolidays = MarketHolidays()


class MarketCalendarTools:
    """
    Market Calendar Tools
    """

    @staticmethod
    def compare_time_cutoff(date_: date):
        tomorrow = (date_ + BDay()).date()
        yesterday = (date_ - BDay()).date()
        cut_off = TIME_CUTOFF
        now = datetime.now(tz=TIME_ZONE).time()

        if date_ > tomorrow and date_ > yesterday:
            return date_

        if date_ < tomorrow and date_ < yesterday:
            return date_

        if cut_off > now:
            if date_ == TODAY:
                if date_.weekday() in (0, 6):
                    return (date_ - BDay()).date()

                return yesterday

            if date_ == yesterday:
                if date_.weekday() in (0, 6):
                    return (date_ + BDay()).date()
                return date_

            if date_ == tomorrow:
                if date_.weekday() in (0, 6):
                    return (date_ + BDay()).date()

                return tomorrow

            if tomorrow < date_ and date_ > yesterday:
                return (date_ - BDay()).date()

        return date_

    @staticmethod
    def next_business_day(
            today: Union[date, datetime] = None, next_b: int = 1,
            strftime: str = DATE_FMT
    ) -> str:
        """
        Utility method to identify next Trading day,
        accounting weekends and Market holidays.

        Assuming Today is 21-Jan-2022,
        the Next market day is supposed to be 24-Jan-2022.
        The output will be 24-Jan-2022.

        >> obj = MarketCalendar()
        >>obj.next_b_day()
        # As in the example above, Today's date is 21-Jan-2022
        # output will be
        >> '24-Jan-2022'

        Optionally, any date can be passed, and the next available
        business date will be provided.
        >>obj.next_b_day(date(29, 1, 2022))
        >>'31-Jan-2022'

        Assuming it is 25th Jan 2022, next business day will be 27-Jan-2022
        Since, 26th Jan 2022, is a market day off in India.
        >>obj.next_b_day(date(25, 1, 2022))
        >>'27-Jan-2022'

        You may as well change the date format to your choice
        using the strftime param.

        :param today: date
                      By default, consumes today's date and
                      provides the next
        :param next_b: int
        :param strftime: (str)
        :return: (str)
                 Next Business date.
        """
        leaves = [str(datetime.strptime(i, strftime).date()) for i in
                  MarketHolidays()]

        if today is None:
            today = TODAY

        try:
            today = MarketCalendarTools.compare_time_cutoff(today.date())
        except AttributeError:
            today = MarketCalendarTools.compare_time_cutoff(today)

        if today is None:
            today = date.today()

        tomorrow = (today + BDay()).date()

        while True:
            # print("Debug Next: {0}".format(str(tomorrow)))
            if str(tomorrow) in leaves:
                tomorrow = (tomorrow + BDay()).date()

            else:
                # print("Break Next: {0}".format(str(tomorrow)))
                break

        logger.info("The Next business day is {0}".format(tomorrow))
        return tomorrow

    @staticmethod
    def previous_business_day(
            today: Union[date, datetime] = None, next_b: int = 1,
            strftime: str = DATE_FMT
    ):

        leaves = [str(datetime.strptime(i, strftime).date()) for i in
                  MarketHolidays()]

        if today is None:
            today = TODAY

        try:
            today = MarketCalendarTools.compare_time_cutoff(today.date())
        except AttributeError:
            today = MarketCalendarTools.compare_time_cutoff(today)

        try:
            yesterday = today.date()
        except AttributeError:
            yesterday = today

        if yesterday > TODAY:
            yesterday = (today - BDay()).date()

        while True:
            # print("Debug: {0}".format(str(yesterday)))
            if str(yesterday) in leaves:
                yesterday = (yesterday - BDay()).date()

            else:
                # print("Break Prev {0}".format(str(yesterday)))
                break

        logger.info("The Previous business day is {0}".format(yesterday))
        return yesterday

    @staticmethod
    def iterate_to_next_business_day(given_date: Union[date, str],
                                     next_b: int = 1, in_str: bool = False) -> Union[date, str]:

        leaves = [str(datetime.strptime(i, DATE_FMT).date())
                  for i in MarketHolidays()]
        i = 0
        while i < next_b:
            given_date = (given_date + Bday()).date()

            i += 1

        if str(given_date) in leaves:
            given_date = (given_date + Bday()).date()

        if in_str:
            return given_date.strftime(DATE_FMT)

        return given_date

    @staticmethod
    def iterate_to_previous_business_day(given_date: Union[date, str],
                                         prev_b: int = 1, in_str: bool = False) -> Union[date, str]:

        leaves = [str(datetime.strptime(i, DATE_FMT).date())
                  for i in MarketHolidays()]
        i = 0
        while i < prev_b:
            given_date = (given_date - BDay()).date()
            i += 1

        if str(given_date) in leaves:
            given_date = (given_date - BDay()).date()

        if in_str:
            return given_date.strftime(DATE_FMT)

        return given_date

    @staticmethod
    def get_datetime_today_now(today_start: str = None) -> Tuple[
        datetime, datetime]:
        """
        Intention of this method is to return today's date
        from market start hours to the current time or close time
        which ever is the last.

        :param today_start:
                            Default value of None,
                            Returns's today's date and time.
                            Else consumes date in %d-%b-%Y
                            format for a select date.
        :returns : today_start(Market hours) in datetime format and
                   now time in datetime format.
        """
        now = datetime.now(tz=TIME_ZONE)
        time_now = now.time()

        if today_start is None:
            today_start = datetime.combine(TODAY, MARKET_START_TIME)

        else:
            today_start = datetime.combine(
                datetime.strptime(today_start, "%d-%b-%Y"), MARKET_START_TIME
            )

        if time_now.hour < 15 and time_now.minute < 30:
            return today_start, now

        else:
            now = datetime.combine(today_start.date(), MARKET_CLOSE_TIME)
            return today_start, now

    @staticmethod
    def get_weekly_expiry(
            symbol: str = 'NIFTY',
            date_: Optional[Union[date, str]] = None,
            day_index: Optional[int] = 3,
            index_count: Optional[int] = 6,
            to_strftime: bool = True,
    ):

        if symbol == 'FINNIFTY':
            day_index = 1

        # Check if Passed param is a str, None or Date
        if date_ is None:
            date_ = MarketCalendarTools.next_business_day()

        if isinstance(date_, str):
            date_ = datetime.strptime(date_, DATE_FMT)

        offset = (day_index - date_.weekday()) % 7
        date_ += timedelta(days=offset)
        week = timedelta(days=7)

        while True:
            if to_strftime:
                yield date_.strftime(DATE_FMT)
            else:
                yield date_

            date_ += week

    @staticmethod
    def get_month_range(year: int, month: int):

        return monthrange(year, month)[1]

    @staticmethod
    def get_time_delta(date_: date, day_index: int, days_of_week: int = 7):
        return timedelta((date_.isoweekday() - day_index) % days_of_week)

    @staticmethod
    def get_monthly_expiry(
            date_: Optional[Union[date, str]] = None,
            day_index: Optional[int] = 4
    ):

        # Step 1. Check for Incoming Date Format

        if date_ is None:
            date_ = TODAY

        if isinstance(date_, str):
            date_ = datetime.strptime(date_, DATE_FMT)

        # Step 2. Iterate over the Generator
        # to only fetch the next 3 available expiries.

        while True:
            # Get the Month's Last Date.
            month_range = MarketCalendarTools.get_month_range(date_.year,
                                                              date_.month)
            # Get the date in full for the last day of the month.
            last_day_of_month = date(
                day=month_range, month=date_.month, year=date_.year
            )

            # Get the Time Delta upto the Previous Thursday.
            time_delta = MarketCalendarTools.get_time_delta(
                last_day_of_month, day_index
            )
            # Calculate the expiry date.
            expiry_date = last_day_of_month - time_delta

            yield expiry_date.strftime(DATE_FMT)

            # Increment Relative Date on Month.
            date_ += relativedelta(months=1)

    @staticmethod
    def days_until_expiry(today: date = None) -> int:
        """Method to calculate number of working days until expiry."""

        expiry_method = MarketCalendarTools.get_weekly_expiry(today,
                                                              to_strftime=False)
        next_expiry = next(expiry_method)

        if today is None:
            day = TODAY

        if day == next_expiry:
            next_expiry = next(expiry_method)

        count = 0

        while day < next_expiry:
            day = MarketCalendarTools.next_business_day(day)

            count += 1

        if count == 0:
            count = 1

        return count

    @staticmethod
    def number_of_days_until_year_end(today: date = None) -> int:

        if today is None:
            day = TODAY

        last_day_of_year = datetime.now().date().replace(month=12, day=31)

        days_ = np.busday_count(day, last_day_of_year)

        return days_

    @staticmethod
    def holidays_in_a_month(today: date) -> int:
        """Returns Number of Holidays in a month."""

        holidays = MarketHolidays()

        holidays = [datetime.strptime(i, DATE_FMT).date() for i in holidays]

        holidays = [True for i in holidays if today.month == i.month]

        return len(holidays)

    @staticmethod
    def number_of_working_days_in_a_month(today: date = None) -> int:
        """Returns number of working days in a month."""

        if today is None:
            day = TODAY

        day += relativedelta(months=1)

        holidays = MarketCalendarTools.holidays_in_a_month(day)

        day_start = day.replace(day=1)
        day_end = day.replace(day=monthrange(day.year, day.month)[1])

        return np.busday_count(day_start, day_end) - holidays
