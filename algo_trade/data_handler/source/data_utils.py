from pandas import DataFrame
import pandas as pd
from pandas import json_normalize
from time import perf_counter
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from algo_trade.data_handler.source.constants import NSE_FO_LIQUID_STOCKS
from algo_trade.utils.meta import AsyncLoggingMeta
from typing import Optional, Union
from algo_trade.data_handler.calendar.constants import TODAY, TIME_ZONE
from algo_trade.data_handler.calendar import MarketCalendarTools


class DataUtils(metaclass=AsyncLoggingMeta):

    def __init__(self, date_str: str = None):

        if date_str is None:
            date_str = datetime.now(tz=TIME_ZONE).date()
        else:
            date_str = datetime.strptime(date_str, DATE_FMT)

        self.next_day = MarketCalendarTools.next_business_day(date_str)
        self.prev_day = MarketCalendarTools.previous_business_day(
            self.next_day)

    def data_remove_quotes_with_na(self,
                                   data: DataFrame,
                                   column: str = 'close'):

        return data.loc[~data[column].isna(), :]

    def calculate_pct_change(
            self, data: DataFrame,
            column1: str = "close",
            column2: str = "prev_close",
            change_column: str = 'pct_change') -> DataFrame:
        """
        Calculate percentage Change for a given
        :param data:
        :param column1:
        :param column2:
        :param change_column:
        :return:
        """
        data[change_column] = ((data[column1] - data[column2]) / data[
            column2]) * 100

        return data

    def highlight_select_stocks(
            self, data: DataFrame,
            select_stocks: list = NSE_FO_LIQUID_STOCKS
    ) -> DataFrame:
        data["priority_stocks"] = "-"
        data.loc[
            data.Symbol.isin(select_stocks), "Priority Stocks"] = "Priority"

        return data

    def get_all_weekly_expiries(
            self, max_count: Optional[int] = 6,
            date_: Optional[Union[date, str]] = None
    ):
        i = 0
        expiry_set = list()
        # TODO: Apply Date to expiry to back date data.
        expiry = MarketCalendarTools.get_weekly_expiry()
        while i < max_count:
            expiry_set.append(next(expiry))
            i += 1

        return expiry_set

    def get_all_monthly_expiries(
            self, max_count: Optional[int] = 3,
            date_: Optional[Union[date, str]] = None
    ):
        i = 0
        expiry_set = list()

        # TODO: Apply Date to expiry to back date data.
        expiry = MarketCalendarTools.get_monthly_expiry()

        while i < max_count:
            expiry_set.append(next(expiry))
            i += 1

        return expiry_set
