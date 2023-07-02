import re
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, date
from calendar import monthrange
from typing import List, Tuple

from algo_trade.data_handler.calendar.constants import DATE_FMT, TODAY
from algo_trade.data_handler.source.constants import YF_UTILS_EXCEPTION_LIST
from algo_trade.utils.meta import AsyncLoggingMeta
from algo_trade.data_handler.calendar.calendar_tools import MarketCalendarTools
from algo_trade.data_handler.source.data_utils import DataUtils


class YFUtils(DataUtils, metaclass=AsyncLoggingMeta):
    """
    Intention of YFUtils Class is to reuse YFinance methods
    in a very encapsulated way while also in a use-case specific way.
    """

    def get_period_data(
            self,
            symbol: str,
            period: str = "1mo",
            interval: str = "1d",
            date_fmt: str = None,
            rounding: bool = True,
            index: bool = False,
            ascending: bool = False,
            auto_adjust: bool = True,
            progress: bool = False,
            **kwargs
    ) -> pd.DataFrame:
        """
        Gets you daily data for the selected range and interval.
        """

        if not index:
            symbol = symbol.upper() + ".NS"

        else:
            symbol = symbol.upper()

        data = yf.download(symbol, period=period, interval=interval,
                           rounding=rounding, auto_adjust=auto_adjust,
                           progress=progress,
                           **kwargs)

        if 0 in data.shape:
            self.logger.error("Error Incurred for symbol: {0}".format(symbol))
            if symbol in list(YF_UTILS_EXCEPTION_LIST.keys()):
                return self.get_period_data(
                    YF_UTILS_EXCEPTION_LIST[symbol], period, interval)
            return pd.DataFrame()

        data["prev_close"] = data.Close.shift(1)
        data = data.loc[~data.prev_close.isna(), :]
        data = self.calculate_pct_change(data, "Close", "prev_close")
        data = data.sort_index(ascending=ascending)
        data = data.reset_index()

        try:
            data["Date"] = pd.to_datetime(data.Date).apply(lambda x: x.date())
        except AttributeError:
            pass

        data.columns = data.columns.str.lower()

        if date_fmt is not None:
            data["date"] = pd.to_datetime(data.date).apply(lambda x: x.strftime(date_fmt))

        return data.round(2)

    def get_unique_ticker_set(self, tickers: list) -> tuple:
        tickers = [i + ".NS" for i in tickers]

        return tuple(tickers)

    def get_previous_quote(self, tickers: list, time_frame: str = "week") -> pd.DataFrame:

        _, eom_day = monthrange(self.prev_day.year, self.prev_day.month)

        if time_frame == "week":
            expiry = next(MarketCalendarTools.get_weekly_expiry())
            expiry = datetime.strptime(expiry, DATE_FMT)
            date_format = "%d-%b-%Y"
            if date.today().weekday() not in (0, 1, 2) and datetime.today() <= expiry:
                period, interval = "3wk", "1wk"

            else:
                period, interval = "2wk", "1wk"

        else:
            expiry = next(MarketCalendarTools.get_monthly_expiry())
            expiry = datetime.strptime(expiry, DATE_FMT).date()
            date_format = "%b-%Y"
            if TODAY.day <= eom_day and TODAY <= expiry:
                period, interval = "2mo", "1mo"

            else:
                period, interval = "3mo", "1mo"

        data_dict = {i: self.get_period_data(i, period, interval, date_format).head(1) for i in tickers}

        for i, j in data_dict.items():
            j["symbol"] = i

        data_dict = pd.concat(data_dict.values())

        return data_dict

    def get_previous_timeframe_quote(
            self, tickers: list, timeframe: str = "month"
    ) -> pd.DataFrame:

        """
        `get_previous_timeframe_quote` uses a list of tickers
        and returns previous week or month based on the selected timeframe
        for all the ticker/Symbols in the list.

        :param tickers: List of all Stocks for which the previous
                        timeframe data is to be chosen.
        :param timeframe: (Default) 'month' or 'week'

        :returns: Data Frame with All Symbols with previous OHLC data & % Change
        """

        return self.get_previous_quote(tickers, timeframe)

    def pullback_quote_generator(self,
                                 ticker: str,
                                 days: Tuple[int],
                                 period: str,
                                 interval: str) -> pd.DataFrame:

        data = self.get_period_data(ticker, period=period, interval=interval)
        data = data.sort_values(by='date', ascending=False)
        interval_days = [float(data['pct_change'].iloc[:i].sum()) for i in
                         days]

        avg_trading_volume = data.volume.mean()
        data = data.head(1)
        for i, j in enumerate(interval_days):
            data['{0}days'.format(days[i])] = j

        data['symbol'] = ticker
        data['volume'] = avg_trading_volume
        self.logger.info("Pullback Quote Generated for: {0}".format(ticker))

        return data

    def larger_timeframe_quotes(self,
                                tickers: str,
                                days: tuple = (30, 90, 180),
                                period: str = '6mo',
                                interval: str = '1d'):

        ticker_quotes = list()
        for i in tickers:
            ticker_quotes.append(self.pullback_quote_generator(i, days,
                                                               period,
                                                               interval))

        return pd.concat(ticker_quotes)
