import re
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, date
from calendar import monthrange

from algo_trade.data_handler.calendar.constants import DATE_FMT
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
            rounding: bool = True,
            index: bool = False,
            exclude_dividends: bool = True,
            ascending: bool = False,
            drop_stock_dividend: bool = True,
            **kwargs
    ) -> pd.DataFrame:
        """
        Gets you daily data for the selected range and interval.
        """

        if not index:
            symbol = symbol.upper() + ".NS"

        else:
            symbol = symbol.upper()

        data = yf.Ticker(symbol).history(
            period=period, interval=interval, rounding=rounding, **kwargs
        )

        if exclude_dividends:
            try:

                data = data.loc[
                       ~((data.Dividends != 0) & (data["Stock Splits"] != 0)),
                       :
                       ]
            except AttributeError:
                self.logger.error(
                    "Error Incurred for symbol: {0}".format(symbol))
                if symbol in list(YF_UTILS_EXCEPTION_LIST.keys()):
                    return self.get_period_data(
                        YF_UTILS_EXCEPTION_LIST[symbol])

        data["prev_close"] = data.Close.shift(1)
        data = data.loc[~data.prev_close.isna(), :]
        data = self.calculate_pct_change(data, "Close", "prev_close")
        data = data.sort_index(ascending=ascending)
        data = data.reset_index()
        data["Date"] = pd.to_datetime(data.Date).apply(lambda x: x.date())
        data.columns = data.columns.str.lower()

        if drop_stock_dividend:
            data = data.drop(data.columns[[-3, -4]], axis=1)

        return data.round(2)

    def get_unique_ticker_set(self, tickers: list):
        tickers = [i + ".NS" for i in tickers]

        return tickers

    def get_previous_1mo_or_1wk_data(
            self,
            data: pd.DataFrame,
            symbol: str,
            period: str,
            interval: str,
            drop_stock_dividend: bool = True) -> pd.DataFrame:

        """
        `get_previous_1mo_or_1wk_data` fetches you
        the last month's or last week's
        OHLC data along with % Change  from the previous close
        of the previous month data from
        the current month or current week.
        The purpose of this method is to only
        serve you the previous week or previous month's data
        for the selected 'symbol' (stock).

        :param data: Data Frame containing OHLC data
        :param symbol: Stock Symbol
        :param period: period in str as required by yfinance
        :param interval: interval in str as required by yfinance
        :param drop_stock_dividend:

        :returns: Dataframe with OHLC, Prevclose
                  & % Change for selected Symbol
        """
        self.logger.info(
            "Extracting Bhav for : {0}".format(re.sub(r"\.NS$", "", symbol))
        )
        data = data.history(period=period, interval=interval)
        data = data.loc[~((data.Dividends != 0) | (data["Stock Splits"] != 0)),
               :]
        data.columns = data.columns.str.lower()
        data["prev_close"] = data.close.shift(1)
        data = data.reset_index()
        data["Date"] = pd.to_datetime(data.Date).apply(lambda x: x.date())
        data.rename(columns={"Date": "date"}, inplace=True)
        data = data.loc[~data.prev_close.isna(), :]

        if period in ("2mo", "1wk"):
            # Assuming that when the period is '2mo' or '2wk',
            # we will only have
            try:
                data = pd.DataFrame.from_dict(
                    [
                        {
                            "date": data.date.iloc[0],
                            "open": data.open.iloc[0],
                            "high": max(data.high),
                            "low": min(data.low),
                            "close": data.close.iloc[-1],
                            "volume": data.volume.iloc[0] + data.volume.iloc[
                                1],
                            "prev_close": data.prev_close.iloc[0],
                        }
                    ]
                )
            except IndexError:
                pass

        data = data.head(1).reset_index()
        data = self.calculate_pct_change(data, "close", "prev_close")
        data["symbol"] = re.sub(r"\.NS$", "", symbol)

        try:
            data = data.drop("index", axis=1)
        except KeyError:
            pass

        if drop_stock_dividend:
            data = data.drop(data.columns[[-3, -4]], axis=1)

        return data

    def get_previous_weekly_quote(self, tickers: list) -> pd.DataFrame:
        """
        Get Previous Weekly Quote

        :param tickers: List of Stocks
        :returns: Previous Weekly Quote.

        """
        tickers = self.get_unique_ticker_set(tickers)
        data = yf.Tickers(" ".join(tickers))

        weekly_expiry = datetime.strptime(
            next(MarketCalendarTools.get_weekly_expiry()), DATE_FMT
        )

        # If the weekday falls on a Friday, Saturday & Sunday i.e. the day after expiry
        # and if today is less than or equal to next expiry
        # Merge the 1wk data with the current day data.
        # else Only return previous week's data.
        if date.today().weekday() in (
                3, 4, 5, 6) and datetime.today() <= weekly_expiry:
            data_dict = {
                i: self.get_previous_1mo_or_1wk_data(
                    data.tickers[i], i, period="1wk", interval="1wk"
                )
                for i in tickers
            }

        else:
            data_dict = {
                i: self.get_previous_1mo_or_1wk_data(
                    data.tickers[i], i, period="2wk", interval="1wk"
                )
                for i in tickers
            }

        data_dict = pd.concat(data_dict.values())
        data_dict["date"] = pd.to_datetime(data_dict.date).dt.strftime(
            "%d-%b-%Y")

        return data_dict

    def get_previous_monthly_quote(self, tickers: list) -> pd.DataFrame:
        """
        Get Previous Monthly Quote

        :param tickers: List of Stocks
        :returns: Previous Monthly Quote.
        """
        # TODO: Needs fixing.
        tickers = self.get_unique_ticker_set(tickers)
        data = yf.Tickers(" ".join(tickers))

        monthly_expiry = datetime.strptime(
            next(MarketCalendarTools.get_monthly_expiry()), DATE_FMT
        )
        _, eom_day = monthrange(date.today().year, date.today().month)

        # If the day is less than the day in the calendar month
        # and today is less than the monthly expiry which is the current month's expiry
        # Consume the second data from the 3mo period on 1mo interval
        # else return curent month's data including the last day until month completes.

        if date.today().day < eom_day and datetime.today() < monthly_expiry:

            data_dict = {
                i: self.get_previous_1mo_or_1wk_data(
                    data.tickers[i], i, period="3mo", interval="1mo"
                )
                for i in tickers
            }

        else:
            data_dict = {
                i: self.get_previous_1mo_or_1wk_data(
                    data.tickers[i], i, period="2mo", interval="1mo"
                )
                for i in tickers
            }

        data_dict = pd.concat(data_dict.values())
        data_dict["date"] = pd.to_datetime(data_dict.date).dt.strftime("%b-%Y")

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

        time_methods = {
            "week": self.get_previous_weekly_quote,
            "month": self.get_previous_monthly_quote,
        }

        return time_methods[timeframe](tickers)
