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


def merge_quote_option_chain(method):
    def quote_option_chain(self, *args, **kwargs):
        start_time = perf_counter()
        data = method(self, *args, **kwargs)

        if data is None:
            return None

        try:
            symbol, expiry, strike = args

        except ValueError:
            symbol, expiry = args
            strike = None

        all_expiries: list = data["records"]["expiryDates"]
        last_updated: str = data["records"]["timestamp"]
        strike_prices: list = data["records"]["strikePrices"]
        underlying: int = data["records"]["underlyingValue"]

        # Creating A Respone
        response = dict()
        response.update(
            {
                "Symbol": symbol,
                "selected_expiry": expiry,
                "all_expiries": all_expiries,
                "last_updated": last_updated,
                "Strikes": strike_prices,
                "selected_strike": None if strike is None else strike,
                "underlying_value": underlying,
            }
        )

        # DataFrame Operations Section.
        df = DataFrame(json_normalize(data["records"]["data"]))
        df = df.loc[(df.expiryDate == expiry), :]

        if 0 in df.shape:
            return None

        if strike is not None:
            df = df.loc[df.strikePrice == strike, :]

        df.set_index("strikePrice", inplace=True)

        # Common Columns
        columns = [
            "openInterest",
            "changeinOpenInterest",
            "pchangeinOpenInterest",
            "totalTradedVolume",
            "impliedVolatility",
            "lastPrice",
            "change",
            "pChange",
            "totalBuyQuantity",
            "totalSellQuantity",
            "bidQty",
            "bidprice",
            "askQty",
            "askPrice",
        ]

        options = ["Calls", "Puts"]
        # Call Options
        if "NIFTY" in symbol:
            ce_data = df.iloc[:, -19:]
            pe_data = df.iloc[:, :20]
        else:
            ce_data = df.iloc[:, :20]
            pe_data = df.iloc[:, 20:]

        ce_data.columns = ce_data.columns.str.replace(r"CE\.", "")
        ce_data = ce_data[columns].round(2)
        ce_data.columns = "CE_" + ce_data.columns

        # ce_data.columns = pd.MultiIndex.from_product([[options[0]],
        #                                               ce_data.columns.tolist()])
        ce_data.reset_index(inplace=True)

        # Put Options
        pe_data.columns = pe_data.columns.str.replace(r"PE\.", "")
        pe_data = pe_data[columns].round(2)
        pe_data.columns = "PE_" + pe_data.columns
        # pe_data.columns = pd.MultiIndex.from_product([[options[1]],
        #                                                pe_data.columns.tolist()])
        pe_data.reset_index(inplace=True)

        merged_call_put = pd.merge(ce_data, pe_data, on="strikePrice",
                                   how="left")
        # response.update({'OptionChain': merged_call_put.to_dict('records')})
        response.update(
            {
                "OptionChain": merged_call_put.round(2),
                "ResponseGenerateTime": datetime.now(tz=TIME_ZONE),
            }
        )
        end_time = round(perf_counter() - start_time, 2)

        return response

    return quote_option_chain


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

    @merge_quote_option_chain
    def get_select_stock_options_bhavcopy(self, symbol: str, expiry: str):

        expiry_set: list = self.get_all_monthly_expiries()
        fo_symbols: list = self.get_fo_list()
        ce_data, pe_data = self.get_stock_options_bhavcopy()

        if expiry in expiry_set and symbol in fo_symbols:
            ce_data = ce_data.loc[
                      (ce_data.Symbol == symbol) & (
                              ce_data.Expiry_dt == expiry), :
                      ]
            pe_data = pe_data.loc[
                      (pe_data.Symbol == symbol) & (
                              pe_data.Expiry_dt == expiry), :
                      ]

            return ce_data.round(2), pe_data.round(2)

        return None

    @merge_quote_option_chain
    def get_select_stock_option_chain(
            self, symbol: str, expiry: str, strike: int = None
    ):

        expiries = self.get_all_monthly_expiries()
        all_symbols = self.get_fo_list()

        if symbol in all_symbols and expiry in expiries:
            data = self.get_option_chain(symbol)

            return data

        return None

    @merge_quote_option_chain
    def get_select_index_option_chain(
            self, symbol: str, expiry: str, strike: int = None
    ):

        expiries = self.get_all_weekly_expiries()
        all_symbols = self.get_processed_index_names()

        if symbol in all_symbols and expiry in expiries:
            data = self.get_option_chain_index(symbol)

            return data

        return None
