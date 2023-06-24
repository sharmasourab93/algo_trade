import pandas as pd
import numpy as np
from math import floor, ceil
from typing import Optional, List, Union, Dict, Tuple
from functools import cache

# Internal Modules import.
from algo_trade.data_handler import DataHandler
from algo_trade.utils.meta import AsyncLoggingMeta
from algo_trade.market.strategy.indicators.moving_averages import \
    MovingAverages


class ConsolidationRange(metaclass=AsyncLoggingMeta):
    """
    StockConsolidationRange class provides all methods to calculate the
    consolidation range for a stock/index.
    """

    def __init__(self, con_threshold: int = 1):
        self.yf_utils = DataHandler()
        self.moving_avg = MovingAverages()

    @cache
    def _get_data_for_consolidation(self,
                                    tickers: Union[tuple, str],
                                    period: str,
                                    interval: str
                                    ) -> Dict[str, pd.DataFrame]:
        if isinstance(tickers, str):
            return {tickers: self.yf_utils.get_period_data(tickers,
                                                           period=period,
                                                           interval=interval)}

        data = {i: self.yf_utils.get_period_data(i, period=period,
                                                 interval=interval)
                for i in tickers}

        return data

    def _get_count_consolidation(self, data: list, con_threshold: int = 1) -> \
            int:
        """
        _get_count_consolidation provides the number of days/market days
        the provided stock or index has been consolidating.
        """

        i, count = 0, 0

        # Iterate over the % Change for a given day
        # and then count or break.
        while i < len(data):
            try:
                if floor(abs(data[i])) < con_threshold:
                    count += 1

                else:
                    break

            except ValueError:
                pass

            i += 1

        return count

    def _get_consolidation_range(self, data: pd.DataFrame) -> int:

        data = data.reset_index()

        data = data["pct_change"].values.tolist()

        return self._get_count_consolidation(data)

    def get_time_consolidation(self, tickers: Union[List[str], str],
                               chart: str = 'D', to_df: bool = True,
                               period: str = 'ytd', interval: str = '1d', ) -> \
            Union[pd.DataFrame, Dict[str, int]]:

        data = self._get_data_for_consolidation(tuple(tickers), period,
                                                interval)

        data = {i: self._get_consolidation_range(j)
                for i, j in data.items()}

        if to_df:
            data = pd.DataFrame(tuple(data.items()), columns=["symbol",
                                                              "time_con_range"])

        return data

    def get_price_consolidation(self,
                                tickers: Union[Tuple[str], str],
                                period: str, interval: str,
                                emas: Tuple[int] = (10, 20, 50),
                                min_max_range: Tuple[int, int] = (5, 10),
                                to_df: bool = True
                                ) -> Union[Dict[str, int], pd.DataFrame]:
        """
        Method to get price consolidation. Pass in ticker, period, interval
        and retrieve price consolidation.
        """

        data = self._get_data_for_consolidation(tickers,
                                                period=period,
                                                interval=interval)
        data = {i: self.moving_avg.add_moving_averages(j, ma_range=emas)
                for i, j in data.items()}

        # TODO: implement logic for price consolidation.

        col_names = ["EMA{0}".format(i) for i in emas]

        result = list()

        for i, j in data.items():
            j['close/min'] = ((j.close - j[col_names].min(axis=1)) /
                              j.close) * 100
            j['close/max'] = ((j.close - j[col_names].max(axis=1)) /
                              j.close) * 100
            j['close/min-max'] = (j["close/min"]
                                  - j["close/max"]). \
                apply(lambda x: floor(x)).round(2)

            j['price_consolidation'] = j["close/min-max"]. \
                apply(
                lambda x: 1 if min_max_range[0] <= x <= min_max_range[1]
                else 0)

            j = j.drop(columns=['close/min', 'close/max', 'close/min-max',
                                *col_names])
            j = j.price_consolidation.values.tolist()

            result.append((i, self._get_count_consolidation(j)))

        if to_df:
            data = pd.DataFrame(result, columns=["symbol", "price_con_range"])

        return data

    def get_price_time_consolidation(self, tickers: Union[List[str], str],
                                     emas: Tuple[int] = (10, 20, 50),
                                     chart: str = 'D') -> pd.DataFrame:

        """
        Gets you price consolidation and time consolidation combined.
        """

        if chart not in ("D", "W", "M"):
            raise KeyError("{0} Chart Timeframe provided doesn't "
                           "exist.".format(chart))

        period = "ytd"

        chart_interval = {
            "D": "1d",
            "W": "1wk",
            "M": "1mo"
        }

        interval = chart_interval[chart]

        tickers = tuple(tickers)

        price_consolidation = self.get_price_consolidation(tuple(tickers),
                                                           period,
                                                           interval, emas)

        time_consolidation = self.get_time_consolidation(tuple(tickers),
                                                         period,
                                                         interval, chart)

        return pd.merge(price_consolidation,
                        time_consolidation,
                        on="symbol")
