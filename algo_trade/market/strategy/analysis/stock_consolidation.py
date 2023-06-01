import pandas as pd
import numpy as np
from math import floor
from typing import Optional

# Internal Module Imports
from algo_trade.data_handler import DataHandler
from algo_trade.utils.meta import AsyncLoggingMeta


class StockConsolidationRange(metaclass=AsyncLoggingMeta):
    """
    StockConsolidationRange class provides all methods to
    calculate the consolidation range for a stock/index.
    """

    def __init__(self, con_threshold: int = 1):
        self.yf_utils = DataHandler()
        self.con_threshold = con_threshold

    def get_count_consolidation(self, data) -> int:
        """
        get_count_consolidation provides the number of days/market days
        the provided stock or index has been consolidating.
        """

        i, count = 0, 0

        # Iterate over the % Change for a given day
        # and then count or break.
        while i < len(data):
            try:
                if floor(abs(data[i])) < self.con_threshold:
                    count += 1

                else:
                    break

            except ValueError:
                pass

            i += 1

        return count

    def get_daily_consolidation_range(self, symbol: str) -> int:
        """
        Utility method to combine the period data and
        identify the consolidation range for the given stock
        in the D timeframe.

        :param symbol:  (str) Preferably name of the Company/Ticker.
        :returns: (int)
        """
        data = self.yf_utils.get_period_data(symbol)

        data = data.reset_index()

        data = data["pct_change"].values.tolist()

        return self.get_count_consolidation(data)

    def get_weekly_consolidation_range(self, symbol: str) -> int:
        """
        Utility method to combine the period data and
        identify the consolidation range for the given stock
        in the W timeframe.

        :param symbol:  (str) Preferably name of the Company/Ticker.
        :returns: (int)
        """

        data = self.yf_utils.get_period_data(symbol, period="ytd",
                                             interval="1wk")
        data = data.reset_index()

        data = data["% Change"].values.tolist()

        return self.get_count_consolidation(data)

    def get_monthly_consolidation_range(self, symbol: str) -> int:
        """
        Utility method to combine the period data and
        identify the consolidation range for the given stock
        in the M timeframe.

        :param symbol:  (str) Preferably name of the Company/Ticker.
        :returns: (int)
        """

        data = self.yf_utils.get_period_data(symbol, period="ytd",
                                             interval="1mo")
        data = data.reset_index()

        data = data["% Change"].values.tolist()

        return self.get_count_consolidation(data)

    def get_consolidation_range(self, symbol: str, chart: str = "D") -> int:
        """
        get_consolidation_range method picks the symbol consolidation range analysis
        for the provided Timeframe ('D', 'W', 'M').
        """

        consolidation_range = {
            "D": self.get_daily_consolidation_range,
            "W": self.get_weekly_consolidation_range,
            "M": self.get_monthly_consolidation_range,
        }

        count = consolidation_range[chart](symbol)
        return count

    def get_consolidation_range_for_series(
            self, tickers: list, chart: str = "D", to_df: bool = True
    ) -> Optional[pd.DataFrame]:

        """Method to get consolidation range for a series of stock tickers."""

        data = {i: self.get_consolidation_range(i, chart) for i in tickers}

        if to_df:
            data = pd.DataFrame.from_dict(data.items())
            data.columns = ["symbol", "con_range"]

        return data
