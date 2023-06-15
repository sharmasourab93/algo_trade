import pandas as pd
import numpy as np
from math import sqrt
from calendar import monthrange

from algo_trade.utils.meta import AsyncLoggingMeta
from algo_trade.market.strategy.indicators import PivotPoints
from algo_trade.market.strategy.constants import ALL_INDICES_COLUMNS_ORDER, \
    ALL_INDICES_COLUMN_RENAME, ALL_INDICES_COLUMNS_INCLUDE_SYMBOL, \
    SELECT_COLUMNS_FOR_INDEX_REPORT
from algo_trade.data_handler.calendar.constants import DATE_FMT, TODAY
from algo_trade.data_handler import DataHandler
from algo_trade.data_handler.calendar import MarketCalendarTools


class DailyIntradayIndicesReport(PivotPoints, metaclass=AsyncLoggingMeta):
    def __init__(self):
        self.nse_processor = DataHandler()
        self.next_bday = self.nse_processor.next_day
        self.last_bday = self.nse_processor.prev_day
        self.month_range = monthrange(TODAY.year, TODAY.month)
        self.data = self.nse_processor.get_nse_all_indices()

    def indices_report(self, filter_symbols: bool = True) -> pd.DataFrame:
        """Generating a Daily Analysis Report on the Index.

        Steps include:
        1. Getting All Indices Report in to a dataframe
        2.  Basic Transformations
        3. Plotting CPR Pivots
        4. Including and excluding Items in Symbol & Sectors
        5. Calculating Day high Low Range based on VIX.

        """

        data = self.data

        data = data.fillna(0)

        data["Advance:Decline"] = (
                data.advances.astype(str)
                + "-"
                + data.unchanged.astype(str)
                + "-"
                + data.declines.astype(str)
        )
        data["timestamp"] = self.last_bday.strftime(DATE_FMT)

        data = data.rename(columns=ALL_INDICES_COLUMN_RENAME)

        data = self.plot_pivots_with_cpr(data)
        data = data[ALL_INDICES_COLUMNS_ORDER]

        # Excluding FIXED INCOME INDICES & Only including some Symbols.
        if filter_symbols:

            data = data.loc[
                   ~data.loc[:, "key"].isin(["FIXED INCOME INDICES"])
                   & data.loc[:, "symbol"].isin(
                       ALL_INDICES_COLUMNS_INCLUDE_SYMBOL),
                   :,
                   ]
        else:
            data = data.loc[~data.loc[:, "key"].isin(["FIXED INCOME "
                                                      "INDICES"]), :]

        data = self.calculate_vix_index_range(data, 1)
        data = self.calculate_vix_index_range(data, 5)
        data = self.calculate_vix_index_range(data, self.month_range[1])

        return data.round(2)[SELECT_COLUMNS_FOR_INDEX_REPORT]

    def calculate_vix_index_range(
            self, data: pd.DataFrame, days_to_consider: int = 1
    ) -> pd.DataFrame:
        """Method to Calculate Vix Index Range"""

        # Source: https://www.vtrender.com/vix-nifty-range-explained/

        indices_for = ["INDIA VIX", "NIFTY 50", "NIFTY BANK",
                       "NIFTY FIN SERVICE"]

        # Step 1: Spot Close Price
        nf = data.loc[data.symbol == indices_for[1], :].close.values.tolist()[
            0]
        bnf = data.loc[data.symbol == indices_for[2], :].close.values.tolist()[
            0]
        fin = data.loc[data.symbol == indices_for[3], :].close.values.tolist()[
            0]

        # Step 2: Vix Close Price
        vix = (data.loc[data.symbol == indices_for[0],
               :].close / 100).values.tolist()[
            0
        ]

        # Step 3: days until Expiry
        # no_days_in_year = MarketCalendarTools.number_of_days_until_year_end()
        no_days_in_year = 365

        # Step 4: Calculated Adjusted Volatility Index.
        adj_vol_index = vix / sqrt(no_days_in_year / days_to_consider)

        # Step 5: Value in step 4 multiplied by Spot close price.
        nf_spot_range = nf * adj_vol_index
        bnf_spot_range = bnf * adj_vol_index
        fin_spot_range = fin * adj_vol_index

        # Step 6: Adding & Subtracting for identifying Index range
        nf_spot_range = (nf + nf_spot_range, nf - nf_spot_range)
        bnf_spot_range = (bnf + bnf_spot_range, bnf - bnf_spot_range)
        fin_spot_range = (fin + fin_spot_range, fin - fin_spot_range)

        # Extending Index Range to Weekly & Monthly.

        if days_to_consider == 1:
            range_str_1, range_str_2 = "day_higher_range", "day_lower_range"

        elif 1 < days_to_consider <= 5:
            range_str_1, range_str_2 = "weekly_higher_range", \
                "weekly_lower_range"

        else:
            range_str_1, range_str_2 = "monthly_higher_range", \
                "monthly_lower_range"

        data[range_str_1] = int()
        data[range_str_2] = int()

        data.loc[
            data.symbol == indices_for[1], [range_str_1, range_str_2]
        ] = nf_spot_range
        data.loc[
            data.symbol == indices_for[2], [range_str_1, range_str_2]
        ] = bnf_spot_range
        data.loc[
            data.symbol == indices_for[3], [range_str_1, range_str_2]
        ] = fin_spot_range

        # Weekly Range Generated every Thursday.
        if TODAY.weekday() == 3 and days_to_consider == 1:
            days_to_consider = MarketCalendarTools.days_until_expiry()

            data = self.calculate_vix_index_range(data, days_to_consider)

        # month_range = calendar.monthrange(TODAY.year, TODAY.month)

        # Monthly Range generated last 3 days of every month
        if TODAY.day >= self.month_range[1] - 2 and days_to_consider < 10:
            days_to_consider = (
                    MarketCalendarTools.number_of_working_days_in_a_month()
                    + (self.month_range[1] - TODAY.day)
            )

            data = self.calculate_vix_index_range(data, days_to_consider)

        return data


if __name__ == "__main__":
    from datetime import datetime

    obj = DailyIntradayIndicesReport()
    data = obj.indices_report()
    print(data)
