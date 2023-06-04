import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from collections import ChainMap
from algo_trade.data_handler import DataHandler
from algo_trade.data_handler.calendar.constants import DATE_FMT
from algo_trade.market.strategy.analysis import StockConsolidationRange
from algo_trade.market.strategy.indicators import MovingAverages, PivotPoints
from algo_trade.market.strategy.analysis import StockConsolidationRange

VCPR_ANALYSIS_CPR_CUT_OFF = 0.2


class CPRAbstract(MovingAverages, PivotPoints):
    CPR_FREQUENCY = 'DAILY'

    def __init__(self,
                 *,
                 ascending: bool = False,
                 date_str: str = None,
                 consolidation: bool = True,
                 order_by: tuple = ("con_range", "cpr_width"),
                 nse: int = 250,
                 vcpr_analysis: bool = True):
        self.nse_n = nse
        self.asc = ascending
        self.order_by = order_by
        self.consolidation = consolidation
        self.yf_utils = DataHandler()
        self.consol_range_obj = StockConsolidationRange()
        self.vcpr_analysis = vcpr_analysis

        if self.vcpr_analysis:
            self.vcpr_cpr_cutoff = VCPR_ANALYSIS_CPR_CUT_OFF

    @abstractmethod
    def cpr_strategy_output(self):
        raise NotImplementedError()

    def find_vcpr_with_date(self, data: list) -> str:
        """Find VCPR with Date."""

        vcpr = list()
        cpr = str()

        for i, j in enumerate(data):
            if i == 0 and j[1] in ("No VCPR", "Wide VCPR"):
                return j[1]

            elif j[1] == "Mild VCPR":
                cpr = "Mild VCPR from {0}".format(j[0])

            elif i > 0 and j[1] == "Wide VCPR":

                return "Wide VCPR from {0}".format(j[0])

            elif cpr is not None and j[1] == "No VCPR":
                return cpr

        return cpr

    async def vcpr_util(self, symbol: str, period: str, interval: str) -> dict:
        """
        vcpr_util fetches CPR Range for past several days/candles per the given timeframe.

        :param symbol: Ticker Symbol for which the VCPR is to be found.

        :param period:

        :param interval:

        :returns: dict with a tuple of Long VCPR & Short VCPR.
        """

        self.logger.info(
            "VCPR Analysis under progress for {0}.".format(symbol))

        data = self.yf_utils.get_period_data(symbol, period=period,
                                             interval=interval)
        data = data.reset_index()
        data.loc[:, "date"] = data.date.apply(lambda x: x.strftime(DATE_FMT))
        data = self.plot_pivots_with_cpr(data)

        data["Prev CPR max"] = data[["tcpr", "bcpr"]].max(axis=1).shift(-1)
        data["Prev1 CPR max"] = data[["tcpr", "bcpr"]].max(axis=1).shift(-2)
        data["Prev2 CPR max"] = data[["tcpr", "bcpr"]].max(axis=1).shift(-3)
        data["Prev3 CPR max"] = data[["tcpr", "bcpr"]].max(axis=1).shift(-4)
        data["Prev4 CPR max"] = data[["tcpr", "bcpr"]].max(axis=1).shift(-5)

        data["Prev CPR min"] = data[["tcpr", "bcpr"]].min(axis=1).shift(-1)
        data["Prev1 CPR min"] = data[["tcpr", "bcpr"]].min(axis=1).shift(-2)
        data["Prev2 CPR min"] = data[["tcpr", "bcpr"]].min(axis=1).shift(-3)
        data["Prev3 CPR min"] = data[["tcpr", "bcpr"]].min(axis=1).shift(-4)
        data["Prev4 CPR min"] = data[["tcpr", "bcpr"]].min(axis=1).shift(-5)

        data = data.fillna(0).loc[:10, :]

        data["Long VCPR"] = np.where(
            (
                    (data.high <= data["Prev CPR max"])
                    | (data.high <= data["Prev1 CPR max"])
                    | (data.high <= data["Prev2 CPR max"])
                    | (data.high <= data["Prev3 CPR max"])
                    | (data.high <= data["Prev4 CPR max"])
            ),
            "VCPR",
            "No VCPR",
        )

        data["Short VCPR"] = np.where(
            (
                    (data.low >= data["Prev CPR min"])
                    | (data.low >= data["Prev1 CPR min"])
                    | (data.low >= data["Prev2 CPR min"])
                    | (data.low >= data["Prev3 CPR min"])
                    | (data.low >= data["Prev4 CPR min"])
            ),
            "VCPR",
            "No VCPR",
        )

        data["Long VCPR"] = data["Long VCPR"].where(
            ~((data["cpr"] == "Wide CPR") & (data["Long VCPR"] == "VCPR")),
            "Wide VCPR"
        )
        data["Long VCPR"] = data["Long VCPR"].where(
            ~(
                    (data["cpr"].isin(["Narrow CPR", "Mid CPR"]))
                    & (data["Long VCPR"] == "VCPR")
            ),
            "Mild VCPR",
        )

        data["Short VCPR"] = data["Short VCPR"].where(
            ~((data["cpr"] == "Wide CPR") & (data["Short VCPR"] == "VCPR")),
            "Wide VCPR"
        )
        data["Short VCPR"] = data["Short VCPR"].where(
            ~(
                    (data["cpr"].isin(["Narrow CPR", "Mid CPR"]))
                    & (data["Short VCPR"] == "VCPR")
            ),
            "Mild VCPR",
        )

        long_vcpr = data.loc[:5, ["date", "Long VCPR"]].values.tolist()
        short_vcpr = data.loc[:5, ["date", "Short VCPR"]].values.tolist()
        vcpr = list(map(self.find_vcpr_with_date, (long_vcpr, short_vcpr)))

        if vcpr[0] == str():
            vcpr[0] = "Mild VCPR to No VCPR"

        if vcpr[1] == str():
            vcpr[1] = "Mild VCPR to No VCPR"

        self.logger.info(
            "VCPR for {0} is as follows: Long VCPR {1} & Short VCPR {2}".format(
                symbol, *vcpr
            )
        )

        return {symbol: vcpr}

    async def identify_ticker_with_vcpr(
            self, symbols: list, period: str = "1mo", interval: str = "1d"
    ) -> dict:
        """List of tickers to iterate over and identify VCPR."""

        args = list(
            zip(symbols, [period] * len(symbols), [interval] * len(symbols)))

        return dict(ChainMap(*[await self.vcpr_util(*i) for i in args]))
