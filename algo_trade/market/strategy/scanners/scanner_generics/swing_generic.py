from abc import abstractmethod
import pandas as pd
from algo_trade.utils.meta import AsyncLoggingMeta
from algo_trade.market.strategy.indicators import MovingAverages
from algo_trade.data_handler import DataHandler
from algo_trade.data_handler.calendar.constants import TODAY, DATE_FMT
from algo_trade.market.strategy.constants import SWING_NSE_LIMIT, \
    SWING_FNO_FILTER, SWING_EARNINGS_DELTA, SWING_MIN_SHARE_PRICE, \
    SWING_MIN_PCT_CHANGE, STOCK_MODIFICATION


class SwingTradingGeneric(MovingAverages, metaclass=AsyncLoggingMeta):
    """
    A Generic Abstract SwingTrading class which is a parent
    to all the derived Swing Trading classes.
    The prime indicator required are
        1. Moving Averages

    """

    processed_data = None
    swing_result = pd.DataFrame()

    def __init__(
            self,
            nse: int = SWING_NSE_LIMIT,
            date_str: str = None,
            fno_filter: bool = SWING_FNO_FILTER,
            result_delta: int = SWING_EARNINGS_DELTA,
    ):
        self.nse_nos = nse
        self.date_str = date_str
        self.processor = DataHandler()
        self.fno_filter = fno_filter

    def get_swing_trading_ready_list(self) -> pd.DataFrame:
        """List of Stocks ready for Swing Trading."""

        if self.processed_data is not None:
            return self.processed_data

        # 1. Consume Processed data using Top NSE nos.
        data = self.processor.processed_bhavcopy(self.nse_nos)

        # 2. Select columns and basic column type and rename actions.
        data_columns = [
            "index",
            "symbol",
            "lotsize",
            "close",
            "prev_close",
            "volume",
            "pct_change",
            "timestamp"
        ]
        data = data[data_columns]
        # data = data[data_columns].rename(columns={"Tottrdqty": "Last Volume"})
        # data.loc[:, "Last Volume"] = data["Last Volume"].astype(int)

        # 3. Align Result Date Over the span of a + or - 15 days.
        if self.date_str is None:
            self.date_str = TODAY.strftime(DATE_FMT)

        # 4. Look for basic stock ticker name changes.
        # Manual transformation of Stocks whose name has been changed.
        modification = list(STOCK_MODIFICATION.keys())

        if any(data.symbol.isin(modification)):
            data.loc[data.symbol.isin(modification), "symbol"] = data.loc[
                data.symbol.isin(modification), "symbol"
            ].map(STOCK_MODIFICATION)

        self.logger.info("Data Generated for Swing Trade")

        data.set_index("index")

        SwingTradingGeneric.processed_data = data

        return data

    def append_swing_results(self, data):
        """ Method to append swing result. """

        SwingTradingGeneric.swing_result = pd.concat(
            [SwingTradingGeneric.swing_result, data])

    def get_swing_result(self) -> pd.DataFrame:
        """
        Meant to return Swing Result
        after all the strategies have been executed.
        . """

        data = SwingTradingGeneric.swing_result.fillna(0)
        return data

    @abstractmethod
    def generate_swing_output(self):
        """This abstract method needs to be implemented to execute
        Derived swing strategies.
        """

        raise NotImplementedError()
